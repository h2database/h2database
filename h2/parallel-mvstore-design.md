# H2 MVStore Parallel Chunk I/O — Design Document

## 1. Current Write Pipeline (As-Is)

After studying the source, the current write path is a **3-stage pipeline** with two single-threaded executors:

```
MVStore.store()                   [main thread, under storeLock]
  │
  ├─ collectChangedMapRoots()     → List<Page> of dirty root pages
  │
  └─ FileStore.storeIt()
       │
       ├─ serializationExecutor   [single thread, serializationLock]
       │   └─ serializeAndStore()
       │       ├─ createChunk(time, version)
       │       ├─ serializeToBuffer(buff, changed, chunk, prevChunk)
       │       │   ├─ for each root page:
       │       │   │   └─ page.writeUnsavedRecursive(PSM)   ← depth-first recursive
       │       │   ├─ acceptChunkOccupancyChanges()
       │       │   ├─ layout map serialization
       │       │   └─ PSM.serializeToC()                    ← flat Table of Contents
       │       │
       │       └─ bufferSaveExecutor  [single thread, saveChunkLock]
       │           └─ storeBuffer()
       │               ├─ allocateChunkSpace()    ← FreeSpaceBitSet.allocate()
       │               ├─ writeChunkHeader/Footer into buffer
       │               └─ writeChunk()            ← writeFully() to FileChannel
       │
       └─ releaseSavedPages()
```

### Key Observations

1. **All pages for one version go into ONE chunk, ONE WriteBuffer**. The `PageSerializationManager` tracks sequential page numbers via `toc.size()` and writes pages contiguously.

2. **NonLeaf serialization has a patching dependency**: `NonLeaf.writeUnsavedRecursive()` writes the parent node first (with placeholder child positions), then recursively serializes children, then patches the parent's child position slots. This is fundamentally **bottom-up within a single B-tree**.

3. **Cross-map serialization is independent**. In `serializeToBuffer()`, the loop `for (Page p : changed)` processes each map's root independently. Map A's subtree never references Map B's pages.

4. **The pipeline already has asynchronous hand-off** between serialization and I/O, but both stages are single-threaded.

5. **Space allocation is inherently serial** — `FreeSpaceBitSet` is not thread-safe and `allocateChunkSpace()` runs under `saveChunkLock`.


## 2. Parallelism Opportunities

### Level 1: Parallel Map Serialization (within a chunk)

**What**: Serialize different maps' B-trees in parallel into separate buffers, then concatenate.

**Why it works**: In `serializeToBuffer()`, each `page.writeUnsavedRecursive(PSM)` call for different maps is independent. No shared mutable state between maps except the `PageSerializationManager` (page numbering and the single WriteBuffer).

**Estimated impact**: High for workloads with many dirty maps. The serialization cost is CPU-bound (key/value serialization, compression), so this scales with cores.

### Level 2: Parallel Sub-tree Serialization (within a single map)

**What**: For a large B-tree, serialize sibling subtrees in parallel.

**Why it's harder**: `NonLeaf.writeUnsavedRecursive()` writes parent → recurse children → patch parent. Siblings are independent, but the parent must be written first to reserve space for child positions, and patched after children are done.

**Approach**: Pre-allocate the parent node bytes, fork parallel serialization of each child subtree into separate buffers, join, concatenate, then patch the parent.

### Level 3: Parallel Chunk I/O

**What**: Write multiple chunks concurrently using positional writes.

**Why it works**: `FileChannel.write(ByteBuffer, long position)` is thread-safe for non-overlapping regions. With pre-allocated space, multiple chunks can be written simultaneously.

### Level 4: Parallel Reads

**What**: Already possible. `Chunk.readBufferForPage()` uses `readFully(fileStore, filePos, length)` which calls `FileChannel.read(buf, pos)` — this is positional and thread-safe. The `CacheLongKeyLIRS` page cache is already concurrent.


## 3. Phase 1 Design: Parallel Map Serialization

This is the cleanest win with the smallest blast radius.

### 3.1 New: `ParallelPageSerializationManager`

Instead of one `PageSerializationManager` shared across all maps, we create one per map (or per batch of maps), each with its own `WriteBuffer` and local `toc` list.

```java
public final class ParallelPageSerializationManager {

    private final int chunkId;
    private final int pageNoBase;          // starting page number for this segment
    private final WriteBuffer buff;
    private final List<Long> toc = new ArrayList<>();

    ParallelPageSerializationManager(int chunkId, int pageNoBase) {
        this.chunkId = chunkId;
        this.pageNoBase = pageNoBase;
        this.buff = new WriteBuffer();
    }

    public int getPageNo() {
        return pageNoBase + toc.size();
    }

    // ... same getPagePosition() logic, but uses local buff/toc
}
```

### 3.2 Modified: `serializeToBuffer()`

```java
private void serializeToBuffer(WriteBuffer finalBuff, ArrayList<Page<?,?>> changed,
                               C c, C previousChunk) {
    int headerLength = c.estimateHeaderSize();
    c.next = headerLength;
    long version = c.version;

    // Separate layout-affecting pages from regular map pages
    List<Page<?,?>> regularPages = new ArrayList<>();
    for (Page<?,?> p : changed) {
        if (p.getTotalCount() == 0) {
            layout.remove(MVMap.getMapRootKey(p.getMapId()));
        } else {
            regularPages.add(p);
        }
    }

    // Phase 1: Parallel serialization of independent map trees
    int numWorkers = Math.min(regularPages.size(), Runtime.getRuntime().availableProcessors());
    if (numWorkers <= 1) {
        // Fall back to serial path
        serialPath(finalBuff, changed, c, previousChunk, headerLength, version);
        return;
    }

    // Each map gets its own PSM with a local WriteBuffer
    List<CompletableFuture<MapSerializationResult>> futures = new ArrayList<>();
    AtomicInteger pageNoCounter = new AtomicInteger(0);

    for (Page<?,?> p : regularPages) {
        // Pre-reserve a page number range estimate (will be corrected post-merge)
        CompletableFuture<MapSerializationResult> f =
            CompletableFuture.supplyAsync(() -> {
                WriteBuffer localBuff = new WriteBuffer();
                // Each gets its own local PSM — page numbers assigned post-merge
                LocalPageSerializationManager localPSM =
                    new LocalPageSerializationManager(c.id, localBuff);
                p.writeUnsavedRecursive(localPSM);
                return new MapSerializationResult(p, localBuff, localPSM.getToc());
            }, serializationPool);
        futures.add(f);
    }

    // Join all results
    List<MapSerializationResult> results = futures.stream()
        .map(CompletableFuture::join)
        .collect(Collectors.toList());

    // Phase 2: Sequential assembly into the final buffer
    // (this is fast — just buffer copies and page number fixups)
    finalBuff.position(headerLength);
    int globalPageNo = 0;
    List<Long> mergedToc = new ArrayList<>();

    for (MapSerializationResult result : results) {
        int localPageCount = result.toc.size();
        // Remap page numbers: offset local page numbers by globalPageNo
        // and fix up the ToC entries and page positions
        for (int i = 0; i < localPageCount; i++) {
            long tocEntry = result.toc.get(i);
            // tocEntry contains (mapId, offset, length, type) — offset needs rebasing
            int localOffset = DataUtils.getPageOffset(tocEntry);
            int newOffset = finalBuff.position() + localOffset;
            // Recompose with corrected offset
            tocEntry = rebaseOffset(tocEntry, newOffset);
            mergedToc.add(tocEntry);
        }
        // Copy serialized bytes
        ByteBuffer src = result.buff.getBuffer();
        src.flip();
        finalBuff.put(src);
        globalPageNo += localPageCount;

        // Update layout with root positions
        long rootPos = result.rootPage.getPos();
        layout.put(MVMap.getMapRootKey(result.rootPage.getMapId()),
                   Long.toHexString(rootPos));
    }

    // Phase 3: Layout map serialization (must be serial — it references chunk metadata)
    // ... existing layout serialization logic ...
}
```

### 3.3 The Core Problem: Page Position Encoding

This is the subtlest issue. In the current design, `getPagePosition()` computes positions relative to the start of the chunk buffer:

```java
// In PageSerializationManager.getPagePosition():
long tocElement = DataUtils.composeTocElement(mapId, offset, pageLength, type);
// offset here is the byte position within the WriteBuffer
```

For parallel serialization with separate buffers, each local buffer starts at offset 0. During the assembly phase, we need to **rebase** all page offsets by adding the final position within the merged buffer.

**This also means page `pos` fields (stored in Page objects) must be fixed up**, since they encode chunk ID + offset. This is the trickiest part — `Page.pos` is set atomically via `posUpdater.compareAndSet()` during serialization.

#### Solution: Two-pass approach

1. **Pass 1 (parallel)**: Serialize each map's tree into a local buffer. Record all page positions relative to local buffer offset 0. Store a list of `(Page, localPos)` pairs.

2. **Pass 2 (serial, fast)**: Determine final byte offsets for each segment. Walk the recorded `(Page, localPos)` pairs and update each `Page.pos` to the correct global position. Rebuild the ToC with corrected offsets. Copy local buffers into the final buffer.

Pass 2 is pure memory operations — no serialization, no compression — so it should be very fast.


## 4. Phase 2 Design: Parallel Reads

### 4.1 Current Read Path Analysis

The read path has three layers:

```
MVMap.get(key) / Cursor iteration
  │
  └─ Page.get(p, key)                     [iterative descent]
       │
       └─ while (!p.isLeaf())
            p = p.getChildPage(index)      [on-demand load]
              │
              └─ NonLeaf.getChildPage(i)
                   │  PageReference ref = children[i];
                   │  Page page = ref.getPage();   ← null if not loaded
                   │  if (page == null)
                   │    page = map.readPage(ref.getPos());
                   │
                   └─ FileStore.readPage(map, pos)
                        │  1. cache lookup          ← CacheLongKeyLIRS (concurrent)
                        │  2. getChunk(pos)          ← ConcurrentHashMap lookup
                        │  3. chunk.readBufferForPage(fileStore, offset, pos)
                        │     ├─ compute filePos = block * 4096 + offset
                        │     ├─ determine length (may require 128-byte pre-read for PAGE_LARGE)
                        │     └─ readFully(fileStore, filePos, length)  ← FileChannel positional read
                        │  4. Page.read(buff, pos, map)
                        │     ├─ header/checksum validation
                        │     ├─ decompression (LZF or Deflate if compressed)
                        │     └─ key/value deserialization via DataType.read()
                        │  5. cachePage(page)
                        └─
```

**Key observation from `NonLeaf.readPayLoad()`**: When a NonLeaf page is deserialized, its `children[]` array is fully populated with `PageReference(pos, count)` entries — we know every child's file position immediately, but `page` is null. Children are loaded lazily by `getChildPage()`.

This means the moment we have a NonLeaf in hand, we have a complete prefetch manifest.

**Concurrency safety of the existing path**:
- `FileChannel.read(ByteBuffer, long position)` — thread-safe for positional reads
- `CacheLongKeyLIRS` — already concurrent (segmented)
- `chunks` — `ConcurrentHashMap`
- `chunksToC` — `CacheLongKeyLIRS` (concurrent)
- `Chunk.block` is volatile — handles chunk relocation during compaction

So reads are already *safe* to parallelize. The problem is they are *demand-driven* — each level of the B-tree waits for the previous level to load.


### 4.2 Read Pattern Analysis

There are four distinct read patterns, each with different parallelism opportunities:

**Pattern 1: Point Lookup** (`Page.get(p, key)`)
Iterative root→leaf descent. Each level depends on the previous (you need to binarySearch the parent to know which child to read). Inherently **sequential per-lookup**.

Parallelism opportunity: **None within a single lookup**, but multiple concurrent lookups from different threads are already safe. The win here is prefetching — if you can predict which child will be needed next, start the I/O early.

**Pattern 2: Range Scan** (Cursor)
Descends to a starting leaf, then walks leaf siblings via parent backtracking. Once the cursor identifies the next leaf's position, that I/O is independent.

Parallelism opportunity: **Sibling prefetch**. When processing leaf N, you already know (from the parent NonLeaf's `children[]` array) the positions of leaves N+1, N+2, etc. These can be prefetched in parallel.

**Pattern 3: Multi-Map Operations**
Different MVMaps have completely independent B-trees in potentially different chunks.

Parallelism opportunity: **Fully parallel**. No shared state between maps. This is free concurrency.

**Pattern 4: Bulk Load / Recovery**
During store opening, `readStoreHeader()` and `getChunksFromLayoutMap()` traverse the layout map and load chunk metadata. During compaction, `rewriteChunks()` scans entire chunks.

Parallelism opportunity: **Chunk-level parallelism** — read/process multiple chunks simultaneously.


### 4.3 Strategy 1: NonLeaf Sibling Prefetch

The highest-impact optimization for range scans. When a NonLeaf page is loaded, submit prefetch tasks for child pages that aren't already cached.

**Where to hook in**: `FileStore.readPage()`, after deserializing a NonLeaf, before returning it.

```java
<K,V> Page<K,V> readPage(MVMap<K,V> map, long pos) {
    // ... existing cache check and deserialization ...

    cachePage(page);

    // NEW: prefetch children of NonLeaf pages
    if (!page.isLeaf() && prefetchExecutor != null) {
        prefetchChildren(map, page);
    }

    return page;
}

private <K,V> void prefetchChildren(MVMap<K,V> map, Page<K,V> nonLeaf) {
    int childCount = nonLeaf.getRawChildPageCount();
    // Don't prefetch all children of a high-fanout node — limit to a window
    int prefetchLimit = Math.min(childCount, prefetchWindowSize);

    for (int i = 0; i < prefetchLimit; i++) {
        long childPos = nonLeaf.getChildPagePos(i);
        if (DataUtils.isPageSaved(childPos) && !isInCache(childPos)) {
            prefetchExecutor.submit(() -> {
                try {
                    // readPage populates cache as a side effect
                    readPage(map, childPos);
                } catch (Exception ignored) {
                    // Prefetch failure is not fatal — demand read will retry
                }
            });
        }
    }
}

boolean isInCache(long pos) {
    return cache != null && cache.get(pos) != null;
}
```

**Prefetch window sizing**: For a B-tree with fanout ~48 (default `keysPerPage`), prefetching all children of an internal node means up to 49 concurrent reads. On NVMe this is fine, on spinning disk it could cause thrashing. The `prefetchWindowSize` should be configurable:
- NVMe/SSD: 16–32 (saturate the device queue depth)
- HDD: 2–4 (minimal readahead to avoid seek storms)
- Disabled: 0 (for constrained environments)


### 4.4 Strategy 2: Cursor Readahead

More targeted than blind NonLeaf prefetch. When a `Cursor` is doing a forward scan and reaches the end of a leaf, it knows which sibling leaves come next by examining the parent's `children[]` array.

**Where to hook in**: The `Cursor` class, when advancing from one leaf to the next.

```java
// Conceptual change in Cursor.fetchNext() / advance logic:
private void advanceToNextLeaf() {
    // ... existing logic to find next leaf position from parent ...

    long nextLeafPos = parent.getChildPagePos(nextIndex);

    // NEW: while we're here, prefetch the leaf AFTER next
    if (nextIndex + 1 < parent.getRawChildPageCount()) {
        long prefetchPos = parent.getChildPagePos(nextIndex + 1);
        if (DataUtils.isPageSaved(prefetchPos) && !store.isInCache(prefetchPos)) {
            store.submitPrefetch(map, prefetchPos);
        }
    }

    // Now do the demand read for the immediate next leaf
    currentPage = map.readPage(nextLeafPos);
}
```

This is a one-ahead prefetch that overlaps I/O for leaf N+1 with processing of leaf N. For sequential scans this transforms the read pattern from:

```
[read leaf0][process leaf0][read leaf1][process leaf1][read leaf2]...
```

to:

```
[read leaf0][process leaf0 + read leaf1][process leaf1 + read leaf2]...
```

Halving the effective I/O latency for sequential scans.


### 4.5 Strategy 3: Chunk Buffering

Read the entire chunk into memory on first page access from that chunk. All subsequent page reads from the same chunk become memory copies — no I/O at all.

**The mechanism already exists**: `Chunk.buffer` is checked in both `readBufferForPage()` and `readPage()`. When `buffer != null`, pages are sliced from it. Currently this field is only non-null for chunks that haven't been flushed to disk yet (freshly written). We can reuse it for read buffering.

```java
// In FileStore.readPage(), before the per-page read:
<K,V> Page<K,V> readPage(MVMap<K,V> map, long pos) {
    Page<K,V> page = readPageFromCache(pos);
    if (page == null) {
        C chunk = getChunk(pos);

        // NEW: if chunk isn't buffered and it's small enough, buffer the whole thing
        if (chunk.buffer == null && chunk.len * BLOCK_SIZE <= chunkBufferThreshold) {
            bufferEntireChunk(chunk);
        }

        int pageOffset = DataUtils.getPageOffset(pos);
        // ... existing readBufferForPage / Page.read logic ...
        // (readBufferForPage already handles chunk.buffer != null)
    }
    return page;
}

private void bufferEntireChunk(C chunk) {
    if (chunk.isSaved() && chunk.buffer == null) {
        long filePos = chunk.block * BLOCK_SIZE;
        int length = chunk.len * BLOCK_SIZE;
        ByteBuffer buff = readFully(chunk, filePos, length);
        chunk.buffer = buff;
        // Schedule eviction after a timeout to bound memory usage
        chunkBufferEvictor.schedule(() -> chunk.buffer = null,
            chunkBufferTTL, TimeUnit.MILLISECONDS);
    }
}
```

**Trade-offs**:
- Turns N random reads into 1 sequential read (huge win on HDD, still significant on SSD)
- Memory cost: one chunk can be up to 2 GB (though typical chunks are much smaller)
- `chunkBufferThreshold` controls the trade-off — only buffer chunks below a size limit
- Must coordinate with the LRU page cache to avoid double-caching

**When this is most effective**: Sequential scans through a single chunk's worth of pages, which is the common case since pages written together (in the same commit) end up in the same chunk.


### 4.6 Strategy 4: io_uring Integration (JUring-specific)

This is where your JUring work fits in perfectly. Instead of N individual `pread64` syscalls, submit all reads as a batch of SQEs:

```java
// In a JUringFileStore extending SingleFileStore:
@Override
public ByteBuffer readFully(SFChunk chunk, long pos, int len) {
    // Single read — fall through to regular pread64
    return super.readFully(chunk, pos, len);
}

// NEW: batch read for prefetch
public CompletableFuture<ByteBuffer>[] readBatch(long[] positions, int[] lengths) {
    int count = positions.length;
    CompletableFuture<ByteBuffer>[] futures = new CompletableFuture[count];
    ByteBuffer[] buffers = new ByteBuffer[count];

    for (int i = 0; i < count; i++) {
        buffers[i] = ByteBuffer.allocateDirect(lengths[i]);
        futures[i] = new CompletableFuture<>();
    }

    // Submit all as a batch to the io_uring ring
    ring.prepareReads(fd, positions, buffers);
    ring.submit();  // single syscall for all N reads

    // Process completions
    ring.awaitCompletions(count, (index, result) -> {
        if (result >= 0) {
            buffers[index].flip();
            futures[index].complete(buffers[index]);
        } else {
            futures[index].completeExceptionally(
                new IOException("io_uring read failed: " + result));
        }
    });

    return futures;
}
```

**Where to use batch reads**:

1. **NonLeaf prefetch** (§4.3): Instead of submitting individual prefetch tasks to a thread pool, collect all child positions and submit them as one io_uring batch. Zero thread overhead, one syscall.

2. **Chunk ToC + first pages**: When accessing a new chunk, batch the ToC read with the first few page reads.

3. **Cursor readahead**: Batch the next N leaf positions into one submission.

**Performance model**: For a NonLeaf with 48 children, the current approach would be 48 `pread64` syscalls. With io_uring, it's 1 `io_uring_enter` syscall that submits 48 SQEs. On a modern NVMe drive with queue depth 32+, the drive can service these in parallel at the hardware level.

### 4.7 Strategy 5: Parallel Page Deserialization

After the I/O completes, `Page.read()` does:
1. Header/checksum validation (cheap)
2. Decompression — LZF or Deflate (expensive if enabled)
3. `KeyType.read()` / `ValueType.read()` — cost depends on data types

When compression is enabled, deserialization is CPU-bound and a good parallelism target. This dovetails with the io_uring approach: I/O completions arrive asynchronously, and each completion can trigger deserialization on a worker thread:

```java
ring.awaitCompletions(count, (index, result) -> {
    // I/O complete — submit deserialization to ForkJoinPool
    deserializationPool.submit(() -> {
        Page<K,V> page = Page.read(buffers[index], positions[index], map);
        cachePage(page);
    });
});
```


### 4.8 Read Parallelism Summary

| Strategy | Latency Impact | Throughput Impact | Complexity | Best For |
|----------|---------------|-------------------|------------|----------|
| NonLeaf prefetch | Medium — hides I/O behind processing | High — saturates device queue | Low | General workloads |
| Cursor readahead | High — halves effective latency for scans | Medium | Low | Sequential scans |
| Chunk buffering | High — eliminates per-page I/O | High for intra-chunk locality | Low | Scan-heavy workloads |
| io_uring batching | Medium — reduces syscall overhead | Very high — max device utilization | Medium (JUring dependency) | High-IOPS NVMe |
| Parallel deser. | Medium — hides CPU cost | Medium | Low | Compressed pages |

The recommended implementation order:
1. **Chunk buffering** — lowest risk, already half-implemented via `Chunk.buffer`
2. **Cursor readahead** — simple one-ahead prefetch in the cursor
3. **NonLeaf prefetch** — configurable prefetch window
4. **io_uring batching** — in the JUring-specific FileStore subclass
5. **Parallel deserialization** — only if profiling shows CPU bottleneck on decompression


## 5. Phase 3 Design: Multi-Chunk Parallel Writes

For very high-throughput scenarios, we can split dirty pages across multiple chunks and write them in parallel.

### 5.1 Pre-allocation of Multiple Chunks

```java
// Before serialization, pre-allocate N chunk slots
List<ChunkSlot> slots = new ArrayList<>();
saveChunkLock.lock();
try {
    for (int i = 0; i < parallelism; i++) {
        C chunk = createChunk(time, version);
        chunks.put(chunk.id, chunk);
        // Pre-allocate file space based on estimated size
        long estimatedSize = estimateChunkSize(batchSize);
        long filePos = freeSpace.allocate(estimatedSize, 0, 0);
        chunk.block = filePos / BLOCK_SIZE;
        slots.add(new ChunkSlot(chunk, filePos, estimatedSize));
    }
} finally {
    saveChunkLock.unlock();
}
```

### 5.2 Consistency: The Commit Record

With multiple chunks per version, we need a way to atomically validate that all chunks for a version were successfully written. The store header update (which currently points to `lastChunk`) becomes a **commit record** that references all chunks for that version:

```
Store Header:
  version: N
  chunks: [id1, id2, id3]    // all chunks for version N
  committed: true
```

On recovery, if any chunk in the list is missing or corrupt, the entire version N is rolled back.

### 5.3 Layout Map Challenge

The layout map is a single B-tree that records all chunk metadata and root positions. In the current design, it's serialized into the last chunk of each version. With multiple chunks, we have two options:

**Option A**: Designate one chunk as the "leader" that contains the layout map. Other chunks are "data-only". The leader chunk is written last, acting as the commit point.

**Option B**: Each chunk gets a partial layout map recording only the roots it contains. A merge step during recovery reconstructs the full layout.

Option A is simpler and preserves the existing recovery model.


## 6. Implementation Roadmap

### Step 1: Refactor `PageSerializationManager` (Low Risk)

Extract the `WriteBuffer` and `toc` into a composable unit that can be instantiated per-map. This is pure refactoring — no behavior change.

**Files modified**: `FileStore.java` (inner class), `Page.java` (signature change)

### Step 2: Chunk Buffering for Reads (Low Risk)

Populate `Chunk.buffer` on first read access to a chunk (with size threshold and TTL eviction). The deserialization path in `readBufferForPage()` already handles `buffer != null`.

**Files modified**: `FileStore.java` (readPage), `Chunk.java` (buffer lifecycle)

**Key invariant**: `chunk.buffer` must be set atomically (it's already volatile). Eviction must not race with in-progress slicing — use reference counting or copy-on-read.

### Step 3: Cursor Readahead (Low Risk)

One-ahead prefetch in the Cursor: when advancing to the next leaf, submit a prefetch for the leaf after that. Requires a prefetch executor (shared `ForkJoinPool` or dedicated).

**Files modified**: Cursor class, `FileStore.java` (new `submitPrefetch()` method)

### Step 4: NonLeaf Prefetch with Configurable Window (Low-Medium Risk)

After deserializing a NonLeaf page, prefetch N children that aren't in cache. Window size is configurable per storage medium.

**Files modified**: `FileStore.java` (readPage), new `PrefetchPolicy` configuration

### Step 5: Parallel Map Serialization with Post-merge Fixup (Medium Risk)

Implement the two-pass write approach from §3.3. Gate behind a `parallelSerialize` flag so the serial path remains as fallback.

**Files modified**: `FileStore.java` (serializeToBuffer), `Page.java` (pos fixup support)

**Key invariant to preserve**: After serialization, `page.getPos()` must return a valid position that can be used by `readPage()` to find the page in the chunk.

### Step 6: io_uring Batch Reads via JUring (Medium Risk)

Implement `JUringFileStore` as a subclass of `SingleFileStore` that uses io_uring for batched positional reads. Integrate with the NonLeaf prefetch and Cursor readahead from Steps 3–4.

**Files modified**: New `JUringFileStore.java`, `JUringChunk.java`; adapt prefetch call sites to use batch API when available.

### Step 7: Multi-Chunk Parallel Writes (High Risk)

This fundamentally changes the storage model. Should only be attempted after Steps 1–6 are stable.

**Files modified**: `FileStore.java`, `RandomAccessStore.java`, `Chunk.java`, `MVStore.java`


## 7. Risk Analysis

| Risk | Mitigation |
|------|-----------|
| **Writes** | |
| Page position encoding breaks after buffer merge | Two-pass design: serialize locally, fixup globally. Extensive unit tests on position roundtrip. |
| NonLeaf child position patching race | Children are serialized in the same local buffer as their parent — patching is buffer-local, no cross-buffer fixups needed within a single map tree. |
| Layout map references pages from parallel buffers | Layout map is always serialized last, in the serial assembly phase, after all page positions are finalized. |
| FreeSpaceBitSet contention on multi-chunk allocation | Pre-allocate all chunk slots under a single lock acquisition. |
| Recovery with partial multi-chunk writes | Leader-chunk model: recovery only considers versions where the leader chunk (containing layout map) is valid. |
| Memory pressure from multiple in-flight write buffers | Pool `WriteBuffer` instances, limit parallelism to `min(numMaps, availableCores)`, and fall back to serial when heap is tight. |
| **Reads** | |
| Chunk.buffer eviction races with concurrent slicing | The buffer is already volatile. Use `ByteBuffer.duplicate()` before slicing (already done in `readBufferForPage`). The duplicate holds a reference that prevents premature GC. Setting `chunk.buffer = null` for eviction is safe because `readBufferForPage` re-reads from disk on retry if `originalBlock == block` check fails. |
| Prefetch causes memory pressure from too many cached pages | Bound the prefetch window size. Prefetched pages enter the same LRU cache as demand-loaded pages, so they'll be evicted naturally. Add a cache pressure check before prefetching. |
| Prefetch threads contend with demand-read threads on FileChannel | `FileChannel.read(buf, pos)` is synchronized on the channel for overlapping regions. Use `O_DIRECT` + io_uring for true parallelism, or accept that prefetch I/O may serialize with demand I/O at the channel level (still a win because it hides latency). |
| Chunk buffering doubles memory for cached pages | Pages read from `chunk.buffer` are still individually cached. Set a conservative `chunkBufferThreshold` (e.g., 256 KB) and short TTL (e.g., 5 seconds). Only buffer chunks that are being actively scanned. |
| io_uring completion ordering | Completions arrive out of order. Each SQE carries a user_data tag mapping back to the original (position, buffer) pair. No ordering assumptions in the completion handler. |
| Prefetch of pages that are never actually needed | Wasted I/O and memory. The cursor readahead (1-ahead) is conservative. NonLeaf prefetch should be limited to leaves (not recursive) and gated on the cache hit rate — if hit rate is already high, prefetching adds no value. |


## 8. Starting Point: The Refactored `PageSerializationManager`

Here is the concrete first change — splitting `PageSerializationManager` into a composable unit:

```java
// New: Standalone serialization context, not tied to FileStore inner class
public final class PageSerializationContext {
    private final int chunkId;
    private final WriteBuffer buff;
    private final List<Long> toc = new ArrayList<>();
    private final BiConsumer<Page<?,?>, Integer> onPageSerialized;

    public PageSerializationContext(int chunkId, WriteBuffer buff,
                                    BiConsumer<Page<?,?>, Integer> onPageSerialized) {
        this.chunkId = chunkId;
        this.buff = buff;
        this.onPageSerialized = onPageSerialized;
    }

    public WriteBuffer getBuffer() { return buff; }

    public int getPageNo() { return toc.size(); }

    public long getPagePosition(int mapId, int offset, int pageLength, int type) {
        long tocElement = DataUtils.composeTocElement(mapId, offset, pageLength, type);
        toc.add(tocElement);
        long pagePos = DataUtils.composePagePos(chunkId, tocElement);
        int check = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        buff.putInt(offset, pageLength)
            .putShort(offset + 4, (short) check);
        return pagePos;
    }

    public List<Long> getToc() { return toc; }

    public void notifyPageSerialized(Page<?,?> page, int diskSpaceUsed) {
        onPageSerialized.accept(page, diskSpaceUsed);
    }
}
```

This is the foundation that enables everything else. The existing `FileStore.PageSerializationManager` becomes a thin wrapper that delegates to this context while maintaining backward compatibility with the caching and accounting logic.
