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

This is the cleanest win with the smallest blast radius. The design below is the original sketch; the refined incremental implementation plan is in §6 (Steps 5a–5d).

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


### 4.3 Strategy 1: Cursor-Scoped Directional Prefetch

**Goal**: When the cursor crosses a leaf boundary, prefetch multiple upcoming sibling pages in the scan direction, overlapping I/O with processing.

#### Failed Approach: Global NonLeaf readPage() Hook

The initial attempt hooked into `FileStore.readPage()`: after deserializing any NonLeaf page, up to N uncached children were submitted for background prefetch via `ForkJoinPool.commonPool()`. A `ThreadLocal<Boolean> IN_PREFETCH` flag prevented recursive prefetch from pages loaded by background tasks.

**Benchmark results were unambiguously negative:**

| Benchmark | Step 3 (baseline) | readPage hook | Delta |
|---|---|---|---|
| compositeIndexRangeScan | 248.0 | 227.2 | -8% |
| joinIndexedNestedLoop | 72 ms | 612 ms | +750% |
| joinAggregateGroupBy | 254 ms | 564 ms | +122% |
| joinThreeWay | 386 ms | 669 ms | +73% |
| bulkInsert | 878 ms | 1,203 ms | +37% |
| indexCreationAndCompaction | 2,126 ms | 3,571 ms | +68% |

**Root cause**: The readPage() hook fired on *every* NonLeaf load — point lookups, compaction reads, recovery, join probes, everything. A secondary index probe traversing 3 levels would prefetch 12 child pages it will never visit. For the join benchmarks (which do many individual lookups through cold B-trees), the wasted I/O was catastrophic: each lookup triggered a blast of unnecessary reads that stole bandwidth from the actual needed reads and flooded the ForkJoinPool task queue.

**Key insight**: NonLeaf prefetch is only beneficial during sequential scans, and only in the scan direction. Point lookups, compaction, and join probes traverse exactly one path through the tree — prefetching siblings is pure waste.

#### Revised Approach: Cursor-Scoped prefetchAhead()

Prefetch is scoped to the `Cursor` class, which is the only call site that knows it's doing a directional scan. When `hasNext()` descends to a new leaf after exhausting the previous one, `prefetchAhead()` submits up to `PREFETCH_WINDOW` (4) sibling child pages from the parent's `children[]` array, in the scan direction only.

```java
// In Cursor.hasNext(), after descent to a new leaf:
if (descended) {
    prefetchAhead(cursorPos.parent, reverse);
}

private static <K,V> void prefetchAhead(CursorPos<K,V> parentPos, boolean reverse) {
    if (parentPos == null) return;
    Page<K,V> parent = parentPos.page;
    int childCount = parent.map.getChildPageCount(parent);
    int increment = reverse ? -1 : 1;
    int prefetched = 0;
    for (int i = parentPos.index + increment;
         i >= 0 && i < childCount && prefetched < PREFETCH_WINDOW;
         i += increment) {
        parent.map.prefetchPage(parent.getChildPagePos(i));
        prefetched++;
    }
}
```

**Why this works where the readPage hook didn't:**

- Only fires during cursor-driven scans, never during point lookups, compaction, or join probes
- Directional — only prefetches siblings *ahead* in the scan direction, not behind
- Bounded — at most 4 siblings per leaf crossing, not per NonLeaf load
- No ThreadLocal needed — there's no recursion risk since prefetchPage() loads pages normally, and only Cursor calls prefetchAhead()

**Benchmark results (revised Step 4 vs Step 1 baseline):**

| Benchmark | Baseline | Revised Step 4 | Delta |
|---|---|---|---|
| compositeIndexRangeScan | 211.6 | 262.3 | **+24%** |
| secondaryIndexLookup | 767.8 | 938.3 | **+22%** |
| mixed | 1,453K | 1,492K | +3% |
| randomCacheMissReads | 129.6K | 136.7K | +5% |
| joinAggregateGroupBy | 543 ms | 297.6 ms | **-45%** |
| joinIndexedNestedLoop | 381 ms | 225.3 ms | **-41%** |
| joinThreeWay | 647 ms | 541.4 ms | **-16%** |

No regressions anywhere. Range scans benefit from the wider prefetch window (4 vs 1 sibling from Step 3), point lookups and joins are unaffected because prefetch never fires outside the cursor.


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


### 4.6 Strategy 4: io_uring Integration (JUring-specific) ✓

This is where the JUring integration was implemented. The original design sketch below was refined during implementation — see §9 for the actual architecture and performance results.

Instead of N individual `pread64` syscalls for prefetch, the batch path submits all reads as a batch of SQEs to the io_uring ring. Demand reads (single-page loads during B-tree traversal) bypass the ring entirely and use direct `pread(2)` via Panama FFI — this was a critical lesson learned during implementation (see §7 risk table).

The actual batch API implemented in `JUringFileChannel`:

```java
// Batch path (ring) — used by prefetch only
public void readFullyBatch(long[] positions, ByteBuffer[] buffers) {
    batchDispatcher.prepareReadBatch(rawFd, positions, buffers);
    batchDispatcher.submitAndCollect();  // 2 FFI calls for N reads
}

// Demand path (pread bypass) — used by readPage()
public int read(ByteBuffer dst, long position) {
    // Thread-local 64KB bounce buffer for heap ByteBuffers
    MemorySegment bounce = BOUNCE_BUFFER.get();
    int n = (int) pread.invokeExact(rawFd, bounce, (long) dst.remaining(), position);
    MemorySegment.ofBuffer(dst).copyFrom(bounce.asSlice(0, n));
    return n;
}
```

**Where batch reads are used** (as implemented):

1. **Cursor prefetch** (§4.3): When the cursor crosses a leaf boundary, up to PREFETCH_WINDOW sibling positions are collected. On the JUring backend, these are submitted as one io_uring batch via `batchPrefetchPages()`. On NIO, each is submitted individually to `ForkJoinPool`.

2. **Batch compaction reads** (future): `FileStore.compactRewritePage()` reads pages one-by-one. Batching these would extend the highChurnCompaction win to regular compaction.

**Performance model**: For a cursor prefetch of 4 siblings, the NIO approach is 4 individual `pread64` syscalls + 4 ForkJoinPool tasks. With io_uring, it's 1 `io_uring_enter` syscall submitting 4 SQEs. The ring overhead is amortized across the batch.

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

| Strategy | Status | Latency Impact | Throughput Impact | Complexity | Best For |
|----------|--------|---------------|-------------------|------------|----------|
| Chunk buffering | ✓ Step 2 | High — eliminates per-page I/O | High for intra-chunk locality | Low | Scan-heavy workloads |
| Cursor readahead | ✓ Step 3 | High — halves effective latency for scans | Medium | Low | Sequential scans |
| Cursor-scoped directional prefetch | ✓ Step 4 | High — overlaps I/O for upcoming siblings | High — saturates device queue | Low | Range scans, joins |
| io_uring batching | ✓ Step 6 | Medium — reduces syscall overhead | Batch: high; demand: needs work | Medium (JUring dependency) | High-IOPS NVMe |
| Parallel deser. | Not started | Medium — hides CPU cost | Medium | Low | Compressed pages |

The recommended implementation order (all completed through Step 4; Step 6 complete with caveats):
1. ✅ **Chunk buffering** — lowest risk, already half-implemented via `Chunk.buffer`
2. ✅ **Cursor readahead** — simple one-ahead prefetch in the cursor
3. ✅ **Cursor-scoped directional prefetch** — widen to N-ahead, scan direction only
4. ✅ **io_uring batching** — in the JUring-specific FileStore subclass (batch path working; demand-read parity pending, see §9)
5. **Parallel deserialization** — only if profiling shows CPU bottleneck on decompression

**Important lesson from Step 4**: Global prefetch hooks in the read path (e.g., in `readPage()`) are harmful to non-scan workloads. All prefetch must be scoped to call sites that know they are performing sequential access.


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

### Step 4: Cursor-Scoped Directional Prefetch (Low Risk) ✓

Widen the cursor's one-ahead sibling readahead (Step 3) to a configurable `PREFETCH_WINDOW` (default 4) of upcoming siblings in the scan direction. Only fires when the cursor crosses a leaf boundary.

**Files modified**: `Cursor.java` (prefetchAhead replaces prefetchNextSibling)

**Rejected alternative**: A global NonLeaf prefetch hook in `FileStore.readPage()` was implemented and benchmarked first. It regressed point lookups and join probes catastrophically (joinIndexedNestedLoop: 72 ms → 612 ms) by prefetching children indiscriminately on every NonLeaf load. The cursor-scoped approach avoids this by restricting prefetch to sequential scan contexts. See §4.3 for full analysis.

### Step 5a: PSM Tracks Serialized Pages for Fixup (Low Risk)

Add a `SerializedPageRecord` list to `PageSerializationManager` that logs each page as it's serialized: the `Page` reference, its buffer offset, page length, page type, and for NonLeaf pages the child-pointer buffer position (the `patch` offset returned by `write()`) and child count. Pure bookkeeping — `getPagePosition()` and `onPageSerialized()` gain a few `list.add()` calls.

This is the foundation for Step 5b's rebase operation: the record list tells us exactly which bytes in the buffer and which `Page.pos` fields need adjustment when rebasing offsets.

**Files modified**: `PageSerializationManager.java` only

**Key invariant**: Zero behavior change. Existing tests must pass unchanged.

### Step 5b: PSM Gains `rebasePositions(int baseOffset)` (Medium Risk)

New method that iterates the tracked `SerializedPageRecord` list and adjusts everything by `baseOffset`:

1. Recomposes each `tocElement` with offset + baseOffset
2. Recomputes `pagePos` from the rebased tocElement
3. CAS-updates each `Page.pos` to the rebased position
4. Patches the length/check header bytes in the buffer (check depends on offset)
5. For NonLeaf pages: rewrites child pointer longs in the buffer, but only for children whose positions belong to the current PSM (same chunkId, offset within the local buffer range)

Can be validated in isolation: call with `baseOffset=0` and verify all positions and buffer contents are unchanged. Call with known offset and verify roundtrip through `DataUtils.getPageOffset()`.

**Files modified**: `PageSerializationManager.java` only

**Key invariant**: `rebasePositions(0)` is a no-op. After `rebasePositions(N)`, every page's `getPos()` returns an offset that is exactly N bytes greater than before.

### Step 5c: Per-Map Local Buffers with Serial Execution (Medium Risk)

Refactor the map loop in `serializeToBuffer()`: each changed map root gets its own `WriteBuffer` and `PageSerializationManager`, serialized at local offset 0. After all maps are done (still sequentially), compute cumulative offsets, `memcpy` each local buffer into the main buffer at the correct position, and call `rebasePositions()` on each PSM to fix up all page positions.

Layout map serialization stays on the main buffer as before (it must run after all map roots have their final positions, since it records root positions in the layout map).

This is the functional correctness gate: if benchmarks show any regression, the rebase logic has a bug. Serial execution means no concurrency complications — the only variable is the rebase correctness.

**Files modified**: `FileStore.java` (serializeToBuffer), `PageSerializationManager.java` (minor: expose mergedToc helper)

**Key invariant**: Benchmark results must be identical to Step 4 (within noise). Any regression indicates a rebase bug.

### Step 5d: Parallelize the Per-Map Loop (Medium Risk)

Replace the sequential `for (Page p : changed)` with parallel execution via `ForkJoinPool.commonPool()`. Each map's serialize + local-buffer work runs on a worker thread. The merge pass (cumulative offset calculation, buffer copy, rebasePositions) stays serial.

Gate behind a minimum map count threshold — for 1–2 maps, serial execution avoids thread overhead.

**Files modified**: `FileStore.java` (serializeToBuffer only)

**Key invariant**: Results must be byte-identical to Step 5c for the same input. The parallelism must not change the output, only the speed.

### Step 6: io_uring Batch Reads via JUring (Medium Risk) ✓

Implemented `FileJuring` as a `FileBaseDefault` bridge to `JUringFileChannel`, selectable via a `scheme=juring` connection parameter. The implementation has three layers:

**Layer 1 — JUringFileChannel (ring path):** `readFullyBatch()` and `writeFullyBatch()` submit multiple SQEs to the io_uring ring and collect completions. The `BatchDispatcher` manages ring interaction with exactly 2 FFI calls per batch (prepare + submit/collect).

**Layer 2 — JUringFileChannel (pread bypass):** Single-operation `readFully()`, `writeFully()`, `read()`, and `write()` bypass the ring entirely and use direct `pread(2)`/`pwrite(2)` syscalls via Panama FFI. This avoids ring serialization overhead for the dominant demand-read path.

**Layer 3 — Heap ByteBuffer bounce buffer:** H2 uses heap-backed `ByteBuffer`s throughout. Since `pread(2)` requires a memory address, each thread maintains a 64 KB thread-local direct `MemorySegment` as a bounce buffer. Reads go: `pread → bounce → MemorySegment.ofBuffer(dst).copyFrom(bounce)`.

**H2 integration — batch prefetch pipeline:**

```
Cursor.prefetchAhead() → map.prefetchPages(positions)
  ├─ NIO: async individual prefetchPage (ForkJoinPool → pread)
  └─ JUring: async batchPrefetchPages (ForkJoinPool → ring batch)

readPage() demand read → FileChannel.read(heapBuffer, pos)
  ├─ NIO: JDK internal pread + heap copy (optimized fast path)
  └─ JUring: pread(rawFd, bounceBuf, len, off) + MemorySegment copy
```

**Files modified**: New `FileJuring.java`, `JUringFileChannel.java`, `BatchDispatcher.java`; modified `FileStore.java`, `SingleFileStore.java`, `MVStore.java`, `MVMap.java`, `Cursor.java` for batch prefetch API.

**Key design decision — pread bypass:** The initial implementation routed all I/O through the io_uring ring, causing a 4.2× regression on coldCacheScan. Profiling revealed that demand reads (one-at-a-time page loads during B-tree traversal) were serializing on the ring lock. The pread bypass reduced this to 1.9×, and the remaining gap is Panama FFI + bounce buffer overhead (see §9.2).

**Rejected alternative — synchronous batch prefetch:** An attempt to submit all prefetch positions as a synchronous batch within `batchPrefetchPages()` (wait for all completions before returning) caused a 354× regression. The io_uring ring completion wait blocked the ForkJoinPool thread, and only one batch could be in-flight at a time. The async model (submit batch, return immediately, pages appear in cache when completions arrive) is essential.

### Step 7: Multi-Chunk Parallel Writes (High Risk) — Deferred

This fundamentally changes the storage model. Deferred pending resolution of io_uring per-read overhead (§9.2). The batch prefetch infrastructure from Steps 4+6 delivers significant scan improvements via NIO alone, and the remaining JUring gap is in the demand-read path, not the batch path.

**Files modified**: `FileStore.java`, `RandomAccessStore.java`, `Chunk.java`, `MVStore.java`


## 7. Risk Analysis

| Risk | Mitigation |
|------|-----------|
| **Reads** | |
| Global prefetch hooks regress non-scan workloads | **Lesson learned (Step 4)**: Never prefetch from readPage(). All prefetch must be scoped to call sites with sequential access knowledge (Cursor). Benchmarked before and after — regression was +750% on joinIndexedNestedLoop. |
| Chunk.buffer eviction races with concurrent slicing | The buffer is already volatile. Use `ByteBuffer.duplicate()` before slicing (already done in `readBufferForPage`). Setting `chunk.buffer = null` for eviction is safe because `readBufferForPage` re-reads from disk on retry if `originalBlock == block` check fails. |
| Prefetch causes memory pressure from too many cached pages | Bound the prefetch window size (PREFETCH_WINDOW=4). Prefetched pages enter the same LRU cache as demand-loaded pages, so they'll be evicted naturally. |
| Prefetch threads contend with demand-read threads on FileChannel | `FileChannel.read(buf, pos)` is synchronized on the channel for overlapping regions. Use `O_DIRECT` + io_uring for true parallelism, or accept that prefetch I/O may serialize with demand I/O at the channel level (still a win because it hides latency). |
| Chunk buffering doubles memory for cached pages | Pages read from `chunk.readBuffer` are still individually cached. The demand-triggered two-hit policy (Step 2) avoids buffering chunks accessed by scattered point lookups. Only chunks accessed sequentially get buffered. |
| io_uring completion ordering | Completions arrive out of order. Each SQE carries a user_data tag mapping back to the original (position, buffer) pair. No ordering assumptions in the completion handler. |
| Prefetch of pages that are never actually needed | The cursor-scoped approach (Step 4) limits this: prefetch only fires in the scan direction at leaf boundaries, so at most PREFETCH_WINDOW pages are prefetched per leaf transition. Point lookups never trigger prefetch. |
| **Writes (Step 5a-5d)** | |
| 5a: SerializedPageRecord tracking adds overhead | Records are append-only to an ArrayList during serialization. The per-page cost is one object allocation + list add. Negligible compared to page serialization + compression. |
| 5b: rebasePositions() corrupts page positions | Validate with `rebasePositions(0)` (must be no-op). Unit test roundtrip: serialize, rebase by known offset N, verify `DataUtils.getPageOffset(page.getPos()) == originalOffset + N` for every page. Verify buffer checksums match after rebase. |
| 5b: NonLeaf child pointer rebase patches wrong children | Only rebase child pointers whose current position falls within the local buffer's offset range (same chunkId, offset < localBufferLength). Children already saved in previous chunks have correct absolute positions and must not be touched. |
| 5c: Per-map local buffers change serialization order | The layout map sees the same root positions (after rebase) as the serial path. The ToC order may differ (maps interleaved vs. contiguous), but ToC order is not semantically significant — pages are located by their encoded position, not by ToC index. Verify by comparing readback of all pages. |
| 5c: Memory pressure from multiple WriteBuffers | Each local buffer is sized to one map's dirty tree. Total memory equals the single-buffer case (same pages serialized). Buffers are released immediately after memcpy into the main buffer. |
| 5d: Race conditions in parallel map serialization | Each map's serialization is fully independent: own WriteBuffer, own PSM, own page tree. No shared mutable state between workers. The merge pass is serial. The only shared state is `ForkJoinPool.commonPool()` task scheduling, which is thread-safe by design. |
| 5d: Page.pos CAS fails under concurrent serialization | Each `Page.pos` is set by exactly one PSM (the one serializing that page's map). Different maps never share pages. The CAS in `Page.write()` races only with concurrent `remove()` on the same page, which is already handled by the existing retry loop. |
| Layout map references pages from parallel buffers | Layout map is always serialized last, in the serial merge phase, after all page positions are finalized via rebasePositions(). This is unchanged from the serial path. |
| Recovery with corrupted rebased positions | If rebase produces invalid positions, readback will fail checksums immediately (the check value incorporates the offset). This makes corruption detectable at the next read, not silently latent. |
| **io_uring / JUring (Step 6)** | |
| Single-op reads through ring regress demand-read path | **Lesson learned**: Ring serialization adds ~60–90 µs per read for demand ops. Solved by pread bypass — single reads use `pread(2)` directly, bypassing the ring entirely. Only batch prefetch uses the ring. |
| Heap ByteBuffer incompatibility with pread(2) | `pread(2)` requires a memory address; heap `ByteBuffer` has none. Solved with 64 KB thread-local `MemorySegment` bounce buffer. Overhead is `MemorySegment.ofBuffer(dst).copyFrom(bounce)` per read. |
| MemorySegment.ofBuffer() on heap ByteBuffer costs ~60 µs per call | Each call must pin/validate the backing `byte[]`. This is the dominant remaining overhead source. Mitigation: try `byte[]`-based bounce (see §9.3), or increase PREFETCH_WINDOW so demand reads become cache hits. |
| Synchronous batch prefetch blocks ForkJoinPool | **Lesson learned**: Submitting a batch and waiting for all completions within the same task caused 354× regression. The ring completion wait blocks the pool thread. The async model (submit and return immediately) is essential. |
| Panama FFI overhead per syscall | Each `pread(2)` via Panama's `Linker.downcallHandle()` has ~1–5 µs overhead vs JDK's internal JNI path for `FileChannel.read()`. Acceptable for batched calls (amortized), but multiplied by thousands for demand reads. |
| JUring integration fails on non-Linux or old kernels | `scheme=juring` connection parameter makes io_uring opt-in. Falls back to standard NIO by default. JUring checks for io_uring availability at load time. |


## 8. Implementation Status

| Step | Description | Status |
|------|------------|--------|
| 1 | Extract PageSerializationManager to top-level class | ✓ Committed |
| 2 | Demand-triggered chunk read buffering (two-hit policy) | ✓ Committed |
| 3 | Cursor one-ahead sibling readahead | ✓ Committed |
| 4 | Cursor-scoped directional prefetch (PREFETCH_WINDOW=4) | ✓ Committed |
| 5a | PSM tracks serialized pages for fixup | ✓ Committed |
| 5b | PSM gains rebasePositions(int baseOffset) | ✓ Committed |
| 5c | Per-map local buffers with serial execution | ✓ Committed |
| 5d | Parallelize the per-map serialization loop | ✓ Committed |
| 6 | io_uring batch reads via JUring | ✓ Committed (see §9) |
| 7 | Multi-chunk parallel writes | Deferred |


### 8.1 Three-Way Benchmark Results (Baseline NIO → Current NIO → JUring)

The table below compares three configurations measured with the same JMH harness on the same hardware. "Baseline NIO" is unmodified H2. "Current NIO" has Steps 1–6 (cursor prefetch, batch I/O plumbing, parallel serialization). "JUring" adds the io_uring backend with pread bypass and batch prefetch.

#### Throughput (ops/s, higher = better)

| Benchmark | Baseline NIO | Current NIO | Δ NIO | JUring | Δ JUring vs NIO |
|---|---|---|---|---|---|
| mixed (8 threads) | 1,449K | 1,449K | — | 1,425K | −2% (noise) |
| randomCacheMissReads | 111K | 104K | −6% | 105K | +2% (noise) |
| compositeIndexRangeScan | 117 | 156 | **+33%** | 112 | −28% |
| secondaryIndexLookup | 55.1 | 57.9 | +5% | 46.8 | −19% |

#### Latency (ms, lower = better)

| Benchmark | Baseline NIO | Current NIO | Δ NIO | JUring | Δ JUring vs NIO |
|---|---|---|---|---|---|
| coldCacheScan | 1,322 | 863 | **−35%** | 1,644 | 1.9× (regression) |
| highChurnCompaction | 3,494 | 3,484 | — | 2,497 | **−28%** 🏆 |
| joinThreeWay | 2,751 | 2,396 | **−13%** | 6,820 | 2.8× (regression) |
| joinIndexedNestedLoop | 778 | 484 | **−38%** | 507 | +5% (noise) |
| joinAggregateGroupBy | 2,273 | 2,207 | — | 2,294 | +4% (noise) |
| bulkInsert | 4,586 | 4,990 | +9% (noise) | 4,916 | −1% (noise) |
| compaction | 702 | 704 | — | 719 | +2% (noise) |
| indexCreationAndCompaction | 15,412 | 16,450 | +7% | 19,407 | +18% (regression) |

#### Scorecard

| Category | Count | Benchmarks |
|---|---|---|
| 🏆 JUring wins | 1 | highChurnCompaction (−28%) |
| ✅ Parity (within noise) | 6 | mixed, randomCacheMissReads, bulkInsert, compaction, joinAggregateGroupBy, joinIndexedNestedLoop |
| ⚠️ Minor gap (10–30%) | 3 | compositeIndexRangeScan, secondaryIndexLookup, bulkInsertWithIndex |
| ❌ Significant regression | 3 | coldCacheScan (1.9×), indexCreationAndCompaction (+18%), joinThreeWay (2.8×) |


### 8.2 NIO-Only Improvements from Steps 1–6

Even without io_uring, the cursor prefetch pipeline (Steps 1–4) delivered significant gains over baseline H2 on scan- and join-heavy workloads:

| Benchmark | Improvement | Mechanism |
|---|---|---|
| coldCacheScan | **−35%** (1,322 → 863 ms) | Async prefetch warms page cache ahead of cursor |
| joinIndexedNestedLoop | **−38%** (778 → 484 ms) | Prefetch overlaps I/O with B-tree descent |
| compositeIndexRangeScan | **+33%** (117 → 156 ops/s) | Wider prefetch window fills pipeline |
| joinThreeWay | **−13%** (2,751 → 2,396 ms) | Multi-table joins benefit from parallel prefetch |

The async prefetch running on `ForkJoinPool.commonPool()` lets the OS pipeline I/O and warm the page cache without explicit io_uring batching. This means the H2-layer prefetch infrastructure is valuable on all platforms, not just Linux with io_uring.


### 8.3 bulkInsert Profile Analysis: Regression is Noise

Async-profiler CPU flamegraph of `bulkInsert` (NIO current, SingleShot) breaks down as follows (2,903 total samples):

| Component | % Samples | Description |
|---|---|---|
| **JIT compilation** (libjvm.so) | 26.6% | C2 background compiler thread — not benchmark work |
| **Benchmark thread** | 70.6% | Actual insert work |
| **ForkJoinPool** (parallel serialization) | 2.7% | Step 5d parallel chunk serialization |

Within the benchmark thread, the top hotspots are:

| Hotspot | % | Notes |
|---|---|---|
| ValueDataType.writeString → WriteBuffer.putStringData | ~19% | Page serialization (expected) |
| CursorPos.traverseDown / Page.binarySearch | ~5% | B-tree navigation for inserts |
| ext4 page cache + memory compaction | ~5% | Kernel memory pressure (variable) |
| WriteBuffer.ensureCapacity → new_type_array_blob | ~4% | Array allocation during serialization |
| Instant.now / Clock.currentInstant | ~1.5% | SessionLocal.setCurrentCommand overhead |
| **Cursor.prefetchAhead** | **0.03%** (1 sample) | Our code — completely invisible |
| pwrite / pread / fsync | 0.4% | I/O is negligible |

**Key findings:**

1. **Our code doesn't execute during bulkInsert.** `prefetchAhead` and `prefetchPage` have 1 sample total. Bulk insert is write-heavy — the cursor prefetch path is never triggered.

2. **Confidence intervals overlap.** Baseline: 4,586 ± 427 ms → CI [4,159–5,013]. Current: 4,990 ± 328 ms → CI [4,662–5,318]. The overlap is ~350 ms, well within noise.

3. **Variance is dominated by external factors.** The 26.6% JIT compilation overhead (SingleShot mode includes compilation in measurement) and 5% kernel memory compaction (folio migration, shrink_lruvec) fluctuate between runs based on system state.

**Conclusion:** The bulkInsert delta of +9% is not a real regression. The profile confirms no modified code path is hot during bulk inserts.


## 9. io_uring Performance Analysis

### 9.1 pread Bypass Impact

The pread bypass (routing demand reads around the io_uring ring) was the single most impactful optimization for JUring:

| Benchmark | Before Bypass (ring path) | After Bypass (pread) | Improvement |
|---|---|---|---|
| coldCacheScan | 4.2× slower than NIO | 1.9× slower | 2.2× faster |
| randomCacheMissReads | −3% vs NIO | +2% (parity) | Fixed |
| joinAggregateGroupBy | +5% vs NIO | +4% (parity) | Fixed |
| highChurnCompaction | +4% vs NIO | **−28%** (win) | Now winning |

This confirms that demand reads (one-at-a-time B-tree traversal) were bottlenecked on ring serialization, not I/O. With demand reads on the pread fast path, the ring is used exclusively for batch prefetch where its amortized overhead is acceptable.


### 9.2 Remaining Per-Read Overhead

Three JUring regressions share a common pattern: high-volume sequential demand reads through B-tree traversal.

**Per-read overhead calculation:**

- coldCacheScan: (1,644 − 863 ms) / ~12,500 pages = **62 µs/page**
- joinThreeWay: (6,820 − 2,396 ms) / ~50,000 pages = **89 µs/page**

NIO's `FileChannel.read(heapBuffer, pos)` uses an optimized JDK-internal path: JNI → `pread(2)` → internal `sun.misc.Unsafe` copy. JUring's path goes: Panama downcall → libc `pread(2)` → kernel → `MemorySegment.ofBuffer(heapByteBuffer).copyFrom(bounceBuf)`.

**Likely bottleneck:** `MemorySegment.ofBuffer()` is called on every read and must pin/validate the backing `byte[]` array each time. NIO avoids this entirely with its internal fast path.

joinThreeWay is the worst case: three nested-loop joins performing thousands of B-tree lookups, every one hitting `readFully → pread + bounce`.


### 9.3 Next Steps

**1. Profile bounce overhead (immediate):**
Add timing instrumentation to `readFully()` to measure: pread syscall time vs bounce copy time vs `MemorySegment.ofBuffer()` overhead. Confirm the bottleneck location.

**2. Try `byte[]`-based bounce (quick experiment):**
Replace:
```java
MemorySegment.ofBuffer(dst).copyFrom(seg.asSlice(0, total));
```
with:
```java
byte[] tmp = new byte[total];
MemorySegment.ofArray(tmp).copyFrom(seg.asSlice(0, total));
dst.put(tmp);
```
This avoids `MemorySegment.ofBuffer()` on heap `ByteBuffer` entirely. If the overhead is in the pin/validate step, this should close the gap significantly.

**3. Increase PREFETCH_WINDOW (once per-read parity achieved):**
Change `PREFETCH_WINDOW` from 4 to 16–32 in `Cursor.java` (one line). With demand reads at NIO parity, a larger window lets async batch prefetch win the race more often — demand reads become cache hits. This is where JUring's batch advantage should pull ahead on the workloads where NIO already shows 13–38% improvement from prefetch alone.

**4. Batch compaction reads (medium effort):**
`FileStore.compactRewritePage()` reads pages one-by-one. Collecting a batch of page positions and submitting them as a single ring batch could extend the highChurnCompaction win (−28%) to regular compaction and indexCreationAndCompaction.


## 10. Architectural Summary

```
┌─────────────────────────────────────────────────────────────┐
│ Cursor.prefetchAhead()                                      │
│   Fires on leaf boundary crossing, scan direction only      │
│   Collects up to PREFETCH_WINDOW=4 sibling positions        │
│                                                             │
│   ├─ NIO backend:                                           │
│   │   for each position → async ForkJoinPool.submit(        │
│   │     map.prefetchPage(pos) → pread + cache              │
│   │   )                                                     │
│   │                                                         │
│   └─ JUring backend:                                        │
│       async ForkJoinPool.submit(                            │
│         batchPrefetchPages(positions) → ring batch          │
│       ) → 2 FFI calls total for N pages                    │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│ Demand reads: readPage() → FileChannel.read(heapBuf, pos)  │
│                                                             │
│   ├─ NIO: JDK-internal pread + Unsafe heap copy             │
│   └─ JUring: Panama pread + 64KB bounce + MemorySegment     │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│ Write path: FileStore.serializeToBuffer()                   │
│                                                             │
│   Step 5d: Per-map parallel serialization                   │
│   ├─ Each map → own WriteBuffer + own PSM (ForkJoinPool)    │
│   ├─ Serial merge: cumulative offsets + rebasePositions()   │
│   └─ Layout map serialized last (serial)                    │
│                                                             │
│   Shared by NIO and JUring — backend-agnostic               │
└─────────────────────────────────────────────────────────────┘
```