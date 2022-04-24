/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Class RandomAccessStore.
 * <UL>
 * <LI> 4/5/20 2:51 PM initial creation
 * </UL>
 *
 * @author <a href="mailto:andrei.tokar@gmail.com">Andrei Tokar</a>
 */
public abstract class RandomAccessStore extends FileStore<SFChunk> {
    /**
     * The free spaces between the chunks. The first block to use is block 2
     * (the first two blocks are the store header).
     */
    protected final FreeSpaceBitSet freeSpace = new FreeSpaceBitSet(2, BLOCK_SIZE);

    /**
     * Allocation mode:
     * false - new chunk is always allocated at the end of file
     * true - new chunk is allocated as close to the begining of file, as possible
     */
    private volatile boolean reuseSpace = true;

    private long reservedLow;
    private long reservedHigh;



    public RandomAccessStore(Map<String, Object> config) {
        super(config);
    }

    protected final SFChunk createChunk(int newChunkId) {
        return new SFChunk(newChunkId);
    }

    public SFChunk createChunk(String s) {
        return new SFChunk(s);
    }

    protected SFChunk createChunk(Map<String, String> map) {
        return new SFChunk(map);
    }

    /**
     * Mark the space as in use.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    public void markUsed(long pos, int length) {
        freeSpace.markUsed(pos, length);
    }

    /**
     * Allocate a number of blocks and mark them as used.
     *
     * @param length the number of bytes to allocate
     * @param reservedLow start block index of the reserved area (inclusive)
     * @param reservedHigh end block index of the reserved area (exclusive),
     *                     special value -1 means beginning of the infinite free area
     * @return the start position in bytes
     */
    private long allocate(int length, long reservedLow, long reservedHigh) {
        return freeSpace.allocate(length, reservedLow, reservedHigh);
    }

    /**
     * Calculate starting position of the prospective allocation.
     *
     * @param blocks the number of blocks to allocate
     * @param reservedLow start block index of the reserved area (inclusive)
     * @param reservedHigh end block index of the reserved area (exclusive),
     *                     special value -1 means beginning of the infinite free area
     * @return the starting block index
     */
    private long predictAllocation(int blocks, long reservedLow, long reservedHigh) {
        return freeSpace.predictAllocation(blocks, reservedLow, reservedHigh);
    }

    public boolean shouldSaveNow(int unsavedMemory, int autoCommitMemory) {
        return unsavedMemory > autoCommitMemory;
    }

    private boolean isFragmented() {
        return freeSpace.isFragmented();
    }

    public boolean isSpaceReused() {
        return reuseSpace;
    }

    public void setReuseSpace(boolean reuseSpace) {
        this.reuseSpace = reuseSpace;
    }

    protected void freeChunkSpace(Iterable<SFChunk> chunks) {
        for (SFChunk chunk : chunks) {
            freeChunkSpace(chunk);
        }
        assert validateFileLength(String.valueOf(chunks));
    }

    private void freeChunkSpace(SFChunk chunk) {
        long start = chunk.block * BLOCK_SIZE;
        int length = chunk.len * BLOCK_SIZE;
        free(start, length);
    }

    /**
     * Mark the space as free.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    protected void free(long pos, int length) {
        freeSpace.free(pos, length);
    }

    public int getFillRate() {
        saveChunkLock.lock();
        try {
            return freeSpace.getFillRate();
        } finally {
            saveChunkLock.unlock();
        }
    }

    @Override
    protected final boolean validateFileLength(String msg) {
        assert saveChunkLock.isHeldByCurrentThread();
        assert getFileLengthInUse() == measureFileLengthInUse() :
                getFileLengthInUse() + " != " + measureFileLengthInUse() + " " + msg;
        return true;
    }

    private long measureFileLengthInUse() {
        assert saveChunkLock.isHeldByCurrentThread();
        long size = 2;
        for (SFChunk c : getChunks().values()) {
            if (c.isAllocated()) {
                size = Math.max(size, c.block + c.len);
            }
        }
        return size * BLOCK_SIZE;
    }

    long getFirstFree() {
        return freeSpace.getFirstFree();
    }

    long getFileLengthInUse() {
        return freeSpace.getLastFree();
    }

    protected void allocateChunkSpace(SFChunk chunk, WriteBuffer buff) {
        int headerLength = (int) chunk.next;
        long reservedLow = this.reservedLow;
        long reservedHigh = this.reservedHigh > 0 ? this.reservedHigh : isSpaceReused() ? 0 : getAfterLastBlock();
        long filePos = allocate(buff.limit(), reservedLow, reservedHigh);
        // calculate and set the likely next position
        if (reservedLow > 0 || reservedHigh == reservedLow) {
            chunk.next = predictAllocation(chunk.len, 0, 0);
        } else {
            // just after this chunk
            chunk.next = 0;
        }

        buff.position(0);
        chunk.writeChunkHeader(buff, headerLength);

        buff.position(buff.limit() - Chunk.FOOTER_LENGTH);
        buff.put(chunk.getFooterBytes());

        chunk.block = filePos / BLOCK_SIZE;
    }

    /**
     * Compact store file, that is, compact blocks that have a low
     * fill rate, and move chunks next to each other. This will typically
     * shrink the file. Changes are flushed to the file, and old
     * chunks are overwritten.
     *
     * @param thresholdFildRate do not compact if store fill rate above this value (0-100)
     * @param maxCompactTime the maximum time in milliseconds to compact
     * @param maxWriteSize the maximum amount of data to be written as part of this call
     */
    protected void compactStore(int thresholdFildRate, long maxCompactTime, int maxWriteSize, MVStore mvStore) {
        setRetentionTime(0);
        long stopAt = System.nanoTime() + maxCompactTime * 1_000_000L;
        while (compact(thresholdFildRate, maxWriteSize)) {
            sync();
            compactMoveChunks(thresholdFildRate, maxWriteSize, mvStore);
            if (System.nanoTime() - stopAt > 0L) {
                break;
            }
        }
    }

    /**
     * Compact the store by moving all chunks next to each other, if there is
     * free space between chunks. This might temporarily increase the file size.
     * Chunks are overwritten irrespective of the current retention time. Before
     * overwriting chunks and before resizing the file, syncFile() is called.
     *
     * @param targetFillRate do nothing if the file store fill rate is higher
     *            than this
     * @param moveSize the number of bytes to move
     * @param mvStore owner of this store
     */
    public void compactMoveChunks(int targetFillRate, long moveSize, MVStore mvStore) {
        if (isSpaceReused()) {
            mvStore.executeFilestoreOperation(() -> {
                dropUnusedChunks();
                saveChunkLock.lock();
                try {
                    if (hasPersitentData() && getFillRate() <= targetFillRate) {
                        compactMoveChunks(moveSize);
                    }
                } finally {
                    saveChunkLock.unlock();
                }
            });
        }
    }

    private void compactMoveChunks(long moveSize) {
        long start = getFirstFree() / FileStore.BLOCK_SIZE;
        Iterable<SFChunk> chunksToMove = findChunksToMove(start, moveSize);
        if (chunksToMove != null) {
            compactMoveChunks(chunksToMove);
        }
    }

    private Iterable<SFChunk> findChunksToMove(long startBlock, long moveSize) {
        long maxBlocksToMove = moveSize / FileStore.BLOCK_SIZE;
        Iterable<SFChunk> result = null;
        if (maxBlocksToMove > 0) {
            PriorityQueue<SFChunk> queue = new PriorityQueue<>(getChunks().size() / 2 + 1,
                    (o1, o2) -> {
                        // instead of selection just closest to beginning of the file,
                        // pick smaller chunk(s) which sit in between bigger holes
                        int res = Integer.compare(o2.collectPriority, o1.collectPriority);
                        if (res != 0) {
                            return res;
                        }
                        return Long.signum(o2.block - o1.block);
                    });
            long size = 0;
            for (SFChunk chunk : getChunks().values()) {
                if (chunk.isAllocated() && chunk.block > startBlock) {
                    chunk.collectPriority = getMovePriority(chunk);
                    queue.offer(chunk);
                    size += chunk.len;
                    while (size > maxBlocksToMove) {
                        Chunk<?> removed = queue.poll();
                        if (removed == null) {
                            break;
                        }
                        size -= removed.len;
                    }
                }
            }
            if (!queue.isEmpty()) {
                ArrayList<SFChunk> list = new ArrayList<>(queue);
                list.sort(Chunk.PositionComparator.instance());
                result = list;
            }
        }
        return result;
    }

    private int getMovePriority(SFChunk chunk) {
        return getMovePriority((int)chunk.block);
    }

    private void compactMoveChunks(Iterable<SFChunk> move) {
        assert saveChunkLock.isHeldByCurrentThread();
        if (move != null) {
            // this will ensure better recognition of the last chunk
            // in case of power failure, since we are going to move older chunks
            // to the end of the file
            writeStoreHeader();
            sync();

            Iterator<SFChunk> iterator = move.iterator();
            assert iterator.hasNext();
            long leftmostBlock = iterator.next().block;
            long originalBlockCount = getAfterLastBlock();
            // we need to ensure that chunks moved within the following loop
            // do not overlap with space just released by chunks moved before them,
            // hence the need to reserve this area [leftmostBlock, originalBlockCount)
            for (SFChunk chunk : move) {
                moveChunk(chunk, leftmostBlock, originalBlockCount);
            }
            // update the metadata (hopefully within the file)
            store(leftmostBlock, originalBlockCount);
            sync();

            SFChunk chunkToMove = lastChunk;
            assert chunkToMove != null;
            long postEvacuationBlockCount = getAfterLastBlock();

            boolean chunkToMoveIsAlreadyInside = chunkToMove.block < leftmostBlock;
            boolean movedToEOF = !chunkToMoveIsAlreadyInside;
            // move all chunks, which previously did not fit before reserved area
            // now we can re-use previously reserved area [leftmostBlock, originalBlockCount),
            // but need to reserve [originalBlockCount, postEvacuationBlockCount)
            for (SFChunk c : move) {
                if (c.block >= originalBlockCount &&
                        moveChunk(c, originalBlockCount, postEvacuationBlockCount)) {
                    assert c.block < originalBlockCount;
                    movedToEOF = true;
                }
            }
            assert postEvacuationBlockCount >= getAfterLastBlock();

            if (movedToEOF) {
                boolean moved = moveChunkInside(chunkToMove, originalBlockCount);

                // store a new chunk with updated metadata (hopefully within a file)
                store(originalBlockCount, postEvacuationBlockCount);
                sync();
                // if chunkToMove did not fit within originalBlockCount (move is
                // false), and since now previously reserved area
                // [originalBlockCount, postEvacuationBlockCount) also can be
                // used, lets try to move that chunk into this area, closer to
                // the beginning of the file
                long lastBoundary = moved || chunkToMoveIsAlreadyInside ?
                                        postEvacuationBlockCount : chunkToMove.block;
                moved = !moved && moveChunkInside(chunkToMove, lastBoundary);
                if (moveChunkInside(lastChunk, lastBoundary) || moved) {
                    store(lastBoundary, -1);
                }
            }

            shrinkStoreIfPossible(0);
            sync();
        }
    }

    private void store(long reservedLow, long reservedHigh) {
        this.reservedLow = reservedLow;
        this.reservedHigh = reservedHigh;
        saveChunkLock.unlock();
        try {
            store();
        } finally {
            saveChunkLock.lock();
            this.reservedLow = 0;
            this.reservedHigh = 0;
        }
    }

    private boolean moveChunkInside(SFChunk chunkToMove, long boundary) {
        boolean res = chunkToMove.block >= boundary &&
                predictAllocation(chunkToMove.len, boundary, -1) < boundary &&
                moveChunk(chunkToMove, boundary, -1);
        assert !res || chunkToMove.block + chunkToMove.len <= boundary;
        return res;
    }

    /**
     * Move specified chunk into free area of the file. "Reserved" area
     * specifies file interval to be avoided, when un-allocated space will be
     * chosen for a new chunk's location.
     *
     * @param chunk            to move
     * @param reservedAreaLow  low boundary of reserved area, inclusive
     * @param reservedAreaHigh high boundary of reserved area, exclusive
     * @return true if block was moved, false otherwise
     */
    private boolean moveChunk(SFChunk chunk, long reservedAreaLow, long reservedAreaHigh) {
        // ignore if already removed during the previous store operations
        // those are possible either as explicit commit calls
        // or from meta map updates at the end of this method
        if (!getChunks().containsKey(chunk.id)) {
            return false;
        }
        long start = chunk.block * FileStore.BLOCK_SIZE;
        int length = chunk.len * FileStore.BLOCK_SIZE;
        long pos = allocate(length, reservedAreaLow, reservedAreaHigh);
        long block = pos / FileStore.BLOCK_SIZE;
        // in the absence of a reserved area,
        // block should always move closer to the beginning of the file
        assert reservedAreaHigh > 0 || block <= chunk.block : block + " " + chunk;
        ByteBuffer readBuff = readFully(chunk, start, length);
        writeFully(null, pos, readBuff);
        free(start, length);
        // can not set chunk's new block/len until it's fully written at new location,
        // because concurrent reader can pick it up prematurely,
        chunk.block = block;
        chunk.next = 0;
        saveChunkMetadataChanges(chunk);
        return true;
    }

    /**
     * Shrink the store if possible, and if at least a given percentage can be
     * saved.
     *
     * @param minPercent the minimum percentage to save
     */
    protected void shrinkStoreIfPossible(int minPercent) {
        assert saveChunkLock.isHeldByCurrentThread();
        long result = getFileLengthInUse();
        assert result == measureFileLengthInUse() : result + " != " + measureFileLengthInUse();
        shrinkIfPossible(minPercent);
    }

    private void shrinkIfPossible(int minPercent) {
        if (isReadOnly()) {
            return;
        }
        long end = getFileLengthInUse();
        long fileSize = size();
        if (end >= fileSize) {
            return;
        }
        if (minPercent > 0 && fileSize - end < BLOCK_SIZE) {
            return;
        }
        int savedPercent = (int) (100 - (end * 100 / fileSize));
        if (savedPercent < minPercent) {
            return;
        }
        sync();
        truncate(end);
    }

    @Override
    protected void doHousekeeping(MVStore mvStore) throws InterruptedException {
        int autoCommitMemory = mvStore.getAutoCommitMemory();
        int fillRate = getFillRate();
        if (isFragmented() && fillRate < getAutoCompactFillRate()) {

            mvStore.tryExecuteUnderStoreLock(() -> {
                int moveSize = autoCommitMemory;
                if (isIdle()) {
                    moveSize *= 4;
                }
                compactMoveChunks(101, moveSize, mvStore);
                return true;
            });
        } else if (fillRate >= getAutoCompactFillRate() && hasPersitentData()) {
            int chunksFillRate = getRewritableChunksFillRate();
            int _chunksFillRate = isIdle() ? 100 - (100 - chunksFillRate) / 2 : chunksFillRate;
            if (_chunksFillRate < getTargetFillRate()) {
                mvStore.tryExecuteUnderStoreLock(() -> {
                    int writeLimit = autoCommitMemory * fillRate / Math.max(_chunksFillRate, 1);
                    if (!isIdle()) {
                        writeLimit /= 4;
                    }
                    if (rewriteChunks(writeLimit, _chunksFillRate)) {
                        dropUnusedChunks();
                    }
                    return true;
                });
            }
        }
    }

    private int getTargetFillRate() {
        int targetRate = getAutoCompactFillRate();
        // use a lower fill rate if there were any file operations since the last time
        if (!isIdle()) {
            targetRate /= 2;
        }
        return targetRate;
    }

    protected abstract void truncate(long size);

    /**
     * Mark the file as empty.
     */
    @Override
    public void clear() {
        freeSpace.clear();
    }

    /**
     * Calculates relative "priority" for chunk to be moved.
     *
     * @param block where chunk starts
     * @return priority, bigger number indicate that chunk need to be moved sooner
     */
    public int getMovePriority(int block) {
        return freeSpace.getMovePriority(block);
    }

    /**
     * Get the index of the first block after last occupied one.
     * It marks the beginning of the last (infinite) free space.
     *
     * @return block index
     */
    private long getAfterLastBlock() {
        assert saveChunkLock.isHeldByCurrentThread();
        return getAfterLastBlock_();
    }

    protected long getAfterLastBlock_() {
        return freeSpace.getAfterLastBlock();
    }

    public Collection<SFChunk> getRewriteCandidates() {
        return isSpaceReused() ? null : Collections.emptyList();
    }
}
