/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.util.BitSet;

import org.h2.util.MathUtils;

/**
 * A free space bit set.
 */
public class FreeSpaceBitSet {

    private static final boolean DETAILED_INFO = false;

    /**
     * The first usable block.
     */
    private final int firstFreeBlock;

    /**
     * The block size in bytes.
     */
    private final int blockSize;

    /**
     * The bit set.
     */
    private final BitSet set = new BitSet();

    /**
     * Left-shifting register, which holds outcomes of recent allocations.
     * Only allocations done in "reuseSpace" mode are recorded here.
     * For example, rightmost bit set to 1 means that last allocation failed to find a hole big enough,
     * and next bit set to 0 means that previous allocation request have found one.
     */
    private int failureFlags;


    /**
     * Create a new free space map.
     *
     * @param firstFreeBlock the first free block
     * @param blockSize the block size
     */
    public FreeSpaceBitSet(int firstFreeBlock, int blockSize) {
        this.firstFreeBlock = firstFreeBlock;
        this.blockSize = blockSize;
        clear();
    }

    /**
     * Reset the list.
     */
    public void clear() {
        set.clear();
        set.set(0, firstFreeBlock);
    }

    /**
     * Check whether one of the blocks is in use.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     * @return true if a block is in use
     */
    public boolean isUsed(long pos, int length) {
        int start = getBlock(pos);
        int blocks = getBlockCount(length);
        for (int i = start; i < start + blocks; i++) {
            if (!set.get(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Check whether one of the blocks is free.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     * @return true if a block is free
     */
    public boolean isFree(long pos, int length) {
        int start = getBlock(pos);
        int blocks = getBlockCount(length);
        for (int i = start; i < start + blocks; i++) {
            if (set.get(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Allocate a number of blocks and mark them as used.
     *
     * @param length the number of bytes to allocate
     * @return the start position in bytes
     */
    public long allocate(int length) {
        return allocate(length, true);
    }

    /**
     * Calculate starting position of the prospective allocation.
     *
     * @param length the number of bytes to allocate
     * @return the start position in bytes
     */
    long predictAllocation(int length) {
        return allocate(length, false);
    }

    boolean isFragmented() {
        return Integer.bitCount(failureFlags & 0x0F) > 1;
    }

    private long allocate(int length, boolean allocate) {
        int blocks = getBlockCount(length);
        for (int i = 0;;) {
            int start = set.nextClearBit(i);
            int end = set.nextSetBit(start + 1);
            if (end < 0 || end - start >= blocks) {
                assert set.nextSetBit(start) == -1 || set.nextSetBit(start) >= start + blocks :
                        "Double alloc: " + Integer.toHexString(start) + "/" + Integer.toHexString(blocks) + " " + this;
                if (allocate) {
                    set.set(start, start + blocks);
                } else {
                    failureFlags <<= 1;
                    if (end < 0) {
                        failureFlags |= 1;
                    }
                }
                return getPos(start);
            }
            i = end;
        }
    }

    /**
     * Mark the space as in use.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    public void markUsed(long pos, int length) {
        int start = getBlock(pos);
        int blocks = getBlockCount(length);
        assert set.nextSetBit(start) == -1 || set.nextSetBit(start) >= start + blocks :
                "Double mark: " + Integer.toHexString(start) + "/" + Integer.toHexString(blocks) + " " + this;
        set.set(start, start + blocks);
    }

    /**
     * Mark the space as free.
     *
     * @param pos the position in bytes
     * @param length the number of bytes
     */
    public void free(long pos, int length) {
        int start = getBlock(pos);
        int blocks = getBlockCount(length);
        assert set.nextClearBit(start) >= start + blocks :
                "Double free: " + Integer.toHexString(start) + "/" + Integer.toHexString(blocks) + " " + this;
        set.clear(start, start + blocks);
    }

    private long getPos(int block) {
        return (long) block * (long) blockSize;
    }

    private int getBlock(long pos) {
        return (int) (pos / blockSize);
    }

    private int getBlockCount(int length) {
        return MathUtils.roundUpInt(length, blockSize) / blockSize;
    }

    /**
     * Get the fill rate of the space in percent. The value 0 means the space is
     * completely free, and 100 means it is completely full.
     *
     * @return the fill rate (0 - 100)
     */
    int getFillRate() {
        return getProjectedFillRate(0L, 0);
    }

    /**
     * Calculates a prospective fill rate, which store would have after rewrite of sparsely populated chunk(s)
     * and evacuation of still live data into a new chunk.
     * @param live amount of memory (bytes) from vacated block, which would be written into a new chunk
     * @param vacatedBlocks number of blocks vacated
     * @return prospective fill rate (0 - 100)
     */
    int getProjectedFillRate(long live, int vacatedBlocks) {
        int additionalBlocks = getBlock(live + blockSize - 1);
        // it's not bullet-proof against race condition but should be good enough
        // to get approximation without holding a store lock
        int usedBlocks;
        int totalBlocks;
        do {
            totalBlocks = set.length();
            usedBlocks = set.cardinality();
        } while (totalBlocks != set.length() || usedBlocks > totalBlocks);
        int totalBlocksAdjustment = additionalBlocks - firstFreeBlock;
        usedBlocks += totalBlocksAdjustment - vacatedBlocks;
        totalBlocks += totalBlocksAdjustment;
        return usedBlocks == 0 ? 0 : (int)((100L * usedBlocks + totalBlocks - 1) / totalBlocks);
    }

    /**
     * Get the position of the first free space.
     *
     * @return the position.
     */
    long getFirstFree() {
        return getPos(set.nextClearBit(0));
    }

    /**
     * Get the position of the last (infinite) free space.
     *
     * @return the position.
     */
    long getLastFree() {
        return getPos(set.previousSetBit(set.size()-1) + 1);
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        if (DETAILED_INFO) {
            int onCount = 0, offCount = 0;
            int on = 0;
            for (int i = 0; i < set.length(); i++) {
                if (set.get(i)) {
                    onCount++;
                    on++;
                } else {
                    offCount++;
                }
                if ((i & 1023) == 1023) {
                    buff.append(String.format("%3x", on)).append(' ');
                    on = 0;
                }
            }
            buff.append('\n')
                    .append(" on ").append(onCount).append(" off ").append(offCount)
                    .append(' ').append(100 * onCount / (onCount+offCount)).append("% used ");
        }
        buff.append('[');
        for (int i = 0;;) {
            if (i > 0) {
                buff.append(", ");
            }
            int start = set.nextClearBit(i);
            buff.append(Integer.toHexString(start)).append('-');
            int end = set.nextSetBit(start + 1);
            if (end < 0) {
                break;
            }
            buff.append(Integer.toHexString(end - 1));
            i = end + 1;
        }
        buff.append(']');
        return buff.toString();
    }
}