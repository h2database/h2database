/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;
import java.util.Arrays;

/**
 * A list of bits.
 */
public class BitField {

    private static final int ADDRESS_BITS = 6;
    private static final int BITS = 64;
    private static final int ADDRESS_MASK = BITS - 1;
    private long[] data = new long[10];

    /**
     * Get the index of the last bit that is set.
     *
     * @return the index of the last enabled bit, or -1
     */
    public int getLastSetBit() {
        int i = (data.length << ADDRESS_BITS) - 1;
        while (i >= 0) {
            if (get(i)) {
                return i;
            }
            i--;
        }
        return -1;
    }

    /**
     * Get the index of the next bit that is set.
     *
     * @param fromIndex where to start searching
     * @return the index of the next enabled bit
     */
    public int nextSetBit(int fromIndex) {
        int i = fromIndex >> ADDRESS_BITS;
        int max = data.length;
        int maxAddress = data.length << ADDRESS_BITS;
        for (; i < max; i++) {
            if (data[i] == 0) {
                continue;
            }
            int j = Math.max(fromIndex, i << ADDRESS_BITS);
            for (int end = Math.min(maxAddress, j + 64); j < end; j++) {
                if (get(j)) {
                    return j;
                }
            }
        }
        return -1;
    }

    /**
     * Get the index of the next bit that is not set.
     *
     * @param fromIndex where to start searching
     * @return the index of the next disabled bit
     */
    public int nextClearBit(int fromIndex) {
        int i = fromIndex >> ADDRESS_BITS;
        int max = data.length;
        for (; i < max; i++) {
            if (data[i] == -1) {
                continue;
            }
            int j = Math.max(fromIndex, i << ADDRESS_BITS);
            for (int end = j + 64; j < end; j++) {
                if (!get(j)) {
                    return j;
                }
            }
        }
        return max << ADDRESS_BITS;
    }

    /**
     * Get the bit mask of the bits at the given index.
     *
     * @param i the index (must be a multiple of 64)
     * @return the bit mask as a long
     */
    public long getLong(int i) {
        int addr = getAddress(i);
        if (addr >= data.length) {
            return 0;
        }
        return data[addr];
    }

    /**
     * Get the bit at the given index.
     *
     * @param i the index
     * @return true if the bit is enabled
     */
    public boolean get(int i) {
        int addr = getAddress(i);
        if (addr >= data.length) {
            return false;
        }
        return (data[addr] & getBitMask(i)) != 0;
    }

    /**
     * Get the next 8 bits at the given index.
     * The index must be a multiple of 8.
     *
     * @param i the index
     * @return the next 8 bits
     */
    public int getByte(int i) {
        int addr = getAddress(i);
        if (addr >= data.length) {
            return 0;
        }
        return (int) (data[addr] >>> (i & (7 << 3)) & 255);
    }

    /**
     * Combine the next 8 bits at the given index with OR.
     * The index must be a multiple of 8.
     *
     * @param i the index
     * @param x the next 8 bits (0 - 255)
     */
    public void setByte(int i, int x) {
        int addr = getAddress(i);
        checkCapacity(addr);
        data[addr] |= ((long) x) << (i & (7 << 3));
    }

    /**
     * Set bit at the given index to 'true'.
     *
     * @param i the index
     */
    public void set(int i) {
        int addr = getAddress(i);
        checkCapacity(addr);
        data[addr] |= getBitMask(i);
    }

    /**
     * Set bit at the given index to 'false'.
     *
     * @param i the index
     */
    public void clear(int i) {
        int addr = getAddress(i);
        if (addr >= data.length) {
            return;
        }
        data[addr] &= ~getBitMask(i);
    }

    // this is static to encourage compiler to inline it
    private static int getAddress(int i) {
        return i >> ADDRESS_BITS;
    }

    // this is static to encourage compiler to inline it
    private static long getBitMask(int i) {
        return 1L << (i & ADDRESS_MASK);
    }

    private void checkCapacity(int size) {
        if (size >= data.length) {
            expandCapacity(size);
        }
    }

    private void expandCapacity(int size) {
        while (size >= data.length) {
            int newSize = data.length == 0 ? 1 : data.length * 2;
            long[] d = new long[newSize];
            System.arraycopy(data, 0, d, 0, data.length);
            data = d;
        }
    }

    /**
     * Enable or disable a number of bits.
     *
     * @author Samuel Van Oort
     * @param start the index of the first bit to enable or disable
     * @param len the number of bits to enable or disable
     * @param value the new value
     */
    public void setRange(int start, int len, boolean value) {
        int startIdx = getAddress(start);
        int endIdx = getAddress(start + len - 1);
        int end = start+len;

        // expand BitField if writing past end, unless clearing
        // this prevents OutOfMemoryError mid-modify
        int datalen = data.length;
        if (endIdx >= datalen) {
            if (!value && startIdx >= datalen) {
                // trying to clear past end of set bits - nothing to do
                return;
            }
            expandCapacity(endIdx);
        }

        long startMask = (~0L) << start;
        long endMask = (~0L) >>> -end;
        // if operating on one long, mask is combined
        if (startIdx == endIdx) {
            startMask &= endMask;
        }
        // set first long element in range
        if (value) {
            data[startIdx] |= startMask;
        } else {
            data[startIdx] &= ~startMask;
        }

        // work on additional elements only if needed
        if (startIdx != endIdx) {
            // set the last long element in range
            if (value) {
                data[endIdx] |= endMask;
            } else {
                data[endIdx] &= ~endMask;
            }
            // set longs fully in the range very quickly to all 1 or all 0
            Arrays.fill(data, startIdx + 1, endIdx, value ? -1L : 0L);
        }
    }
}
