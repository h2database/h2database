/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;


/**
 * @author Thomas
 */

public class BitField {
    
    private long[] data = new long[10];
    private static final int ADDRESS_BITS = 6;
    private static final int BITS = 64;
    private static final int ADDRESS_MASK = BITS - 1;
    
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

    public int nextSetBit(int fromIndex) {
        int i = fromIndex >> ADDRESS_BITS;
        int max = data.length;
        int maxAddress = data.length << ADDRESS_BITS;
        for (; i < max; i++) {
            if (data[i] == 0) {
                continue;
            }
            for (int j = Math.max(fromIndex, i << ADDRESS_BITS); j < maxAddress; j++) {
                if (get(j)) {
                    return j;
                }
            }
        }
        return -1;
    }

    public int nextClearBit(int fromIndex) {
        int i = fromIndex >> ADDRESS_BITS;
        int max = data.length;
        for (; i < max; i++) {
            if (data[i] == -1) {
                continue;
            }
            for (int j = Math.max(fromIndex, i << ADDRESS_BITS);; j++) {
                if (!get(j)) {
                    return j;
                }
            }
        }
        return fromIndex;
    }

    public long getLong(int i) {
        int addr = getAddress(i);
        if (addr >= data.length) {
            return 0;
        }
        return data[addr];
    }

    public boolean get(int i) {
        int addr = getAddress(i);
        if (addr >= data.length) {
            return false;
        }
        return (data[addr] & getBitMask(i)) != 0;
    }

    public void set(int i) {
        int addr = getAddress(i);
        checkCapacity(addr);
        data[addr] |= getBitMask(i);
    }

    public void clear(int i) {
        int addr = getAddress(i);
        if (addr >= data.length) {
            return;
        }
        data[addr] &= ~getBitMask(i);
    }

    private int getAddress(int i) {
        return i >> ADDRESS_BITS;
    }

    private long getBitMask(int i) {
        return 1L << (i & ADDRESS_MASK);
    }

    private void checkCapacity(int size) {
        while (size >= data.length) {
            int newSize = data.length == 0 ? 1 : data.length * 2;
            long[] d = new long[newSize];
            System.arraycopy(data, 0, d, 0, data.length);
            data = d;
        }
    }

    /*
    private BitSet data = new BitSet();
    
    public boolean get(int i) {
        return data.get(i);
    }
    
    public void set(int i) {
        data.set(i);
    }
    
    public void clear(int i) {
        data.clear(i);
    }
    */
    
    public void setRange(int start, int len, boolean value) {
        for (int end = start + len; start < end; start++) {
            set(start, value);
        }
    }

    private void set(int i, boolean value) {
        if (value) {
            set(i);
        } else {
            clear(i);
        }
    }

}
