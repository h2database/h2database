/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import java.util.BitSet;

/**
 * Class VersionedBitSet extends standard BitSet to add a version field.
 * This will allow bit set and version to be changed atomically.
 */
final class VersionedBitSet {

    public final long[] bits;
    private final long version;

    public VersionedBitSet() {
        bits = new long[0];
        version = 0;
    }

    public VersionedBitSet(VersionedBitSet other, int bitToFlip) {
        bits = BitSetHelper.flip(other.bits, bitToFlip);
        version = other.version + 1;
    }

    public boolean get(int bitIndex) {
        return BitSetHelper.get(bits, bitIndex);
    }

    public int nextSetBit(int bitIndex) {
        return BitSetHelper.nextSetBit(bits, bitIndex);
    }

    public int nextClearBit(int bitIndex) {
        return BitSetHelper.nextClearBit(bits, bitIndex);
    }

    public int length() {
        return BitSetHelper.length(bits);
    }

    public long getVersion() {
        return version;
    }
}
