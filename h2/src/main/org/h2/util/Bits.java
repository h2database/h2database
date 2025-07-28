/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.UUID;

/**
 * Manipulations with bytes and arrays. Specialized implementation for Java 9
 * and later versions.
 */
public final class Bits {

    /**
     * Access to elements of a byte[] array viewed as if it
     * were a int[] array on big-endian system.
     */
    public static final ByteArrayAsIntArray INT_VH_BE = new ByteArrayAsIntArray(true);

    /**
     * Access to elements of a byte[] array viewed as if it
     * were a int[] array on little-endian system.
     */
    public static final ByteArrayAsIntArray INT_VH_LE = new ByteArrayAsIntArray(false);

    /**
     * Access to elements of a byte[] array viewed as if it
     * were a long[] array on big-endian system.
     */
    public static final ByteArrayAsLongArray LONG_VH_BE = new ByteArrayAsLongArray(true);

    /**
     * Access to elements of a byte[] array viewed as if it
     * were a long[] array on little-endian system.
     */
    public static final ByteArrayAsLongArray LONG_VH_LE = new ByteArrayAsLongArray(false);

    /**
     * Access to elements of a byte[] array viewed as if it
     * were a double[] array on big-endian system.
     */
    public static final ByteArrayAsDoubleArray DOUBLE_VH_BE = new ByteArrayAsDoubleArray(true);

    /**
     * Access to elements of a byte[] array viewed as if it
     * were a double[] array on little-endian system.
     */
    public static final ByteArrayAsDoubleArray DOUBLE_VH_LE = new ByteArrayAsDoubleArray(false);

    /**
     * Converts UUID value to byte array in big-endian order.
     *
     * @param msb
     *            most significant part of UUID
     * @param lsb
     *            least significant part of UUID
     * @return byte array representation
     */
    public static byte[] uuidToBytes(long msb, long lsb) {
        byte[] buff = new byte[16];
        LONG_VH_BE.set(buff, 0, msb);
        LONG_VH_BE.set(buff, 8, lsb);
        return buff;
    }

    /**
     * Converts UUID value to byte array in big-endian order.
     *
     * @param uuid
     *            UUID value
     * @return byte array representation
     */
    public static byte[] uuidToBytes(UUID uuid) {
        return uuidToBytes(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    private Bits() {
    }

}
