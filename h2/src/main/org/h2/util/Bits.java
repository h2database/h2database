/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.UUID;

/**
 * Manipulations with bytes and arrays. Specialized implementation for Java 9
 * and later versions.
 */
public final class Bits {

    /**
     * VarHandle giving access to elements of a byte[] array viewed as if it
     * were a int[] array on big-endian system.
     */
    public static final VarHandle INT_VH_BE = MethodHandles.byteArrayViewVarHandle(int[].class, //
            ByteOrder.BIG_ENDIAN);

    /**
     * VarHandle giving access to elements of a byte[] array viewed as if it
     * were a int[] array on little-endian system.
     */
    public static final VarHandle INT_VH_LE = MethodHandles.byteArrayViewVarHandle(int[].class,
            ByteOrder.LITTLE_ENDIAN);

    /**
     * VarHandle giving access to elements of a byte[] array viewed as if it
     * were a long[] array on big-endian system.
     */
    public static final VarHandle LONG_VH_BE = MethodHandles.byteArrayViewVarHandle(long[].class, //
            ByteOrder.BIG_ENDIAN);

    /**
     * VarHandle giving access to elements of a byte[] array viewed as if it
     * were a long[] array on little-endian system.
     */
    public static final VarHandle LONG_VH_LE = MethodHandles.byteArrayViewVarHandle(long[].class,
            ByteOrder.LITTLE_ENDIAN);

    /**
     * VarHandle giving access to elements of a byte[] array viewed as if it
     * were a double[] array on big-endian system.
     */
    public static final VarHandle DOUBLE_VH_BE = MethodHandles.byteArrayViewVarHandle(double[].class,
            ByteOrder.BIG_ENDIAN);

    /**
     * VarHandle giving access to elements of a byte[] array viewed as if it
     * were a double[] array on little-endian system.
     */
    public static final VarHandle DOUBLE_VH_LE = MethodHandles.byteArrayViewVarHandle(double[].class,
            ByteOrder.LITTLE_ENDIAN);

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
