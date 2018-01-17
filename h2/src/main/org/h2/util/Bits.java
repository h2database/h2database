/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.UUID;

/**
 * Manipulations with bytes and arrays. This class can be overridden in
 * multi-release JAR with more efficient implementation for a newer versions of
 * Java.
 */
public final class Bits {

    /**
     * Reads a int value from the byte array at the given position in big-endian
     * order.
     *
     * @param buff
     *            the byte array
     * @param pos
     *            the position
     * @return the value
     */
    public static int readInt(byte[] buff, int pos) {
        return (buff[pos++] << 24) + ((buff[pos++] & 0xff) << 16) + ((buff[pos++] & 0xff) << 8) + (buff[pos] & 0xff);
    }

    /**
     * Reads a long value from the byte array at the given position in big-endian
     * order.
     *
     * @param buff
     *            the byte array
     * @param pos
     *            the position
     * @return the value
     */
    public static long readLong(byte[] buff, int pos) {
        return (((long) readInt(buff, pos)) << 32) + (readInt(buff, pos + 4) & 0xffffffffL);
    }

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
        for (int i = 0; i < 8; i++) {
            buff[i] = (byte) ((msb >> (8 * (7 - i))) & 0xff);
            buff[8 + i] = (byte) ((lsb >> (8 * (7 - i))) & 0xff);
        }
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

    /**
     * Writes a int value to the byte array at the given position in big-endian
     * order.
     *
     * @param buff
     *            the byte array
     * @param pos
     *            the position
     * @param x
     *            the value to write
     */
    public static void writeInt(byte[] buff, int pos, int x) {
        buff[pos++] = (byte) (x >> 24);
        buff[pos++] = (byte) (x >> 16);
        buff[pos++] = (byte) (x >> 8);
        buff[pos] = (byte) x;
    }

    /**
     * Writes a long value to the byte array at the given position in big-endian
     * order.
     *
     * @param buff
     *            the byte array
     * @param pos
     *            the position
     * @param x
     *            the value to write
     */
    public static void writeLong(byte[] buff, int pos, long x) {
        writeInt(buff, pos, (int) (x >> 32));
        writeInt(buff, pos + 4, (int) x);
    }

    private Bits() {
    }
}
