package org.h2.util;

import java.math.BigInteger;

/**
 * A view of a {@code byte[]}, with a getter and setter as if it were a {@code int[]}.
 */
public class ByteArrayAsIntArray {

    private final boolean be;

    public ByteArrayAsIntArray(boolean be) {
        this.be = be;
    }

    public int get(byte[] array, int offset) {
        int result;
        if (be) {
            result = ((array[offset + 0] & 0xff) << 24) |
                     ((array[offset + 1] & 0xff) << 16) |
                     ((array[offset + 2] & 0xff) << 8 ) |
                     ((array[offset + 3] & 0xff) << 0 );
        } else {
            result = ((array[offset + 3] & 0xff) << 24) |
                     ((array[offset + 2] & 0xff) << 16) |
                     ((array[offset + 1] & 0xff) << 8 ) |
                     ((array[offset + 0] & 0xff) << 0 );
        }
        return result;
    }

    public void set(byte[] array, int offset, int value) {
        if (be) {
            array[offset] = (byte) (value >> 24);
            array[offset + 1] = (byte) (value >> 16 & 0xff);
            array[offset + 2] = (byte) (value >> 8 & 0xff);
            array[offset + 3] = (byte) (value & 0xff);
        } else {
            array[offset] = (byte) (value & 0xff);
            array[offset + 1] = (byte) (value >> 8 & 0xff);
            array[offset + 2] = (byte) (value >> 16 & 0xff);
            array[offset + 3] = (byte) (value >> 24);
        }
    }
}
