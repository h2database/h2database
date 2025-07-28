package org.h2.util;

/**
 * A view of a {@code byte[]}, with a getter and setter as if it were a {@code long[]}.
 */
public class ByteArrayAsLongArray {

    private final boolean be;

    public ByteArrayAsLongArray(boolean be) {
        this.be = be;
    }

    public long get(byte[] array, int offset) {
        long result;
        if (be) {
            result = ((long)(array[offset + 0] & 0xff) << 56) |
                     ((long)(array[offset + 1] & 0xff) << 48) |
                     ((long)(array[offset + 2] & 0xff) << 40) |
                     ((long)(array[offset + 3] & 0xff) << 32) |
                     ((long)(array[offset + 4] & 0xff) << 24) |
                     ((long)(array[offset + 5] & 0xff) << 16) |
                     ((long)(array[offset + 6] & 0xff) << 8 ) |
                     ((long)(array[offset + 7] & 0xff) << 0 );
        } else {
            result = ((long)(array[offset + 7] & 0xff) << 56) |
                     ((long)(array[offset + 6] & 0xff) << 48) |
                     ((long)(array[offset + 5] & 0xff) << 40) |
                     ((long)(array[offset + 4] & 0xff) << 32) |
                     ((long)(array[offset + 3] & 0xff) << 24) |
                     ((long)(array[offset + 2] & 0xff) << 16) |
                     ((long)(array[offset + 1] & 0xff) << 8 ) |
                     ((long)(array[offset + 0] & 0xff) << 0 );
        }
        return result;
    }

    public void set(byte[] array, int offset, long value) {
        if (be) {
            array[offset + 0] = (byte) (value >> 56 & 0xff);
            array[offset + 1] = (byte) (value >> 48 & 0xff);
            array[offset + 2] = (byte) (value >> 40 & 0xff);
            array[offset + 3] = (byte) (value >> 32 & 0xff);
            array[offset + 4] = (byte) (value >> 24 & 0xff);
            array[offset + 5] = (byte) (value >> 16 & 0xff);
            array[offset + 6] = (byte) (value >>  8 & 0xff);
            array[offset + 7] = (byte) (value >>  0 & 0xff);
        } else {
            array[offset + 0] = (byte) (value >>  0 & 0xff);
            array[offset + 1] = (byte) (value >>  8 & 0xff);
            array[offset + 2] = (byte) (value >> 16 & 0xff);
            array[offset + 3] = (byte) (value >> 24 & 0xff);
            array[offset + 4] = (byte) (value >> 32 & 0xff);
            array[offset + 5] = (byte) (value >> 40 & 0xff);
            array[offset + 6] = (byte) (value >> 48 & 0xff);
            array[offset + 7] = (byte) (value >> 56 & 0xff);
        }
    }
}
