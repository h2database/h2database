package org.h2.util;

/**
 * Backports of Java 9's Byte functions, as far as they are needed for Java 8 code.
 */
public class ByteUtils {

    /**
     * Compares two {@code byte} values numerically treating the values
     * as unsigned.
     *
     * @param  x the first {@code byte} to compare
     * @param  y the second {@code byte} to compare
     * @return the value {@code 0} if {@code x == y}; a value less
     *         than {@code 0} if {@code x < y} as unsigned values; and
     *         a value greater than {@code 0} if {@code x > y} as
     *         unsigned values
     * @since 9
     */
    public static int compareUnsigned(byte x, byte y) {
        return Byte.toUnsignedInt(x) - Byte.toUnsignedInt(y);
    }

}
