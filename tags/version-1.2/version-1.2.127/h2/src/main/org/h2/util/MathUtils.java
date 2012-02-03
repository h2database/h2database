/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.math.BigDecimal;
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.message.Message;

/**
 * This is a utility class with mathematical helper functions.
 */
public class MathUtils {

    private MathUtils() {
        // utility class
    }

    /**
     * Round the value up to the next block size. The block size must be a power
     * of two. As an example, using the block size of 8, the following rounding
     * operations are done: 0 stays 0; values 1..8 results in 8, 9..16 results
     * in 16, and so on.
     *
     * @param x the value to be rounded
     * @param blockSizePowerOf2 the block size
     * @return the rounded value
     */
    public static int roundUp(int x, int blockSizePowerOf2) {
        return (x + blockSizePowerOf2 - 1) & (-blockSizePowerOf2);
    }

    /**
     * Round the value up to the next block size. The block size must be a power
     * of two. As an example, using the block size of 8, the following rounding
     * operations are done: 0 stays 0; values 1..8 results in 8, 9..16 results
     * in 16, and so on.
     *
     * @param x the value to be rounded
     * @param blockSizePowerOf2 the block size
     * @return the rounded value
     */
    public static long roundUpLong(long x, long blockSizePowerOf2) {
        return (x + blockSizePowerOf2 - 1) & (-blockSizePowerOf2);
    }

    /**
     * Check if a value is a power of two.
     *
     * @param len the value to check
     * @throws RuntimeException if it is not a power of two
     */
    public static void checkPowerOf2(int len) {
        if ((len & (len - 1)) != 0 && len > 0) {
            Message.throwInternalError("not a power of 2: " + len);
        }
    }

    /**
     * Get the value that is equal or higher than this value, and that is a
     * power of two.
     *
     * @param x the original value
     * @return the next power of two value
     */
    public static int nextPowerOf2(int x) {
        long i = 1;
        while (i < x && i < (Integer.MAX_VALUE / 2)) {
            i += i;
        }
        return (int) i;
    }

    /**
     * Increase the value by about 50%. The method is used to increase the file
     * size in larger steps.
     *
     * @param start the smallest possible returned value
     * @param min the current value
     * @param blockSize the block size
     * @param maxIncrease the maximum increment
     * @return the new value
     */
    public static long scaleUp50Percent(long start, long min, long blockSize, long maxIncrease) {
        long len;
        if (min > maxIncrease * 2) {
            len = MathUtils.roundUpLong(min, maxIncrease);
        } else {
            len = start;
            while (len < min) {
                len += len / 2;
                len += len % blockSize;
            }
        }
        return len;
    }

    /**
     * Set the scale of a BigDecimal value.
     *
     * @param bd the BigDecimal value
     * @param scale the new scale
     * @return the scaled value
     */
    public static BigDecimal setScale(BigDecimal bd, int scale) throws SQLException {
        if (scale > Constants.BIG_DECIMAL_SCALE_MAX) {
            throw Message.getInvalidValueException("" + scale, "scale");
        } else if (scale < -Constants.BIG_DECIMAL_SCALE_MAX) {
            throw Message.getInvalidValueException("" + scale, "scale");
        }
        return bd.setScale(scale, BigDecimal.ROUND_HALF_UP);
    }

    /**
     * Parse a string to a byte. This method uses the decode method to support
     * decimal, hexadecimal and octal values.
     *
     * @param s the string to parse
     * @return the value
     */
    public static byte decodeByte(String s) {
        return Byte.decode(s).byteValue();
    }

    /**
     * Parse a string to a short. This method uses the decode method to support
     * decimal, hexadecimal and octal values.
     *
     * @param s the string to parse
     * @return the value
     */
    public static short decodeShort(String s) {
        return Short.decode(s).shortValue();
    }

    /**
     * Parse a string to an int. This method uses the decode method to support
     * decimal, hexadecimal and octal values.
     *
     * @param s the string to parse
     * @return the value
     */
    public static int decodeInt(String s) {
        return Integer.decode(s).intValue();
    }

    /**
     * Parse a string to a long. This method uses the decode method to support
     * decimal, hexadecimal and octal values.
     *
     * @param s the string to parse
     * @return the value
     */
    public static long decodeLong(String s) {
        return Long.decode(s).longValue();
    }

    /**
     * Convert a long value to an int value. Values larger than the biggest int
     * value is converted to the biggest int value, and values smaller than the
     * smallest int value are converted to the smallest int value.
     *
     * @param l the value to convert
     * @return the converted int value
     */
    public static int convertLongToInt(long l) {
        if (l <= Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        } else if (l >= Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int) l;
        }
    }

    /**
     * Reverse the bits in a 32 bit integer. This code is also available in Java
     * 5 using Integer.reverse, however not available yet in Retrotranslator.
     * The code was taken from http://www.hackersdelight.org - reverse.c
     *
     * @param x the original value
     * @return the value with reversed bits
     */
    public static int reverse(int x) {
        x = (x & 0x55555555) << 1 | (x >>> 1) & 0x55555555;
        x = (x & 0x33333333) << 2 | (x >>> 2) & 0x33333333;
        x = (x & 0x0f0f0f0f) << 4 | (x >>> 4) & 0x0f0f0f0f;
        x = (x << 24) | ((x & 0xff00) << 8) | ((x >>> 8) & 0xff00) | (x >>> 24);
        return x;
    }

    /**
     * Reverse the bits in a 64 bit long. This code is also available in Java 5
     * using Long.reverse, however not available yet in Retrotranslator.
     *
     * @param x the original value
     * @return the value with reversed bits
     */
    public static long reverse(long x) {
        return (reverse((int) (x >>> 32L)) & 0xffffffffL) ^ (((long) reverse((int) x)) << 32L);
    }

    /**
     * Compare two values. Returns -1 if the first value is smaller, 1 if bigger,
     * and 0 if equal.
     *
     * @param a the first value
     * @param b the second value
     * @return the result
     */
    public static int compare(int a, int b) {
        return a == b ? 0 : a < b ? -1 : 1;
    }

    /**
     * Compare two values. Returns -1 if the first value is smaller, 1 if bigger,
     * and 0 if equal.
     *
     * @param a the first value
     * @param b the second value
     * @return the result
     */
    public static int compare(long a, long b) {
        return a == b ? 0 : a < b ? -1 : 1;
    }

}
