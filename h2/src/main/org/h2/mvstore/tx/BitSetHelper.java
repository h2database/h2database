/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import java.util.Arrays;

/**
 * Class BitSetHelper.
 * <UL>
 * <LI> 10/17/25 12:25 PM initial creation
 * </UL>
 *
 * @author <a href="mailto:andrei.tokar@gmail.com">Andrei Tokar</a>
 */
public class BitSetHelper
{
    private static final int ADDRESS_BITS_PER_WORD = 6;

    private BitSetHelper() {
    }

    public static boolean get(long[] bits, int transactionId) {
        int wordIndex = wordIndex(transactionId);
        return wordIndex < bits.length && (bits[wordIndex] & (1L << transactionId)) != 0;
    }

    public static long[] flip(long[] bits, int transactionId) {
        int wordIndex = wordIndex(transactionId);
        bits = Arrays.copyOf(bits, Math.max(bits.length, wordIndex + 1));
        bits[wordIndex] ^= 1L << transactionId;
        return bits;
    }

    public static long[] set(long[] bits, int transactionId) {
        int wordIndex = wordIndex(transactionId);
        bits = Arrays.copyOf(bits, Math.max(bits.length, wordIndex + 1));
        bits[wordIndex] |= 1L << transactionId;
        return bits;
    }

    public static long[] clear(long[] bits, int transactionId) {
        int wordIndex = wordIndex(transactionId);
        bits = Arrays.copyOf(bits, Math.max(bits.length, wordIndex + 1));
        bits[wordIndex] &= ~(1L << transactionId);
        return bits;
    }

    private static int wordIndex(int index) {
        return index >> ADDRESS_BITS_PER_WORD;
    }
}
