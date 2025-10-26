/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.tx;

import java.util.Arrays;

/**
 * Class BitSetHelper provides very limited functionality of BitSet used by TransactionStore.
 *
 * @author <a href="mailto:andrei.tokar@gmail.com">Andrei Tokar</a>
 */
public class BitSetHelper
{
    private static final int ADDRESS_BITS_PER_WORD = 6;
    private static final int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
    private static final long WORD_MASK = 0xffffffffffffffffL;

    private BitSetHelper() {
    }

    public static boolean get(long[] bits, int bitIndex) {
        int wordIndex = wordIndex(bitIndex);
        return wordIndex < bits.length && (bits[wordIndex] & (1L << bitIndex)) != 0;
    }

    public static long[] flip(long[] bits, int bitIndex) {
        int wordIndex = wordIndex(bitIndex);

        int length = bits.length;
        while (--length > wordIndex && bits[length] == 0L) {/**/}
        bits = Arrays.copyOf(bits, Math.max(length, wordIndex) + 1);
        bits[wordIndex] ^= 1L << bitIndex;
        return bits;
    }

    public static int nextSetBit(long[] bits, int bitIndex) {
        int wordIndex = wordIndex(bitIndex);
        if (wordIndex >= bits.length) {
            return -1;
        }

        long word = bits[wordIndex] & (WORD_MASK << bitIndex);

        while (true) {
            if (word != 0) {
                return (wordIndex * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
            }
            if (++wordIndex == bits.length) {
                return -1;
            }
            word = bits[wordIndex];
        }
    }

    public static int nextClearBit(long[] bits, int bitIndex) {
        int wordIndex = wordIndex(bitIndex);
        if (wordIndex >= bits.length) {
            return bitIndex;
        }

        long word = ~bits[wordIndex] & (WORD_MASK << bitIndex);

        while (true) {
            if (word != 0) {
                return (wordIndex * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
            }
            if (++wordIndex == bits.length) {
                return bits.length * BITS_PER_WORD;
            }
            word = ~bits[wordIndex];
        }
    }

    public static int length(long[] bits) {
        int wordsInUse = bits.length;
        if (wordsInUse == 0)
            return 0;

        return BITS_PER_WORD * (wordsInUse - 1) +
            (BITS_PER_WORD - Long.numberOfLeadingZeros(bits[wordsInUse - 1]));
    }

    private static int wordIndex(int index) {
        return index >> ADDRESS_BITS_PER_WORD;
    }
}
