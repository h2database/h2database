package org.h2.util;

import sun.misc.Unsafe;

import java.nio.ByteOrder;

/**
 * Backports of Java 9's ArraysSupport functions, as far as they are needed for Java 8 code.
 */
public class ArraysSupport {

    private static final boolean BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

    public static final int LOG2_ARRAY_BYTE_INDEX_SCALE = exactLog2(Unsafe.ARRAY_BYTE_INDEX_SCALE);
    public static final int LOG2_ARRAY_CHAR_INDEX_SCALE = exactLog2(Unsafe.ARRAY_CHAR_INDEX_SCALE);
    public static final int LOG2_ARRAY_INT_INDEX_SCALE = exactLog2(Unsafe.ARRAY_INT_INDEX_SCALE);
    public static final int LOG2_ARRAY_LONG_INDEX_SCALE = exactLog2(Unsafe.ARRAY_LONG_INDEX_SCALE);

    private static final int LOG2_BYTE_BIT_SIZE = exactLog2(Byte.SIZE);

    private static int exactLog2(int scale) {
        if ((scale & (scale - 1)) != 0)
            throw new Error("data type scale not a power of two");
        return Integer.numberOfTrailingZeros(scale);
    }

    private ArraysSupport() {}

    /**
     * Find the relative index of the first mismatching pair of elements in two
     * primitive arrays of the same component type.  Pairs of elements will be
     * tested in order relative to given offsets into both arrays.
     *
     * <p>This method does not perform type checks or bounds checks.  It is the
     * responsibility of the caller to perform such checks before calling this
     * method.
     *
     * <p>The given offsets, in bytes, need not be aligned according to the
     * given log<sub>2</sub> size the array elements.  More specifically, an
     * offset modulus the size need not be zero.
     *
     * @param a the first array to be tested for mismatch, or {@code null} for
     * direct memory access
     * @param aOffset the relative offset, in bytes, from the base address of
     * the first array to test from, otherwise if the first array is
     * {@code null}, an absolute address pointing to the first element to test.
     * @param b the second array to be tested for mismatch, or {@code null} for
     * direct memory access
     * @param bOffset the relative offset, in bytes, from the base address of
     * the second array to test from, otherwise if the second array is
     * {@code null}, an absolute address pointing to the first element to test.
     * @param length the number of array elements to test
     * @param log2ArrayIndexScale log<sub>2</sub> of the array index scale, that
     * corresponds to the size, in bytes, of an array element.
     * @return if a mismatch is found a relative index, between 0 (inclusive)
     * and {@code length} (exclusive), of the first mismatching pair of elements
     * in the two arrays.  Otherwise, if a mismatch is not found the bitwise
     * compliment of the number of remaining pairs of elements to be checked in
     * the tail of the two arrays.
     */
    public static int vectorizedMismatch(Object a, long aOffset,
                                         Object b, long bOffset,
                                         int length,
                                         int log2ArrayIndexScale) {
        // assert a.getClass().isArray();
        // assert b.getClass().isArray();
        // assert 0 <= length <= sizeOf(a)
        // assert 0 <= length <= sizeOf(b)
        // assert 0 <= log2ArrayIndexScale <= 3

        int log2ValuesPerWidth = LOG2_ARRAY_LONG_INDEX_SCALE - log2ArrayIndexScale;
        int wi = 0;
        for (; wi < length >> log2ValuesPerWidth; wi++) {
            long bi = ((long) wi) << LOG2_ARRAY_LONG_INDEX_SCALE;
            long av = UnsafeUtils.getLongUnaligned(a, aOffset + bi);
            long bv = UnsafeUtils.getLongUnaligned(b, bOffset + bi);
            if (av != bv) {
                long x = av ^ bv;
                int o = BIG_ENDIAN
                        ? Long.numberOfLeadingZeros(x) >> (LOG2_BYTE_BIT_SIZE + log2ArrayIndexScale)
                        : Long.numberOfTrailingZeros(x) >> (LOG2_BYTE_BIT_SIZE + log2ArrayIndexScale);
                return (wi << log2ValuesPerWidth) + o;
            }
        }

        // Calculate the tail of remaining elements to check
        int tail = length - (wi << log2ValuesPerWidth);

        if (log2ArrayIndexScale < LOG2_ARRAY_INT_INDEX_SCALE) {
            int wordTail = 1 << (LOG2_ARRAY_INT_INDEX_SCALE - log2ArrayIndexScale);
            // Handle 4 bytes or 2 chars in the tail using int width
            if (tail >= wordTail) {
                long bi = ((long) wi) << LOG2_ARRAY_LONG_INDEX_SCALE;
                int av = UnsafeUtils.getIntUnaligned(a, aOffset + bi);
                int bv = UnsafeUtils.getIntUnaligned(b, bOffset + bi);
                if (av != bv) {
                    int x = av ^ bv;
                    int o = BIG_ENDIAN
                            ? Integer.numberOfLeadingZeros(x) >> (LOG2_BYTE_BIT_SIZE + log2ArrayIndexScale)
                            : Integer.numberOfTrailingZeros(x) >> (LOG2_BYTE_BIT_SIZE + log2ArrayIndexScale);
                    return (wi << log2ValuesPerWidth) + o;
                }
                tail -= wordTail;
            }
            return ~tail;
        }
        else {
            return ~tail;
        }
    }

    /**
     * Find the index of a mismatch between two arrays.
     *
     * <p>This method does not perform bounds checks. It is the responsibility
     * of the caller to perform such bounds checks before calling this method.
     *
     * @param a the first array to be tested for a mismatch
     * @param b the second array to be tested for a mismatch
     * @param length the number of bytes from each array to check
     * @return the index of a mismatch between the two arrays, otherwise -1 if
     * no mismatch.  The index will be within the range of (inclusive) 0 to
     * (exclusive) the smaller of the two array lengths.
     */
    public static int mismatch(byte[] a,
                               byte[] b,
                               int length) {
        // ISSUE: defer to index receiving methods if performance is good
        // assert length <= a.length
        // assert length <= b.length

        int i = 0;
        if (length > 7) {
            if (a[0] != b[0])
                return 0;
            i = vectorizedMismatch(
                    a, Unsafe.ARRAY_BYTE_BASE_OFFSET,
                    b, Unsafe.ARRAY_BYTE_BASE_OFFSET,
                    length, LOG2_ARRAY_BYTE_INDEX_SCALE);
            if (i >= 0)
                return i;
            // Align to tail
            i = length - ~i;
//            assert i >= 0 && i <= 7;
        }
        // Tail < 8 bytes
        for (; i < length; i++) {
            if (a[i] != b[i])
                return i;
        }
        return -1;
    }

    /**
     * Find the relative index of a mismatch between two arrays starting from
     * given indexes.
     *
     * <p>This method does not perform bounds checks. It is the responsibility
     * of the caller to perform such bounds checks before calling this method.
     *
     * @param a the first array to be tested for a mismatch
     * @param aFromIndex the index of the first element (inclusive) in the first
     * array to be compared
     * @param b the second array to be tested for a mismatch
     * @param bFromIndex the index of the first element (inclusive) in the
     * second array to be compared
     * @param length the number of bytes from each array to check
     * @return the relative index of a mismatch between the two arrays,
     * otherwise -1 if no mismatch.  The index will be within the range of
     * (inclusive) 0 to (exclusive) the smaller of the two array bounds.
     */
    public static int mismatch(byte[] a, int aFromIndex,
                               byte[] b, int bFromIndex,
                               int length) {
        // assert 0 <= aFromIndex < a.length
        // assert 0 <= aFromIndex + length <= a.length
        // assert 0 <= bFromIndex < b.length
        // assert 0 <= bFromIndex + length <= b.length
        // assert length >= 0

        int i = 0;
        if (length > 7) {
            if (a[aFromIndex] != b[bFromIndex])
                return 0;
            int aOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + aFromIndex;
            int bOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET + bFromIndex;
            i = vectorizedMismatch(
                    a, aOffset,
                    b, bOffset,
                    length, LOG2_ARRAY_BYTE_INDEX_SCALE);
            if (i >= 0)
                return i;
            i = length - ~i;
        }
        for (; i < length; i++) {
            if (a[aFromIndex + i] != b[bFromIndex + i])
                return i;
        }
        return -1;
    }
    public static int mismatch(char[] a,
                               char[] b,
                               int length) {
        int i = 0;
        if (length > 3) {
            if (a[0] != b[0])
                return 0;
            i = vectorizedMismatch(
                    a, Unsafe.ARRAY_CHAR_BASE_OFFSET,
                    b, Unsafe.ARRAY_CHAR_BASE_OFFSET,
                    length, LOG2_ARRAY_CHAR_INDEX_SCALE);
            if (i >= 0)
                return i;
            i = length - ~i;
        }
        for (; i < length; i++) {
            if (a[i] != b[i])
                return i;
        }
        return -1;
    }

}
