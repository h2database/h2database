package org.h2.util;

/**
 * Backports of Java 9's Array functions, as far as they are needed for Java 8 code.
 */
public class ArrayUtils {

    /**
     * Finds and returns the relative index of the first mismatch between two
     * {@code byte} arrays over the specified ranges, otherwise return -1 if no
     * mismatch is found.  The index will be in the range of 0 (inclusive) up to
     * the length (inclusive) of the smaller range.
     *
     * <p>If the two arrays, over the specified ranges, share a common prefix
     * then the returned relative index is the length of the common prefix and
     * it follows that there is a mismatch between the two elements at that
     * relative index within the respective arrays.
     * If one array is a proper prefix of the other, over the specified ranges,
     * then the returned relative index is the length of the smaller range and
     * it follows that the relative index is only valid for the array with the
     * larger range.
     * Otherwise, there is no mismatch.
     *
     * <p>Two non-{@code null} arrays, {@code a} and {@code b} with specified
     * ranges [{@code aFromIndex}, {@code atoIndex}) and
     * [{@code bFromIndex}, {@code btoIndex}) respectively, share a common
     * prefix of length {@code pl} if the following expression is true:
     * <pre>{@code
     *     pl >= 0 &&
     *     pl < Math.min(aToIndex - aFromIndex, bToIndex - bFromIndex) &&
     *     Arrays.equals(a, aFromIndex, aFromIndex + pl, b, bFromIndex, bFromIndex + pl) &&
     *     a[aFromIndex + pl] != b[bFromIndex + pl]
     * }</pre>
     * Note that a common prefix length of {@code 0} indicates that the first
     * elements from each array mismatch.
     *
     * <p>Two non-{@code null} arrays, {@code a} and {@code b} with specified
     * ranges [{@code aFromIndex}, {@code atoIndex}) and
     * [{@code bFromIndex}, {@code btoIndex}) respectively, share a proper
     * if the following expression is true:
     * <pre>{@code
     *     (aToIndex - aFromIndex) != (bToIndex - bFromIndex) &&
     *     Arrays.equals(a, 0, Math.min(aToIndex - aFromIndex, bToIndex - bFromIndex),
     *                   b, 0, Math.min(aToIndex - aFromIndex, bToIndex - bFromIndex))
     * }</pre>
     *
     * @param a the first array to be tested for a mismatch
     * @param aFromIndex the index (inclusive) of the first element in the
     *                   first array to be tested
     * @param aToIndex the index (exclusive) of the last element in the
     *                 first array to be tested
     * @param b the second array to be tested for a mismatch
     * @param bFromIndex the index (inclusive) of the first element in the
     *                   second array to be tested
     * @param bToIndex the index (exclusive) of the last element in the
     *                 second array to be tested
     * @return the relative index of the first mismatch between the two arrays
     *         over the specified ranges, otherwise {@code -1}.
     * @throws IllegalArgumentException
     *         if {@code aFromIndex > aToIndex} or
     *         if {@code bFromIndex > bToIndex}
     * @throws ArrayIndexOutOfBoundsException
     *         if {@code aFromIndex < 0 or aToIndex > a.length} or
     *         if {@code bFromIndex < 0 or bToIndex > b.length}
     * @throws NullPointerException
     *         if either array is {@code null}
     * @since 9
     */
    public static int mismatch(byte[] a, int aFromIndex, int aToIndex,
                               byte[] b, int bFromIndex, int bToIndex) {
        rangeCheck(a.length, aFromIndex, aToIndex);
        rangeCheck(b.length, bFromIndex, bToIndex);

        int aLength = aToIndex - aFromIndex;
        int bLength = bToIndex - bFromIndex;
        int length = Math.min(aLength, bLength);
        int i = ArraysSupport.mismatch(a, aFromIndex,
                b, bFromIndex,
                length);
        return (i < 0 && aLength != bLength) ? length : i;
    }

    /**
     * Checks that {@code fromIndex} and {@code toIndex} are in
     * the range and throws an exception if they aren't.
     */
    static void rangeCheck(int arrayLength, int fromIndex, int toIndex) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException(
                    "fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
        }
        if (fromIndex < 0) {
            throw new ArrayIndexOutOfBoundsException(fromIndex);
        }
        if (toIndex > arrayLength) {
            throw new ArrayIndexOutOfBoundsException(toIndex);
        }
    }

    /**
     * Compares two {@code byte} arrays lexicographically.
     *
     * <p>If the two arrays share a common prefix then the lexicographic
     * comparison is the result of comparing two elements, as if by
     * {@link Byte#compare(byte, byte)}, at an index within the respective
     * arrays that is the prefix length.
     * Otherwise, one array is a proper prefix of the other and, lexicographic
     * comparison is the result of comparing the two array lengths.
     * (See Java 9's {@code #mismatch(byte[], byte[])} for the definition of a common and
     * proper prefix.)
     *
     * <p>A {@code null} array reference is considered lexicographically less
     * than a non-{@code null} array reference.  Two {@code null} array
     * references are considered equal.
     *
     * <p>The comparison is consistent with Java 9's {@code #equals(byte[], byte[]) equals},
     * more specifically the following holds for arrays {@code a} and {@code b}:
     * <pre>{@code
     *     Arrays.equals(a, b) == (Arrays.compare(a, b) == 0)
     * }</pre>
     *
     * @apiNote
     * <p>This method behaves as if (for non-{@code null} array references):
     * <pre>{@code
     *     int i = Arrays.mismatch(a, b);
     *     if (i >= 0 && i < Math.min(a.length, b.length))
     *         return Byte.compare(a[i], b[i]);
     *     return a.length - b.length;
     * }</pre>
     *
     * @param a the first array to compare
     * @param b the second array to compare
     * @return the value {@code 0} if the first and second array are equal and
     *         contain the same elements in the same order;
     *         a value less than {@code 0} if the first array is
     *         lexicographically less than the second array; and
     *         a value greater than {@code 0} if the first array is
     *         lexicographically greater than the second array
     * @since 9
     */
    public static int compare(byte[] a, byte[] b) {
        if (a == b)
            return 0;
        if (a == null || b == null)
            return a == null ? -1 : 1;

        int i = ArraysSupport.mismatch(a, b,
                Math.min(a.length, b.length));
        if (i >= 0) {
            return Byte.compare(a[i], b[i]);
        }

        return a.length - b.length;
    }

    /**
     * Compares two {@code char} arrays lexicographically.
     *
     * <p>If the two arrays share a common prefix then the lexicographic
     * comparison is the result of comparing two elements, as if by
     * {@link Character#compare(char, char)}, at an index within the respective
     * arrays that is the prefix length.
     * Otherwise, one array is a proper prefix of the other and, lexicographic
     * comparison is the result of comparing the two array lengths.
     * (See Java 9's {@code #mismatch(char[], char[])} for the definition of a common and
     * proper prefix.)
     *
     * <p>A {@code null} array reference is considered lexicographically less
     * than a non-{@code null} array reference.  Two {@code null} array
     * references are considered equal.
     *
     * <p>The comparison is consistent with Java 9's {@code #equals(char[], char[]) equals},
     * more specifically the following holds for arrays {@code a} and {@code b}:
     * <pre>{@code
     *     Arrays.equals(a, b) == (Arrays.compare(a, b) == 0)
     * }</pre>
     *
     * @apiNote
     * <p>This method behaves as if (for non-{@code null} array references):
     * <pre>{@code
     *     int i = Arrays.mismatch(a, b);
     *     if (i >= 0 && i < Math.min(a.length, b.length))
     *         return Character.compare(a[i], b[i]);
     *     return a.length - b.length;
     * }</pre>
     *
     * @param a the first array to compare
     * @param b the second array to compare
     * @return the value {@code 0} if the first and second array are equal and
     *         contain the same elements in the same order;
     *         a value less than {@code 0} if the first array is
     *         lexicographically less than the second array; and
     *         a value greater than {@code 0} if the first array is
     *         lexicographically greater than the second array
     * @since 9
     */
    public static int compare(char[] a, char[] b) {
        if (a == b)
            return 0;
        if (a == null || b == null)
            return a == null ? -1 : 1;

        int i = ArraysSupport.mismatch(a, b,
                Math.min(a.length, b.length));
        if (i >= 0) {
            return Character.compare(a[i], b[i]);
        }

        return a.length - b.length;
    }

    /**
     * Compares two {@code byte} arrays lexicographically, numerically treating
     * elements as unsigned.
     *
     * <p>If the two arrays share a common prefix then the lexicographic
     * comparison is the result of comparing two elements, as if by
     * Java 9's {@code Byte#compareUnsigned(byte, byte)}, at an index within the
     * respective arrays that is the prefix length.
     * Otherwise, one array is a proper prefix of the other and, lexicographic
     * comparison is the result of comparing the two array lengths.
     * (See Java 9's {@code #mismatch(byte[], byte[])} for the definition of a common
     * and proper prefix.)
     *
     * <p>A {@code null} array reference is considered lexicographically less
     * than a non-{@code null} array reference.  Two {@code null} array
     * references are considered equal.
     *
     * @apiNote
     * <p>This method behaves as if (for non-{@code null} array references):
     * <pre>{@code
     *     int i = Arrays.mismatch(a, b);
     *     if (i >= 0 && i < Math.min(a.length, b.length))
     *         return Byte.compareUnsigned(a[i], b[i]);
     *     return a.length - b.length;
     * }</pre>
     *
     * @param a the first array to compare
     * @param b the second array to compare
     * @return the value {@code 0} if the first and second array are
     *         equal and contain the same elements in the same order;
     *         a value less than {@code 0} if the first array is
     *         lexicographically less than the second array; and
     *         a value greater than {@code 0} if the first array is
     *         lexicographically greater than the second array
     * @since 9
     */
    public static int compareUnsigned(byte[] a, byte[] b) {
        if (a == b)
            return 0;
        if (a == null || b == null)
            return a == null ? -1 : 1;

        int i = ArraysSupport.mismatch(a, b,
                Math.min(a.length, b.length));
        if (i >= 0) {
            return ByteUtils.compareUnsigned(a[i], b[i]);
        }

        return a.length - b.length;
    }
}
