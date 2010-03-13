package org.h2.java.lang;

import java.io.PrintStream;

/**
 * A simple java.lang.System implementation.
 */
public class System {

    /**
     * The stdout stream.
     */
    public static PrintStream out;

    /**
     * Copy data from the source to the target.
     * Source and target may overlap.
     *
     * @param src the source array
     * @param srcPos the first element in the source array
     * @param dest the destination
     * @param destPos the first element in the destination
     * @param length the number of element to copy
     */
    public static void arraycopy(java.lang.Object src, int srcPos, java.lang.Object dest, int destPos, int length) {
        // TODO
        // c: memmove(dest + destPos, src + srcPos, length);
    }

    /**
     * Copy data from the source to the target.
     * Source and target may overlap.
     *
     * @param src the source array
     * @param srcPos the first element in the source array
     * @param dest the destination
     * @param destPos the first element in the destination
     * @param length the number of element to copy
     */
    public static void arraycopyChars(char[] src, int srcPos, char[] dest, int destPos, int length) {
        // c: memmove(((jchar*)dest) + destPos, ((jchar*)src) + srcPos, sizeof(jchar) * length);
    }

}
