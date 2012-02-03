/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
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
    public static void arraycopy(char[] src, int srcPos, char[] dest, int destPos, int length) {
        /* c:
        memmove(((jchar*)dest) + destPos,
            ((jchar*)src) + srcPos, sizeof(jchar) * length);
        */
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
    public static void arraycopy(byte[] src, int srcPos, byte[] dest, int destPos, int length) {
        /* c:
        memmove(((jbyte*)dest) + destPos,
            ((jbyte*)src) + srcPos, sizeof(jbyte) * length);
        */
    }


}
