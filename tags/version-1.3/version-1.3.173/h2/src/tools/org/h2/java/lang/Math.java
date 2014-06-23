/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java.lang;

/**
 * A java.lang.String implementation.
 */
public class Math {

    /**
     * Get the larger of both values.
     *
     * @param a the first value
     * @param b the second value
     * @return the larger
     */
    public static int max(int a, int b) {
        return a > b ? a : b;
    }

}
