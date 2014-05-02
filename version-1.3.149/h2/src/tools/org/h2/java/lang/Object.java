/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java.lang;

/**
 * A java.lang.Object implementation.
 */
public class Object {

    private static final int[] K = { 1 };

    public int hashCode() {
        return K[0];
    }

    public boolean equals(Object other) {
        return this == other;
    }

}
