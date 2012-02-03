/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java.lang;

/**
 * A java.lang.String implementation.
 */
public class StringBuilder {

    private int length;
    private char[] buffer;

    public StringBuilder() {
        buffer = new char[10];
    }

    /**
     * Append the given string.
     *
     * @param s the string
     * @return this
     */
    public StringBuilder append(String s) {
        int l = s.length();
        ensureCapacity(l);
        System.arraycopy(s.chars, 0, buffer, length, l);
        length += l;
        return this;
    }

    private void ensureCapacity(int plus) {
        if (buffer.length < length + plus) {
            char[] b = new char[Math.max(length + plus, buffer.length * 2)];
            System.arraycopy(buffer, 0, b, 0, length);
            buffer = b;
        }
    }

}
