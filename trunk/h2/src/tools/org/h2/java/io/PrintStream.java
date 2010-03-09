package org.h2.java.io;

import org.h2.java.lang.System;

/**
 * A print stream.
 */
public class PrintStream {

    /**
     * Print the given string.
     *
     * @param s the string
     */
    public void println(String s) {
        System.arraycopy(null, 0, null, 0, 1);
        // c: printf("%s\n");
    }

}
