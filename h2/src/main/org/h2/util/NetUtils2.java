/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.IOException;
import java.net.Socket;

/**
 * This utility class contains additional socket helper functions. This class is
 * overridden in multi-release JAR with real implementation.
 *
 *
 * This utility class contains specialized implementation of additional socket
 * helper functions for Java 10 and later versions.
 */
public final class NetUtils2 {

    /*
     * Signatures of methods should match with
     * h2/src/java10/src/org/h2/util/NetUtils2.java and precompiled
     * h2/src/java10/precompiled/org/h2/util/NetUtils2.class.
     */

    /**
     * Returns the value of TCP_QUICKACK option.
     *
     * @param socket
     *            the socket
     * @return the current value of TCP_QUICKACK option
     * @throws IOException
     *             on I/O exception
     * @throws UnsupportedOperationException
     *             if TCP_QUICKACK is not supported
     */
    public static boolean getTcpQuickack(Socket socket) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets the value of TCP_QUICKACK option.
     *
     * @param socket
     *            the socket
     * @param value
     *            the value to set
     * @return whether operation was successful
     */
    public static boolean setTcpQuickack(Socket socket, boolean value) {
        // The default implementation does nothing
        return false;
    }

    private NetUtils2() {
    }

}
