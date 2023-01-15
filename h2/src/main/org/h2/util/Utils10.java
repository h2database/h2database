/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.nio.charset.Charset;

/**
 * Utilities with specialized implementations for Java 10 and later versions.
 *
 * This class contains basic implementations for Java 8 and 9 and it is
 * overridden in multi-release JARs.
 */
public final class Utils10 {

    /*
     * Signatures of methods should match with
     * h2/src/java10/src/org/h2/util/Utils10.java and precompiled
     * h2/src/java10/precompiled/org/h2/util/Utils10.class.
     */

    /**
     * Converts the buffer's contents into a string by decoding the bytes using
     * the specified {@link java.nio.charset.Charset charset}.
     *
     * @param baos
     *            the buffer to decode
     * @param charset
     *            the charset to use
     * @return the decoded string
     */
    public static String byteArrayOutputStreamToString(ByteArrayOutputStream baos, Charset charset) {
        try {
            return baos.toString(charset.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

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

    private Utils10() {
    }

}
