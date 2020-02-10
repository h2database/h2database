/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.base64;

/**
 * Data encoding algorithm.
 */
public interface EncodingAlgorithm {
    /**
     * Encode.
     *
     * @param src the byte array to encode
     * @return A String containing the resulting Base64 encoded characters
     */
    String encode(byte[] src);

    /**
     * Decode.
     *
     * @param src the string to decode
     * @return  byte array containing the decoded bytes.
     */
    byte[] decode(String src);
}
