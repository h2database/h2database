/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.base64;

import java.util.Base64;

/**
 * Data encoding base64 algorithm.
 */
public class EncodingBase64 implements EncodingAlgorithm{
    /** {@inheritDoc} */
    @Override public String encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    /** {@inheritDoc} */
    @Override public byte[] decode(String src) {
        return Base64.getDecoder().decode(src);
    }
}
