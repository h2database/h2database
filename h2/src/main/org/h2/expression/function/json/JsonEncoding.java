/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function.json;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Encoding of binary string with JSON text.
 */
public enum JsonEncoding {

    /**
     * UTF-8 (default).
     */
    UTF8(StandardCharsets.UTF_8),

    /**
     * UTF-16BE.
     */
    UTF16BE(StandardCharsets.UTF_16BE),

    /**
     * UTF-16LE.
     */
    UTF16LE(StandardCharsets.UTF_16LE),

    /**
     * UTF-32BE.
     */
    UTF32BE(Charset.forName("UTF-32BE")),

    /**
     * UTF-32LE.
     */
    UTF32LE(Charset.forName("UTF-32LE"));

    private final Charset charset;

    private JsonEncoding(Charset charset) {
        this.charset = charset;
    }

    /**
     * Returns encoding as {@linkplain Charset}.
     *
     * @return the charset
     */
    public Charset getCharset() {
        return charset;
    }

}
