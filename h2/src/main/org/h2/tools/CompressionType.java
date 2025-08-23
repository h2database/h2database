/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.util.Locale;

/**
 * Compression types for SQL output
 */
public enum CompressionType {
    NONE,
    GZIP,
    ZIP,
    BZIP2,
    KANZI,
    DEFLATE,
    LZF;

    public static CompressionType from(String type) {
        return type==null || type.isEmpty()
            ? NONE
            : Enum.valueOf(CompressionType.class, type.toUpperCase(Locale.ENGLISH));
    }
}
