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
