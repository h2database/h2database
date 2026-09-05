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
public enum CompressionType
{
    /**
     * No compression
     */
    NONE,

    /**
     * <A href="https://en.wikipedia.org/wiki/Gzip">GZIP compression</A>
     */
    GZIP,

    /**
     * <A href="https://en.wikipedia.org/wiki/ZIP_(file_format)">ZIP compression</A>
     */
    ZIP,

    /**
     * <A href="https://en.wikipedia.org/wiki/Bzip2">BZIP2 compression</A>
     */
    BZIP2,

    /**
     * <A href="https://github.com/flanglet/kanzi">KANZI compression</A>
     */
    KANZI,

    /**
     * <A href="https://en.wikipedia.org/wiki/Deflate">DEFLATE compression</A>
     */
    DEFLATE,

    /**
     * <A href="https://github.com/ning/compress/wiki/LZFFormat">LZF compression</A>
     */
    LZF;

    /**
     * Find instance of CompressionType by its name.
     * @param type name of CompressionType
     * @return instance of CompressionType or {@link #NONE} if provided type is empty
     */
    public static CompressionType from(String type) {
        return type==null || type.isEmpty()
            ? NONE
            : Enum.valueOf(CompressionType.class, type.toUpperCase(Locale.ENGLISH));
    }
}
