/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.compress;

import java.sql.SQLException;

/**
 * Each data compression algorithm must implement this interface.
 */
public interface Compressor {

    /**
     * No compression is used.
     */
    int NO = 0;

    /**
     * The LZF compression algorithm is used
     */
    int LZF = 1;

    /**
     * The DEFLATE compression algorithm is used.
     */
    int DEFLATE = 2;

    /**
     * Get the compression algorithm type.
     *
     * @return the type
     */
    int getAlgorithm();

    /**
     * Compress a number of bytes.
     *
     * @param in the input data
     * @param inLen the number of bytes to compress
     * @param out the output area
     * @param outPos the offset at the output array
     * @return the size of the compressed data
     */
    int compress(byte[] in, int inLen, byte[] out, int outPos);

    /**
     * Expand a number of compressed bytes.
     *
     * @param in the compressed data
     * @param inPos the offset at the input array
     * @param inLen the number of bytes to read
     * @param out the output area
     * @param outPos the offset at the output array
     * @param outLen the size of the uncompressed data
     */
    void expand(byte[] in, int inPos, int inLen, byte[] out, int outPos, int outLen) throws SQLException;

    /**
     * Set the compression options. This may include settings for
     * higher performance but less compression.
     *
     * @param options the options
     */
    void setOptions(String options) throws SQLException;
}
