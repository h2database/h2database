/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.compress;

import java.sql.SQLException;

/**
 * Each data compression algorithm must implement this interface.
 */
public interface Compressor {

    int NO = 0, LZF = 1, DEFLATE = 2;

    int getAlgorithm();
    int compress(byte[] in, int inLen, byte[] out, int outPos);
    void expand(byte[] in, int inPos, int inLen, byte[] out, int outPos, int outLen) throws SQLException;
    void setOptions(String options) throws SQLException;
}
