/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.compress;

import java.sql.SQLException;
import java.util.StringTokenizer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.h2.constant.ErrorCode;
import org.h2.message.Message;

/**
 * This is a wrapper class for the Deflater class.
 * This algorithm supports the following options:
 * <ul>
 * <li>l or level: -1 (default), 0 (no compression),
 *  1 (best speed), ..., 9 (best compression)
 * </li><li>s or strategy: 0 (default),
 *  1 (filtered), 2 (huffman only)
 * </li></ul>
 * See also java.util.zip.Deflater for details.
 */
public class CompressDeflate implements Compressor {

    private int level = Deflater.DEFAULT_COMPRESSION;
    private int strategy = Deflater.DEFAULT_STRATEGY;

    public void setOptions(String options) throws SQLException {
        if (options == null) {
            return;
        }
        try {
            StringTokenizer tokenizer = new StringTokenizer(options);
            while (tokenizer.hasMoreElements()) {
                String option = tokenizer.nextToken();
                if ("level".equals(option) || "l".equals(option)) {
                    level = Integer.parseInt(tokenizer.nextToken());
                } else if ("strategy".equals(option) || "s".equals(option)) {
                    strategy = Integer.parseInt(tokenizer.nextToken());
                }
                Deflater deflater = new Deflater(level);
                deflater.setStrategy(strategy);
            }
        } catch (Exception e) {
            throw Message.getSQLException(ErrorCode.UNSUPPORTED_COMPRESSION_OPTIONS_1, options);
        }
    }

    public int compress(byte[] in, int inLen, byte[] out, int outPos) {
        Deflater deflater = new Deflater(level);
        deflater.setStrategy(strategy);
        deflater.setInput(in, 0, inLen);
        deflater.finish();
        int compressed = deflater.deflate(out, outPos, out.length - outPos);
        while (compressed == 0) {
            // the compressed data is 0, meaning compression didn't work
            // (sounds like a JDK bug)
            // try again, using the default strategy and compression level
            strategy = Deflater.DEFAULT_STRATEGY;
            level = Deflater.DEFAULT_COMPRESSION;
            return compress(in, inLen, out, outPos);
        }
        return compressed;
    }

    public int getAlgorithm() {
        return Compressor.DEFLATE;
    }

    public void expand(byte[] in, int inPos, int inLen, byte[] out, int outPos, int outLen) throws SQLException {
        Inflater decompresser = new Inflater();
        decompresser.setInput(in, inPos, inLen);
        decompresser.finished();
        try {
            decompresser.inflate(out, outPos, outLen);
        } catch (DataFormatException e) {
            throw Message.getSQLException(ErrorCode.COMPRESSION_ERROR, e);
        }
        decompresser.end();
    }

}
