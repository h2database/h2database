/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.compress;
import java.sql.SQLException;
import java.util.StringTokenizer;
import java.util.zip.*;

import org.h2.message.Message;

public class CompressDeflate implements Compressor {
    
    private int level = Deflater.BEST_SPEED;
    private int strategy = Deflater.DEFAULT_STRATEGY;
    
    public void setOptions(String options) throws SQLException {
        if(options == null) {
            return;
        }
        try {
            StringTokenizer tokenizer = new StringTokenizer(options);
            while(tokenizer.hasMoreElements()) {
                String option = tokenizer.nextToken();
                if("level".equals(option) || "l".equals(option)) {
                    level = Integer.parseInt(tokenizer.nextToken());
                } else if("strategy".equals(option) || "s".equals(option)) {
                    strategy = Integer.parseInt(tokenizer.nextToken());
                }
            }
        } catch(Exception e) {
            throw Message.getSQLException(Message.UNSUPPORTED_COMPRESSION_OPTIONS_1, options);
        }
    }
    
    public int compress(byte[] in, int inLen, byte[] out, int outPos) {
        Deflater deflater = new Deflater(level);
        deflater.setStrategy(strategy);
        deflater.setInput(in, 0, inLen);
        deflater.finish();
        int compressed = deflater.deflate(out, outPos, out.length - outPos);
        return compressed;
    }

    public int getAlgorithm() {
        return Compressor.DEFLATE;
    }

    public void expand(byte[] in, int inPos, int inLen, byte[] out, int outPos, int outLen) throws DataFormatException {
        Inflater decompresser = new Inflater();
        decompresser.setInput(in, inPos, inLen);
        decompresser.finished();
        decompresser.inflate(out, outPos, outLen);
        decompresser.end();
    }

}
