/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.tools.CompressTool;

public class FileStoreOutputStream extends OutputStream {
    private FileStore store;
    private DataPage page;
    private String compressionAlgorithm;
    private CompressTool compress;
    
    public FileStoreOutputStream(FileStore store, DataHandler handler, String compressionAlgorithm) {
        this.store = store;
        if (compressionAlgorithm != null) {
            compress = CompressTool.getInstance();
            this.compressionAlgorithm = compressionAlgorithm;
        }
        page = DataPage.create(handler, Constants.FILE_BLOCK_SIZE);
    }

    public void write(byte[] buff) throws IOException {
        write(buff, 0, buff.length);
    }

    public void write(byte[] buff, int off, int len) throws IOException {
        if (len > 0) {
            try {
                page.reset();
                if (compress != null) {
                    if (off != 0 || len != buff.length) {
                        byte[] b2 = new byte[len];
                        System.arraycopy(buff, off, b2, 0, len);
                        buff = b2;
                        off = 0;
                    }
                    int uncompressed = len;
                    buff = compress.compress(buff, compressionAlgorithm);
                    len = buff.length;
                    page.writeInt(len);
                    page.writeInt(uncompressed);
                    page.write(buff, off, len);
                } else {
                    page.writeInt(len);
                    page.write(buff, off, len);
                }
                page.fillAligned();
                store.write(page.getBytes(), 0, page.length());
            } catch (SQLException e) {
                throw Message.convertToIOException(e);
            }
        }
    }

    public void close() throws IOException {
        if (store != null) {
            try {
                store.close();
            } finally {
                store = null;
            }
        }
    }

    public void write(int b) throws IOException {
        throw new IOException("this method is not implemented");
    }
    
}
