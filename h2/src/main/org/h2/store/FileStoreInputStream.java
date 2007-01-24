/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.tools.CompressTool;

public class FileStoreInputStream extends InputStream {

    private FileStore store;
    private DataPage page;
    private int remaining;
    private CompressTool compress;
    
    public FileStoreInputStream(FileStore store, DataHandler handler, boolean compression) throws SQLException {
        this.store = store;
        if(compression) {
            compress = CompressTool.getInstance();
        }
        page = DataPage.create(handler, Constants.FILE_BLOCK_SIZE);
        try {
            if(store.length() <= FileStore.HEADER_LENGTH) {
                close();
            } else {
                fillBuffer();
            }
        } catch(IOException e) {
            throw Message.convert(e);
        }
    }

    public int available() {
        return remaining <= 0 ? 0 : remaining;
    }
    
    public int read(byte[] buff) throws IOException {
        return read(buff, 0, buff.length);
    }
    
    public int read(byte[] b, int off, int len) throws IOException {
        if(len == 0) {
            return 0;
        }
        int read = 0;
        while(len > 0) {
            int r = readBlock(b, off, len);
            if(r < 0) {
                break;
            }
            read += r;
            off += r;
            len -= r;
        }
        return read == 0 ? -1 : read;
    }
    
    public int readBlock(byte[] buff, int off, int len) throws IOException {
        fillBuffer();
        if(store == null) {
            return -1;
        }
        int l = Math.min(remaining, len);
        page.read(buff, off, l);
        remaining -= l;
        return l;
    }
    
    private void fillBuffer() throws IOException {
        if(remaining > 0 || store==null) {
            return;
        }
        page.reset();
        try {
            if(store.length() == store.getFilePointer()) {
                close();
                return;
            }
            store.readFully(page.getBytes(), 0, Constants.FILE_BLOCK_SIZE);
        } catch(SQLException e) {
            throw Message.convertToIOException(e);
        }
        page.reset();
        remaining = page.readInt();
        if(remaining<0) {
            close();
            return;
        }
        page.checkCapacity(remaining);
        // get the length to read
        if(compress != null) {
            page.checkCapacity(page.getIntLen());
            page.readInt();
        }
        page.setPos(page.length() + remaining);
        page.fillAligned();
        int len = page.length() - Constants.FILE_BLOCK_SIZE;
        page.reset();
        page.readInt();
        try {
            store.readFully(page.getBytes(), Constants.FILE_BLOCK_SIZE, len);
            page.reset();
            page.readInt();
            if(compress != null) {
                int uncompressed = page.readInt();
                byte[] buff = new byte[remaining];
                page.read(buff, 0, remaining);
                page.reset();
                page.checkCapacity(uncompressed);
                compress.expand(buff, page.getBytes(), 0);
                remaining = uncompressed;
            }
        } catch(SQLException e) {
            throw Message.convertToIOException(e);
        }
    }
    
    public void close() throws IOException {
        if(store != null) {
            try {
                store.close();
            } finally {
                store = null;
            }
        }
    }
    
    protected void finalize() {
        if (!Constants.RUN_FINALIZE) {
            return;
        }        
        try {
            close();
        } catch(IOException e) {
            // ignore
        }
    }
    
    public int read() throws IOException {
        fillBuffer();
        if(store == null) {
            return -1;
        }
        int i = page.readByte() & 0xff;
        remaining--;
        return i;
    }

}
