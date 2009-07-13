/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.tools.CompressTool;
import org.h2.util.MemoryUtils;

/**
 * An input stream that is backed by a file store.
 */
public class FileStoreInputStream extends InputStream {

    private FileStore store;
    private DataPage page;
    private int remainingInBuffer;
    private CompressTool compress;
    private boolean endOfFile;
    private boolean alwaysClose;

    public FileStoreInputStream(FileStore store, DataHandler handler, boolean compression, boolean alwaysClose) throws SQLException {
        this.store = store;
        this.alwaysClose = alwaysClose;
        if (compression) {
            compress = CompressTool.getInstance();
        }
        page = DataPage.create(handler, Constants.FILE_BLOCK_SIZE);
        try {
            if (store.length() <= FileStore.HEADER_LENGTH) {
                close();
            } else {
                fillBuffer();
            }
        } catch (IOException e) {
            throw Message.convertIOException(e, store.name);
        }
    }

    public int available() {
        return remainingInBuffer <= 0 ? 0 : remainingInBuffer;
    }

    public int read(byte[] buff) throws IOException {
        return read(buff, 0, buff.length);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        int read = 0;
        while (len > 0) {
            int r = readBlock(b, off, len);
            if (r < 0) {
                break;
            }
            read += r;
            off += r;
            len -= r;
        }
        return read == 0 ? -1 : read;
    }

    private int readBlock(byte[] buff, int off, int len) throws IOException {
        fillBuffer();
        if (endOfFile) {
            return -1;
        }
        int l = Math.min(remainingInBuffer, len);
        page.read(buff, off, l);
        remainingInBuffer -= l;
        return l;
    }

    private void fillBuffer() throws IOException {
        if (remainingInBuffer > 0 || endOfFile) {
            return;
        }
        page.reset();
        try {
            store.openFile();
            if (store.length() == store.getFilePointer()) {
                close();
                return;
            }
            store.readFully(page.getBytes(), 0, Constants.FILE_BLOCK_SIZE);
        } catch (SQLException e) {
            throw Message.convertToIOException(e);
        }
        page.reset();
        remainingInBuffer = readInt();
        if (remainingInBuffer < 0) {
            close();
            return;
        }
        page.checkCapacity(remainingInBuffer);
        // get the length to read
        if (compress != null) {
            page.checkCapacity(DataPage.LENGTH_INT);
            readInt();
        }
        page.setPos(page.length() + remainingInBuffer);
        page.fillAligned();
        int len = page.length() - Constants.FILE_BLOCK_SIZE;
        page.reset();
        readInt();
        try {
            store.readFully(page.getBytes(), Constants.FILE_BLOCK_SIZE, len);
            page.reset();
            readInt();
            if (compress != null) {
                int uncompressed = readInt();
                byte[] buff = MemoryUtils.newBytes(remainingInBuffer);
                page.read(buff, 0, remainingInBuffer);
                page.reset();
                page.checkCapacity(uncompressed);
                compress.expand(buff, page.getBytes(), 0);
                remainingInBuffer = uncompressed;
            }
        } catch (SQLException e) {
            throw Message.convertToIOException(e);
        }
        if (alwaysClose) {
            store.closeFile();
        }
    }

    public void close() throws IOException {
        if (store != null) {
            try {
                store.close();
                endOfFile = true;
            } finally {
                store = null;
            }
        }
    }

    protected void finalize() {
        if (!SysProperties.runFinalize) {
            return;
        }
        try {
            close();
        } catch (IOException e) {
            // ignore
        }
    }

    public int read() throws IOException {
        fillBuffer();
        if (endOfFile) {
            return -1;
        }
        int i = page.readByte() & 0xff;
        remainingInBuffer--;
        return i;
    }

    private int readInt() {
        if (store.isTextMode()) {
            byte[] buff = new byte[8];
            page.read(buff, 0, 8);
            String s = new String(buff);
            return Integer.parseInt(s, 16);
        }
        return page.readInt();
    }

}
