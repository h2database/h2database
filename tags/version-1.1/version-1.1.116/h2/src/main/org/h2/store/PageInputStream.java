/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import org.h2.message.Trace;

/**
 * An input stream that reads from a page store.
 */
public class PageInputStream extends InputStream {

    private PageStore store;
    private final Trace trace;
    private int trunkNext;
    private int dataPage;
    private PageStreamTrunk trunk;
    private PageStreamData data;
    private boolean endOfFile;
    private int remaining;
    private byte[] buffer = new byte[1];

    PageInputStream(PageStore store, int trunkPage, int dataPage) {
        this.store = store;
        this.trace = store.getTrace();
        this.trunkNext = trunkPage;
        this.dataPage = dataPage;
    }

    public int read() throws IOException {
        int len = read(buffer);
        return len < 0 ? -1 : (buffer[0] & 255);
    }

    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
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
        try {
            fillBuffer();
            if (endOfFile) {
                return -1;
            }
            int l = Math.min(remaining, len);
            data.read(buff, off, l);
            remaining -= l;
            return l;
        } catch (SQLException e) {
            throw new EOFException();
        }
    }

    private void fillBuffer() throws SQLException {
        if (remaining > 0 || endOfFile) {
            return;
        }
        if (trunkNext == 0) {
            endOfFile = true;
            return;
        }
        if (trunk == null) {
            trunk = new PageStreamTrunk(store, trunkNext);
            trunk.read();
        }
        int next;
        while (true) {
            next = trunk.getNextPageData();
            if (dataPage == -1 || dataPage == next) {
                if (next != 0) {
                    break;
                }
                trunk = new PageStreamTrunk(store, trunkNext);
                trunk.read();
            }
        }
        if (trace.isDebugEnabled()) {
            trace.debug("pageIn.readPage " + next);
        }
        dataPage = -1;
        data = new PageStreamData(store, next, 0);
        data.read();
        remaining = data.getLength();
    }

    /**
     * Set all pages as 'allocated' in the page store.
     */
    void allocateAllPages() throws SQLException {
        int trunkPage = trunkNext;
        while (trunkPage != 0) {
            store.allocatePage(trunkPage);
            PageStreamTrunk t = new PageStreamTrunk(store, trunkPage);
            t.read();
            while (true) {
                int n = t.getNextPageData();
                if (n == -1) {
                    break;
                }
                store.allocatePage(n);
            }
            trunkPage = t.getNextTrunk();
        }
    }

    int getDataPage() {
        return data.getPos();
    }

}
