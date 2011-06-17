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
import org.h2.constant.ErrorCode;
import org.h2.index.Page;
import org.h2.message.Message;

/**
 * An output stream that writes into a page store.
 * The format is:
 * <ul><li>0-3: parent page id
 * </li><li>4-4: page type
 * </li><li>5-8: the next page (if there is one) or length
 * </li><li>9-remainder: data
 * </li></ul>
 */
public class PageInputStream extends InputStream {

    private PageStore store;
    private int parentPage;
    private int type;
    private int nextPage;
    private DataPage page;
    private boolean endOfFile;
    private int remaining;

    public PageInputStream(PageStore store, int parentPage, int headPage, int type) {
        this.store = store;
        this.parentPage = parentPage;
        this.type = type;
        nextPage = headPage;
        page = store.createDataPage();
    }

    public int read() throws IOException {
        byte[] b = new byte[1];
        int len = read(b);
        return len < 0 ? -1 : (b[0] & 255);
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
        fillBuffer();
        if (endOfFile) {
            return -1;
        }
        int l = Math.min(remaining, len);
        page.read(buff, off, l);
        remaining -= l;
        return l;
    }

    private void fillBuffer() throws IOException {
        if (remaining > 0 || endOfFile) {
            return;
        }
        if (nextPage == 0) {
            endOfFile = true;
            return;
        }
        page.reset();
        try {
            store.readPage(nextPage, page);
            int p = page.readInt();
            int t = page.readByte();
            boolean last = (t & Page.FLAG_LAST) != 0;
            t &= ~Page.FLAG_LAST;
            if (type != t || p != parentPage) {
                throw Message.getSQLException(
                        ErrorCode.FILE_CORRUPTED_1,
                        "page:" +nextPage+ " type:" + t + " parent:" + p +
                        " expected type:" + type + " expected parent:" + parentPage);
            }
            parentPage = nextPage;
            if (last) {
                nextPage = 0;
                remaining = page.readInt();
            } else {
                nextPage = page.readInt();
                remaining = store.getPageSize() - page.length();
            }
        } catch (SQLException e) {
            throw Message.convertToIOException(e);
        }
    }

}
