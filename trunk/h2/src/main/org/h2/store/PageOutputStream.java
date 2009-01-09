/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import org.h2.index.Page;
import org.h2.message.Message;

/**
 * An output stream that writes into a page store.
 */
public class PageOutputStream extends OutputStream {

    private PageStore store;
    private int parentPage;
    private int type;
    private int pageId;
    private int nextPage;
    private DataPage page;
    private int remaining;

    /**
     * Create a new page output stream.
     *
     * @param store the page store
     * @param parentPage the parent page id
     * @param headPage the first page
     * @param type the page type
     */
    public PageOutputStream(PageStore store, int parentPage, int headPage, int type) {
        this.store = store;
        this.parentPage = parentPage;
        this.nextPage = headPage;
        this.type = type;
        page = store.createDataPage();
        initPage();
    }

    public void write(int b) throws IOException {
        write(new byte[] { (byte) b });
    }

    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    private void initPage() {
        page.reset();
        page.writeInt(parentPage);
        page.writeByte((byte) type);
        page.writeInt(0);
        remaining = store.getPageSize() - page.length();
    }

    public void write(byte[] b, int off, int len) throws IOException {
        if (len <= 0) {
            return;
        }
        while (len > remaining) {
            page.write(b, off, remaining);
            off += remaining;
            len -= remaining;
            parentPage = nextPage;
            pageId = nextPage;
            try {
                nextPage = store.allocatePage();
            } catch (SQLException e) {
                throw Message.convertToIOException(e);
            }
            page.setInt(5, nextPage);
            storePage();
            initPage();
        }
        page.write(b, off, len);
        remaining -= len;
    }

    private void storePage() throws IOException {
        try {
int test;
System.out.println("   pageOut.storePage " + pageId + " next:" + nextPage);
            store.writePage(pageId, page);
        } catch (SQLException e) {
            throw Message.convertToIOException(e);
        }
    }

    public void close() throws IOException {
        page.setPos(4);
        page.writeByte((byte) (type | Page.FLAG_LAST));
        page.writeInt(store.getPageSize() - remaining - 9);
        pageId = nextPage;
        storePage();
        store = null;
    }

    public void flush() throws IOException {
        int todo;
    }

    /**
     * Get the number of remaining bytes that fit in the current page.
     *
     * @return the number of bytes
     */
    public int getRemainingBytes() {
        return remaining;
    }

//    public void write(byte[] buff, int off, int len) throws IOException {
//        if (len > 0) {
//            try {
//                page.reset();
//                if (compress != null) {
//                    if (off != 0 || len != buff.length) {
//                        byte[] b2 = new byte[len];
//                        System.arraycopy(buff, off, b2, 0, len);
//                        buff = b2;
//                        off = 0;
//                    }
//                    int uncompressed = len;
//                    buff = compress.compress(buff, compressionAlgorithm);
//                    len = buff.length;
//                    page.writeInt(len);
//                    page.writeInt(uncompressed);
//                    page.write(buff, off, len);
//                } else {
//                    page.writeInt(len);
//                    page.write(buff, off, len);
//                }
//                page.fillAligned();
//                store.write(page.getBytes(), 0, page.length());
//            } catch (SQLException e) {
//                throw Message.convertToIOException(e);
//            }
//        }
//    }

}
