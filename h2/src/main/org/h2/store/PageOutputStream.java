/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
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
import org.h2.message.Trace;

/**
 * An output stream that writes into a page store.
 */
public class PageOutputStream extends OutputStream {

    private final Trace trace;
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
        this.trace = store.getTrace();
        this.store = store;
        this.parentPage = parentPage;
        this.nextPage = headPage;
        this.type = type;
        page = store.createDataPage();
        initPage();
    }

    public void write(int b) throws IOException {
        int todoOptimizeIfNeeded;
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
        while (len >= remaining) {
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
            if (trace.isDebugEnabled()) {
                trace.debug("pageOut.storePage " + pageId + " next:" + nextPage);
            }
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

}
