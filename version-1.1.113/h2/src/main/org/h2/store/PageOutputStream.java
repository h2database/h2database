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
    private int type;
    private int parentPage;
    private int pageId;
    private int nextPage;
    private DataPage page;
    private int remaining;
    private final boolean allocateAtEnd;
    private byte[] buffer = new byte[1];
    private boolean needFlush;
    private final int streamId;

    /**
     * Create a new page output stream.
     *
     * @param store the page store
     * @param parentPage the parent page id
     * @param headPage the first page
     * @param type the page type
     * @param streamId the stream identifier
     * @param allocateAtEnd whether new pages should be allocated at the end of
     *            the file
     */
    public PageOutputStream(PageStore store, int parentPage, int headPage, int type, int streamId, boolean allocateAtEnd) {
        this.trace = store.getTrace();
        this.store = store;
        this.parentPage = parentPage;
        this.pageId = headPage;
        this.type = type;
        this.allocateAtEnd = allocateAtEnd;
        this.streamId = streamId;
        page = store.createDataPage();
        initPage();
    }

    public void write(int b) throws IOException {
        buffer[0] = (byte) b;
        write(buffer);
    }

    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    private void initPage() {
        page.reset();
        page.writeInt(parentPage);
        page.writeByte((byte) type);
        page.writeByte((byte) streamId);
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
            try {
                nextPage = store.allocatePage(allocateAtEnd);
            } catch (SQLException e) {
                throw Message.convertToIOException(e);
            }
            page.setPos(4);
            page.writeByte((byte) type);
            page.writeByte((byte) streamId);
            page.writeInt(nextPage);
            storePage();
            parentPage = pageId;
            pageId = nextPage;
            initPage();
        }
        page.write(b, off, len);
        needFlush = true;
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

    public void flush() throws IOException {
        if (needFlush) {
            int len = page.length();
            page.setPos(4);
            page.writeByte((byte) (type | Page.FLAG_LAST));
            page.writeByte((byte) streamId);
            page.writeInt(store.getPageSize() - remaining - 9);
            page.setPos(len);
            storePage();
            needFlush = false;
        }
    }

    public void close() throws IOException {
        flush();
        store = null;
    }

}
