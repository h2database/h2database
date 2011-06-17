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
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.util.BitField;
import org.h2.util.IntArray;

/**
 * An output stream that writes into a page store.
 */
public class PageOutputStream extends OutputStream {

    private PageStore store;
    private final Trace trace;
    private int trunkPageId;
    private final BitField exclude;

    private int trunkNext;
    private IntArray reservedPages = new IntArray();
    private PageStreamTrunk trunk;
    private PageStreamData data;
    private int reserved;
    private int remaining;
    private byte[] buffer = new byte[1];
    private boolean needFlush;
    private boolean writing;
    private int pages;
    private boolean atEnd;

    /**
     * Create a new page output stream.
     *
     * @param store the page store
     * @param trunkPage the first trunk page (already allocated)
     * @param atEnd whether only pages at the end of the file should be used
     */
    public PageOutputStream(PageStore store, int trunkPage, BitField exclude, boolean atEnd) {
        this.trace = store.getTrace();
        this.store = store;
        this.trunkPageId = trunkPage;
        this.exclude = exclude;
        this.atEnd = atEnd;
    }

    /**
     * Allocate the required pages so that no pages need to be allocated while
     * writing.
     *
     * @param minBuffer the number of bytes to allocate
     */
    void reserve(int minBuffer) throws SQLException {
        if (reserved < minBuffer) {
            int pageSize = store.getPageSize();
            int capacityPerPage = PageStreamData.getCapacity(pageSize);
            int pages = PageStreamTrunk.getPagesAddressed(pageSize);
            // allocate x data pages
            int pagesToAllocate = pages;
            int totalCapacity = pages * capacityPerPage;
            while (totalCapacity < minBuffer) {
                pagesToAllocate += pagesToAllocate;
                totalCapacity += totalCapacity;
            }
            // allocate the next trunk page as well
            pagesToAllocate++;
            int firstPageToUse = atEnd ? trunkPageId : 0;
            store.allocatePages(reservedPages, pagesToAllocate, exclude, firstPageToUse);
            reserved += totalCapacity;
            if (data == null) {
                initNextData();
            }
        }
    }

    public void write(int b) throws IOException {
        buffer[0] = (byte) b;
        write(buffer);
    }

    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    private void initNextData() throws SQLException {
        int nextData = trunk == null ? -1 : trunk.getNextPageData();
        if (nextData == -1) {
            int parent = trunkPageId;
            if (trunkNext != 0) {
                trunkPageId = trunkNext;
            }
            int len = PageStreamTrunk.getPagesAddressed(store.getPageSize());
            int[] pageIds = new int[len];
            for (int i = 0; i < len; i++) {
                pageIds[i] = reservedPages.get(i);
            }
            trunkNext = reservedPages.get(len);
            trunk = PageStreamTrunk.create(store, parent, trunkPageId, trunkNext, pageIds);
            pages++;
            trunk.write(null);
            reservedPages.removeRange(0, len + 1);
            nextData = trunk.getNextPageData();
        }
        data = PageStreamData.create(store, nextData, trunk.getPos());
        pages++;
        data.initWrite();
    }

    public void write(byte[] b, int off, int len) throws IOException {
        if (len <= 0) {
            return;
        }
        if (writing) {
            Message.throwInternalError("writing while still writing");
        }
        try {
            reserve(len);
            writing = true;
            while (len > 0) {
                int l = data.write(b, off, len);
                if (l < len) {
                    storePage();
                    initNextData();
                }
                reserved -= l;
                off += l;
                len -= l;
            }
            needFlush = true;
            remaining -= len;
        } catch (SQLException e) {
            throw Message.convertToIOException(e);
        } finally {
            writing = false;
        }
    }

    private void storePage() throws IOException {
        try {
            if (trace.isDebugEnabled()) {
                trace.debug("pageOut.storePage " + data.getPos());
            }
            data.write(null);
        } catch (SQLException e) {
            throw Message.convertToIOException(e);
        }
    }

    public void flush() throws IOException {
        if (needFlush) {
            storePage();
            needFlush = false;
        }
    }

    public void close() {
        store = null;
    }

    int getCurrentDataPageId() {
        return data.getPos();
    }

    /**
     * Fill the data page with zeros and write it.
     * This is required for a checkpoint.
     */
    void fillPage() throws SQLException {
        if (trace.isDebugEnabled()) {
            trace.debug("pageOut.storePage fill " + data.getPos());
        }
        reserve(data.getRemaining() + 1);
        reserved -= data.getRemaining();
        data.write(null);
        initNextData();
    }

    long getSize() {
        return pages * store.getPageSize();
    }

    /**
     * Remove a trunk page from the stream.
     *
     * @param t the trunk page
     */
    void free(PageStreamTrunk t) throws SQLException {
        pages -= t.free();
    }

}
