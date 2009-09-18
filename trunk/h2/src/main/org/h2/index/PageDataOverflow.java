/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.store.Data;
import org.h2.store.DataPage;
import org.h2.store.Page;
import org.h2.store.PageStore;

/**
 * Overflow data for a leaf page. Format:
 * <ul>
 * <li>page type: byte (0)</li>
 * <li>parent page id (0 for root): int (1-4)</li>
 * <li>more data: next overflow page id: int (5-8)</li>
 * <li>last remaining size: short (5-6)</li>
 * <li>data (9-/7-)</li>
 * </ul>
 */
public class PageDataOverflow extends Page {

    /**
     * The start of the data in the last overflow page.
     */
    static final int START_LAST = 7;

    /**
     * The start of the data in a overflow page that is not the last one.
     */
    static final int START_MORE = 9;

    private static final int START_NEXT_OVERFLOW = 5;

    /**
     * The page store.
     */
    private final PageStore store;

    /**
     * The page type.
     */
    private int type;

    /**
     * The parent page (overflow or leaf).
     */
    private int parentPageId;

    /**
     * The next overflow page, or 0.
     */
    private int nextPage;

    private Data data;

    private int start;
    private int size;

    /**
     * Create an object from the given data page.
     *
     * @param leaf the leaf page
     * @param pageId the page id
     * @param data the data page
     * @param offset the offset
     */
    private PageDataOverflow(PageStore store, int pageId, Data data) {
        this.store = store;
        setPos(pageId);
        this.data = data;
    }

    /**
     * Read an overflow page.
     *
     * @param store the page store
     * @param data the data
     * @param pageId the page id
     * @return the page
     */
    public static Page read(PageStore store, Data data, int pageId) throws SQLException {
        PageDataOverflow p = new PageDataOverflow(store, pageId, data);
        p.read();
        return p;
    }

    /**
     * Create a new overflow page.
     *
     * @param store the page store
     * @param page the page id
     * @param type the page type
     * @param parentPageId the parent page id
     * @param next the next page or 0
     * @param all the data
     * @param offset the offset within the data
     * @param size the number of bytes
     * @return the page
     */
    static PageDataOverflow create(PageStore store, int page, int type, int parentPageId, int next, Data all, int offset, int size) {
        Data data = store.createData();
        PageDataOverflow p = new PageDataOverflow(store, page, data);
        data.writeByte((byte) type);
        data.writeInt(parentPageId);
        if (type == Page.TYPE_DATA_OVERFLOW) {
            data.writeInt(next);
        } else {
            data.writeShortInt(size);
        }
        p.start = data.length();
        data.write(all.getBytes(), offset, size);
        p.type = type;
        p.parentPageId = parentPageId;
        p.nextPage = next;
        p.size = size;
        return p;
    }

    /**
     * Read the page.
     */
    private void read() throws SQLException {
        data.reset();
        type = data.readByte();
        parentPageId = data.readInt();
        if (type == (Page.TYPE_DATA_OVERFLOW | Page.FLAG_LAST)) {
            size = data.readShortInt();
            nextPage = 0;
        } else if (type == Page.TYPE_DATA_OVERFLOW) {
            nextPage = data.readInt();
            size = store.getPageSize() - data.length();
        } else {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, "page:" + getPos() + " type:" + type);
        }
        start = data.length();
    }

    /**
     * Read the data into a target buffer.
     *
     * @param target the target data page
     * @return the next page, or 0 if no next page
     */
    int readInto(Data target) {
        target.checkCapacity(size);
        if (type == (Page.TYPE_DATA_OVERFLOW | Page.FLAG_LAST)) {
            target.write(data.getBytes(), START_LAST, size);
            return 0;
        }
        target.write(data.getBytes(), START_MORE, size);
        return nextPage;
    }

    int getNextOverflow() {
        return nextPage;
    }

    public int getByteCount(DataPage dummy) {
        return store.getPageSize();
    }

    private void writeHead() {
        data.writeByte((byte) type);
        data.writeInt(parentPageId);
    }

    public void write(DataPage buff) throws SQLException {
        write();
        store.writePage(getPos(), data);
    }


    private void write() {
        data.reset();
        writeHead();
        if (type == Page.TYPE_DATA_OVERFLOW) {
            data.writeInt(nextPage);
        } else {
            data.writeShortInt(size);
        }
    }


    public String toString() {
        return "page[" + getPos() + "] data leaf overflow parent:" + parentPageId + " next:" + nextPage;
    }

    /**
     * Get the estimated memory size.
     *
     * @return number of double words (4 bytes)
     */
    public int getMemorySize() {
        // double the byte array size
        return store.getPageSize() >> 1;
    }

    void setParentPageId(int parent) {
        this.parentPageId = parent;
    }

    public void moveTo(Session session, int newPos) throws SQLException {
        PageDataOverflow p2 = PageDataOverflow.create(store, newPos, type, parentPageId, nextPage, data, start, size);
        store.updateRecord(p2, false, null);
        if (nextPage != 0) {
            PageDataOverflow p3 = (PageDataOverflow) store.getPage(nextPage);
            p3.setParentPageId(newPos);
        }
        Page p = store.getPage(parentPageId);
        if (p == null) {
            throw Message.throwInternalError();
        }
        if (p instanceof PageDataOverflow) {
            PageDataOverflow p1 = (PageDataOverflow) p;
            p1.setNext(newPos);
        } else {
            PageDataLeaf p1 = (PageDataLeaf) p;
            p1.setOverflow(newPos);
        }
        store.freePage(getPos(), true, data);
    }

    private void setNext(int nextPage) throws SQLException {
        this.nextPage = nextPage;
        data.setInt(START_NEXT_OVERFLOW, nextPage);
        store.updateRecord(this, true, data);
    }

}
