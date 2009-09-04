/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.util.BitField;

/**
 * The list of free pages of a page store. The format of a free list trunk page
 * is:
 * <ul>
 * <li>0-3: parent page id (always 0)</li>
 * <li>4-4: page type</li>
 * <li>5-remainder: data</li>
 * </ul>
 */
public class PageFreeList extends Page {

    private static final int DATA_START = 5;

    private final PageStore store;
    private final BitField used = new BitField();
    private final int pageCount;
    private boolean full;
    private Data data;

    private PageFreeList(PageStore store, int pageId) {
        setPos(pageId);
        this.store = store;
        pageCount = (store.getPageSize() - DATA_START) * 8;
        used.set(0);
    }

    /**
     * Read a free-list page.
     *
     * @param store the page store
     * @param data the data
     * @param pageId the page id
     * @return the page
     */
    static PageFreeList read(PageStore store, Data data, int pageId) throws SQLException {
        PageFreeList p = new PageFreeList(store, pageId);
        p.data = data;
        p.read();
        return p;
    }

    /**
     * Create a new free-list page.
     *
     * @param store the page store
     * @param pageId the page id
     * @return the page
     */
    static PageFreeList create(PageStore store, int pageId) {
        return new PageFreeList(store, pageId);
    }

    /**
     * Allocate a page from the free list.
     *
     * @param exclude the exclude list or null
     * @param first the first page to look for
     * @return the page, or -1 if all pages are used
     */
    int allocate(BitField exclude, int first) throws SQLException {
        if (full) {
            return -1;
        }
        // TODO cache last result
        int start = Math.max(0, first - getPos());
        while (true) {
            int free = used.nextClearBit(start);
            if (free >= pageCount) {
                full = true;
                return -1;
            }
            if (exclude != null && exclude.get(free + getPos())) {
                start = exclude.nextClearBit(free + getPos()) - getPos();
                if (start >= pageCount) {
                    return -1;
                }
            } else {
                used.set(free);
                store.updateRecord(this, true, data);
                return free + getPos();
            }
        }
    }

    int getFirstFree() {
        if (full) {
            return -1;
        }
        int free = used.nextClearBit(0);
        if (free >= pageCount) {
            return -1;
        }
        return free + getPos();
    }

    int getLastUsed() {
        int last = used.getLastSetBit();
        return last == -1 ? -1 : last + getPos();
    }

    /**
     * Mark a page as used.
     *
     * @param pageId the page id
     * @return the page id, or -1
     */
    int allocate(int pageId) throws SQLException {
        int idx = pageId - getPos();
        if (idx >= 0 && !used.get(idx)) {
            used.set(idx);
            store.updateRecord(this, true, data);
        }
        return pageId;
    }

    /**
     * Add a page to the free list.
     *
     * @param pageId the page id to add
     */
    void free(int pageId) throws SQLException {
        full = false;
        used.clear(pageId - getPos());
        store.updateRecord(this, true, data);
    }

    /**
     * Read the page from the disk.
     */
    private void read() throws SQLException {
        data.reset();
        int p = data.readInt();
        int t = data.readByte();
        if (p != 0) {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, "pos:" + getPos() + " type:" + t + " parent:" + p
                    + " expected type:" + Page.TYPE_FREE_LIST);
        }
        for (int i = 0; i < pageCount; i += 8) {
            used.setByte(i, data.readByte() & 255);
        }
        full = false;
    }

    public int getByteCount(DataPage dummy) {
        return store.getPageSize();
    }

    public void write(DataPage buff) throws SQLException {
        data = store.createData();
        data.writeInt(0);
        int type = Page.TYPE_FREE_LIST;
        data.writeByte((byte) type);
        for (int i = 0; i < pageCount; i += 8) {
            data.writeByte((byte) used.getByte(i));
        }
        store.writePage(getPos(), data);
    }

    /**
     * Get the number of pages that can fit in a free list.
     *
     * @param pageSize the page size
     * @return the number of pages
     */
    public static int getPagesAddressed(int pageSize) {
        return (pageSize - DATA_START) * 8;
    }

    /**
     * Get the estimated memory size.
     *
     * @return number of double words (4 bytes)
     */
    public int getMemorySize() {
        return store.getPageSize() >> 2;
    }

    /**
     * Check if a page is already in use.
     *
     * @param pageId the page to check
     * @return true if it is in use
     */
    boolean isUsed(int pageId) {
        return used.get(pageId - getPos());
    }

    public void moveTo(Session session, int newPos) throws SQLException {
        // the old data does not need to be copied, as free-list pages
        // at the end of the file are not required
        store.freePage(getPos(), true, data);
    }

}
