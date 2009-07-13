/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.index.Page;
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
public class PageFreeList extends Record {

    private static final int DATA_START = 5;

    private final PageStore store;
    private final BitField used = new BitField();
    private final int pageCount;
    private boolean full;
    private Data data;

    PageFreeList(PageStore store, int pageId) {
        setPos(pageId);
        this.store = store;
        pageCount = (store.getPageSize() - DATA_START) * 8;
        used.set(0);
    }

    /**
     * Allocate a page from the free list.
     *
     * @return the page, or -1 if all pages are used
     */
    int allocate() throws SQLException {
        if (full) {
            return -1;
        }
        // TODO cache last result
        int free = used.nextClearBit(0);
        if (free >= pageCount) {
            full = true;
            return -1;
        }
        used.set(free);
        store.updateRecord(this, true, data);
        return free + getPos();
    }

    int getLastUsed() {
        return used.getLastSetBit() + getPos();
    }

    /**
     * Mark a page as used.
     *
     * @param pos the page id
     * @return the page id, or -1
     */
    int allocate(int pos) throws SQLException {
        int idx = pos - getPos();
        if (idx >= 0 && !used.get(idx)) {
            used.set(idx);
            store.updateRecord(this, true, data);
        }
        return pos;
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
    void read() throws SQLException {
        data = store.createData();
        store.readPage(getPos(), data);
        int p = data.readInt();
        int t = data.readByte();
        if (t == Page.TYPE_EMPTY) {
            return;
        }
        if (t != Page.TYPE_FREE_LIST || p != 0) {
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

}
