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
    private final int firstAddressed;
    private final int pageCount;
    private final int nextPage;
    private boolean full;
    private DataPage data;

    PageFreeList(PageStore store, int pageId, int firstAddressed) {
        setPos(pageId);
        this.store = store;
        this.firstAddressed = firstAddressed;
        pageCount = (store.getPageSize() - DATA_START) * 8;
        for (int i = firstAddressed; i <= pageId; i++) {
            used.set(getAddress(i));
        }
        nextPage = firstAddressed + pageCount;
    }

    private int getAddress(int pageId) {
        return pageId - firstAddressed;
    }

    /**
     * Allocate a page from the free list.
     *
     * @return the page, or -1 if all pages are used
     */
    int allocate() throws SQLException {
        if (full) {
            PageFreeList next = getNext();
            if (next == null) {
                return -1;
            }
            return next.allocate();
        }
        int free = used.nextClearBit(0);
        if (free > pageCount) {
            full = true;
            return allocate();
        }
        used.set(free);
        store.updateRecord(this, true, data);
        return free + firstAddressed;
    }

    /**
     * Allocate a page at the end of the file
     *
     * @param min the minimum page number
     * @return the page id
     */
    int allocateAtEnd(int min) throws SQLException {
        int pos = Math.max(min, getLastUsed() + 1);
        return allocate(pos);
    }

    public int getLastUsed() throws SQLException {
        if (nextPage < store.getPageCount()) {
            PageFreeList next = getNext();
            return next.getLastUsed();
        }
        return used.getLastSetBit() + firstAddressed;
    }

    private PageFreeList getNext() throws SQLException {
        PageFreeList next = (PageFreeList) store.getRecord(nextPage);
        if (next == null) {
            if (nextPage < store.getPageCount()) {
                next = new PageFreeList(store, nextPage, nextPage);
                next.read();
                store.updateRecord(next, false, null);
            }
        }
        return next;
    }

    /**
     * Mark a page as used.
     *
     * @param pos the page id
     * @return the page id, or -1
     */
    int allocate(int pos) throws SQLException {
        if (pos - firstAddressed > pageCount) {
            PageFreeList next = getNext();
            if (next == null) {
                return -1;
            }
            return next.allocate(pos);
        } else {
            int idx = pos - firstAddressed;
            if (idx >= 0 && !used.get(idx)) {
                used.set(pos - firstAddressed);
                store.updateRecord(this, true, data);
            }
            return pos;
        }
    }

    /**
     * Add a page to the free list.
     *
     * @param pageId the page id to add
     */
    void free(int pageId) throws SQLException {
        full = false;
        used.clear(pageId - firstAddressed);
        store.updateRecord(this, true, data);
    }

    /**
     * Read the page from the disk.
     */
    void read() throws SQLException {
        data = store.createDataPage();
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
            used.setByte(i, data.readByte());
        }
    }

    public int getByteCount(DataPage dummy) throws SQLException {
        return store.getPageSize();
    }

    public void write(DataPage buff) throws SQLException {
        data = store.createDataPage();
        data.writeInt(0);
        int type = Page.TYPE_FREE_LIST;
        data.writeByte((byte) type);
        for (int i = 0; i < pageCount; i += 8) {
            data.writeByte((byte) used.getByte(i));
        }
        store.writePage(getPos(), data);
    }

}
