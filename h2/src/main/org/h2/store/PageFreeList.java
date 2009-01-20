/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.index.Page;
import org.h2.message.Message;
import org.h2.util.IntArray;

/**
 * The list of free pages of a page store.
 * The format of a free list trunk page is:
 * <ul><li>0-3: parent page id (always 0)
 * </li><li>4-4: page type
 * </li><li>5-8: the next page (if there are more) or number of entries
 * </li><li>9-remainder: data (4 bytes each entry)
 * </li></ul>
 */
public class PageFreeList extends Record {

    private final PageStore store;
    private final DataPage data;
    private final IntArray array = new IntArray();
    private int nextPage;

    PageFreeList(PageStore store, int pageId, int nextPage) {
        setPos(pageId);
        this.data = store.createDataPage();
        this.store = store;
        this.nextPage = nextPage;
    }

    /**
     * Allocate a page from the free list.
     *
     * @return the page
     */
    int allocate() throws SQLException {
        store.updateRecord(this, data);
        int size = array.size();
        if (size > 0) {
            int x = array.get(size - 1);
            array.remove(size - 1);
            return x;
        }
        store.removeRecord(getPos());
        // no more free pages in this list:
        // set the next page (may be 0, meaning no free pages)
        store.setFreeListRootPage(nextPage, true, 0);
        // and then return the page itself
        return getPos();
    }

    private int getMaxSize() {
        return (store.getPageSize() - 9) / DataPage.LENGTH_INT;
    }

    /**
     * Read the page from the disk.
     */
    void read() throws SQLException {
        data.reset();
        store.readPage(getPos(), data);
        int p = data.readInt();
        int t = data.readByte();
        boolean last = (t & Page.FLAG_LAST) != 0;
        t &= ~Page.FLAG_LAST;
        if (t != Page.TYPE_FREE_LIST || p != 0) {
            throw Message.getSQLException(
                    ErrorCode.FILE_CORRUPTED_1,
                    "pos:" + getPos() + " type:" + t + " parent:" + p +
                    " expected type:" + Page.TYPE_FREE_LIST);
        }
        int size;
        if (last) {
            nextPage = 0;
            size = data.readInt();
        } else {
            nextPage = data.readInt();
            size = getMaxSize();
        }
        for (int i = 0; i < size; i++) {
            array.add(data.readInt());
        }
    }

    /**
     * Add a page to the free list.
     *
     * @param pageId the page id to add
     */
    void free(int pageId) throws SQLException {
        store.updateRecord(this, data);
        if (array.size() < getMaxSize()) {
            array.add(pageId);
        } else {
            // this page is full:
            // the freed page is the next list
            this.nextPage = pageId;
            // set the next page
            store.setFreeListRootPage(pageId, false, getPos());
        }
    }

    public int getByteCount(DataPage dummy) throws SQLException {
        return store.getPageSize();
    }

    public void write(DataPage buff) throws SQLException {
        data.reset();
        data.writeInt(0);
        int type = Page.TYPE_FREE_LIST;
        if (nextPage == 0) {
            type |= Page.FLAG_LAST;
        }
        data.writeByte((byte) type);
        if (nextPage != 0) {
            data.writeInt(nextPage);
        } else {
            data.writeInt(array.size());
        }
        for (int i = 0; i < array.size(); i++) {
            data.writeInt(array.get(i));
        }
        store.writePage(getPos(), data);
    }

}
