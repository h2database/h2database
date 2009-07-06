/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.message.Message;
import org.h2.store.DataPage;
import org.h2.store.PageStore;
import org.h2.store.Record;

/**
 * Overflow data for a leaf page.
 * Format:
 * <ul><li>0-3: parent page id (0 for root)
 * </li><li>4-4: page type
 * </li><li>if there is more data: 5-8: next overflow page id
 * </li><li>otherwise: 5-6: remaining size
 * </li><li>data
 * </li></ul>
 */
public class PageDataLeafOverflow extends Record {

    /**
     * The start of the data in the last overflow page.
     */
    static final int START_LAST = 7;

    /**
     * The start of the data in a overflow page that is not the last one.
     */
    static final int START_MORE = 9;

    private final PageDataLeaf leaf;

    /**
     * The page type.
     */
    private int type;

    /**
     * The parent page (overflow or leaf).
     */
    private int parentPage;

    /**
     * The next overflow page, or 0.
     */
    private int nextPage;

    /**
     * The number of content bytes.
     */
    private int size;

    /**
     * The first content byte starts at the given position
     * in the leaf page when the page size is unlimited.
     */
    private final int offset;

    private DataPage data;

    PageDataLeafOverflow(PageDataLeaf leaf, int pageId, int type, int previous, int next, int offset, int size) {
        this.leaf = leaf;
        setPos(pageId);
        this.type = type;
        this.parentPage = previous;
        this.nextPage = next;
        this.offset = offset;
        this.size = size;
    }

    /**
     * Create an object from the given data page.
     *
     * @param leaf the leaf page
     * @param pageId the page id
     * @param data the data page
     * @param offset the offset
     */
    public PageDataLeafOverflow(PageDataLeaf leaf, int pageId, DataPage data, int offset) {
        this.leaf = leaf;
        setPos(pageId);
        this.data = data;
        this.offset = offset;
    }

    /**
     * Read the page.
     */
    void read() throws SQLException {
        parentPage = data.readInt();
        type = data.readByte();
        if (type == (Page.TYPE_DATA_OVERFLOW | Page.FLAG_LAST)) {
            size = data.readShortInt();
            nextPage = 0;
        } else if (type == Page.TYPE_DATA_OVERFLOW) {
            size = leaf.getPageStore().getPageSize() - START_MORE;
            nextPage = data.readInt();
        } else {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, "page:" + getPos() + " type:" + type);
        }
    }

    /**
     * Read the data into a target buffer.
     *
     * @param target the target data page
     * @return the next page, or 0 if no next page
     */
    int readInto(DataPage target) {
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
        return leaf.getByteCount(dummy);
    }

    public void write(DataPage buff) throws SQLException {
        PageStore store = leaf.getPageStore();
        DataPage overflow = store.createDataPage();
        DataPage data = leaf.getDataPage();
        overflow.writeInt(parentPage);
        overflow.writeByte((byte) type);
        if (type == Page.TYPE_DATA_OVERFLOW) {
            overflow.writeInt(nextPage);
        } else {
            overflow.writeShortInt(size);
        }
        overflow.write(data.getBytes(), offset, size);
        store.writePage(getPos(), overflow);
    }

    public String toString() {
        return "page[" + getPos() + "] data leaf overflow parent:" + parentPage + " next:" + nextPage;
    }

    /**
     * Get the estimated memory size.
     *
     * @return number of double words (4 bytes)
     */
    public int getMemorySize() {
        return leaf.getMemorySize();
    }

    int getParent() {
        return parentPage;
    }

    void setParent(int parent) {
        this.parentPage = parent;
    }

}
