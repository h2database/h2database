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
 * Overflow data for a leaf page.
 * Format:
 * <ul><li>0-3: parent page id (0 for root)
 * </li><li>4-4: page type
 * </li><li>5-8: index id
 * </li><li>if there is more data: 9-12: next overflow page id
 * </li><li>otherwise: 9-10: remaining size
 * </li><li>data
 * </li></ul>
 */
public class PageDataOverflow extends Page {

    /**
     * The start of the data in the last overflow page.
     */
    static final int START_LAST = 11;

    /**
     * The start of the data in a overflow page that is not the last one.
     */
    static final int START_MORE = 13;

    private static final int START_NEXT_OVERFLOW = 9;

    /**
     * The index.
     */
    private final PageScanIndex index;

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

    private Data data;

    PageDataOverflow(PageScanIndex index, int pageId, int type, int previous, int next, Data allData, int offset, int size) {
        this.index = index;
        setPos(pageId);
        this.type = type;
        this.parentPage = previous;
        this.nextPage = next;
        this.size = size;
        data = index.getPageStore().createData();
        data.writeInt(parentPage);
        data.writeByte((byte) type);
        data.writeInt(index.getId());
        if (type == Page.TYPE_DATA_OVERFLOW) {
            data.writeInt(nextPage);
        } else {
            data.writeShortInt(size);
        }
        data.write(allData.getBytes(), offset, size);
    }

    /**
     * Create an object from the given data page.
     *
     * @param leaf the leaf page
     * @param pageId the page id
     * @param data the data page
     * @param offset the offset
     */
    PageDataOverflow(PageScanIndex index, int pageId, Data data) {
        this.index = index;
        setPos(pageId);
        this.data = data;
    }

    /**
     * Read an overflow page.
     *
     * @param index the index
     * @param data the data
     * @param pageId the page id
     * @return the page
     */
    public static Page read(PageScanIndex index, Data data, int pageId) throws SQLException {
        PageDataOverflow p = new PageDataOverflow(index, pageId, data);
        p.read();
        return p;
    }

    /**
     * Read the page.
     */
    private void read() throws SQLException {
        data.reset();
        parentPage = data.readInt();
        type = data.readByte();
        int indexId = data.readInt();
        if (indexId != index.getId()) {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1,
                    "page:" + getPos() + " expected index:" + index.getId() +
                    " got:" + indexId + " type:" + type);
        }
        if (type == (Page.TYPE_DATA_OVERFLOW | Page.FLAG_LAST)) {
            size = data.readShortInt();
            nextPage = 0;
        } else if (type == Page.TYPE_DATA_OVERFLOW) {
            size = index.getPageStore().getPageSize() - START_MORE;
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
        return index.getPageStore().getPageSize();
    }

    public void write(DataPage buff) throws SQLException {
        index.getPageStore().writePage(getPos(), data);
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
        // double the byte array size
        return index.getPageStore().getPageSize() >> 1;
    }

    void setParentPageId(int parent) {
        this.parentPage = parent;
    }

    public void moveTo(Session session, int newPos) throws SQLException {
        PageStore store = index.getPageStore();
        int start =  type == Page.TYPE_DATA_OVERFLOW ? START_MORE : START_LAST;
        PageDataOverflow p2 = new PageDataOverflow(index, newPos, type, parentPage, nextPage, data, start, size);
        store.updateRecord(p2, false, null);
        if (nextPage != 0) {
            PageDataOverflow p3 = (PageDataOverflow) store.getPage(nextPage);
            p3.setParentPageId(newPos);
        }
        Page p = store.getPage(parentPage);
        if (p == null) {
            throw Message.throwInternalError();
        }
        if (p instanceof PageDataOverflow) {
            PageDataOverflow p1 = (PageDataOverflow) p;
            p1.setOverflow(newPos);
        } else {
            PageDataLeaf p1 = (PageDataLeaf) p;
            p1.setOverflow(newPos);
        }
        store.freePage(getPos(), true, data);
    }

    private void setOverflow(int nextPage) throws SQLException {
        this.nextPage = nextPage;
        data.setInt(START_NEXT_OVERFLOW, nextPage);
        index.getPageStore().updateRecord(this, true, data);
    }

}
