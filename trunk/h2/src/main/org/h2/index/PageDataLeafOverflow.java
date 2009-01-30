/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.jdbc.JdbcSQLException;
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
    private final int type;

    /**
     * The previous page (overflow or leaf).
     */
    private final int previous;

    /**
     * The next overflow page, or 0.
     */
    private final int next;

    /**
     * The number of content bytes.
     */
    private final int size;

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
        this.previous = previous;
        this.next = next;
        this.offset = offset;
        this.size = size;
    }

    public PageDataLeafOverflow(PageDataLeaf leaf, int pageId, DataPage data, int offset) throws JdbcSQLException {
        this.leaf = leaf;
        setPos(pageId);
        this.data = data;
        this.offset = offset;
        previous = data.readInt();
        type = data.readByte();
        if (type == (Page.TYPE_DATA_OVERFLOW | Page.FLAG_LAST)) {
            size = data.readShortInt();
            next = 0;
        } else if (type == Page.TYPE_DATA_OVERFLOW) {
            size = leaf.getPageStore().getPageSize() - START_MORE;
            next = data.readInt();
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
        } else {
            target.write(data.getBytes(), START_MORE, size);
            return next;
        }
    }

    public int getByteCount(DataPage dummy) throws SQLException {
        return leaf.getByteCount(dummy);
    }

    public void write(DataPage buff) throws SQLException {
        PageStore store = leaf.getPageStore();
        DataPage overflow = store.createDataPage();
        DataPage data = leaf.getDataPage();
        overflow.writeInt(previous);
        overflow.writeByte((byte) type);
        if (type == Page.TYPE_DATA_OVERFLOW) {
            overflow.writeInt(next);
        } else {
            overflow.writeShortInt(size);
        }
        overflow.write(data.getBytes(), offset, size);
        store.writePage(getPos(), overflow);
    }

}
