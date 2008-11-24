/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.DataPageBinary;

/**
 * A leaf page that contains data of one or multiple rows.
 * Format:
 * <ul><li>0-3: parent page id (0 for root)
 * </li><li>4-4: page type
 * </li><li>5-6: entry count
 * </li><li>only if there is overflow: 7-10: overflow page id
 * </li><li>list of key / offset pairs (4 bytes key, 2 bytes offset)
 * </li></ul>
 * The format of an overflow page is:
 * <ul><li>0-3: parent page id (0 for root)
 * </li><li>4-4: page type
 * </li><li>only if there is overflow: 5-8: next overflow page id
 * </li><li>data
 * </li></ul>
 */
class PageDataLeaf extends PageData {

    /**
     * The row offsets.
     */
    int[] offsets;
    
    /**
     * The rows.
     */
    Row[] rows;
    
    /**
     * The page id of the first overflow page (0 for no overflow).
     */
    int overflowPageId;
    
    /**
     * The start of the data area.
     */
    int start;
    
    PageDataLeaf(PageScanIndex index, int pageId, int parentPageId, DataPageBinary data) {
        super(index, pageId, parentPageId, data);
    }

    void read() throws SQLException {
        data.setPos(4);
        int type = data.readByte();
        entryCount = data.readShortInt();
        offsets = new int[entryCount];
        keys = new int[entryCount];
        rows = new Row[entryCount];
        if (type == Page.TYPE_DATA_LEAF_WITH_OVERFLOW) {
            overflowPageId = data.readInt();
        }
        for (int i = 0; i < entryCount; i++) {
            keys[i] = data.readInt();
            offsets[i] = data.readShortInt();
        }
        start = data.length();
    }
    
    void write() throws SQLException {
        // make sure rows are read
        for (int i = 0; i < entryCount; i++) {
            getRow(i);
        }
        data.reset();
        data.writeInt(parentPageId);
        int type;
        if (overflowPageId == 0) {
            type = Page.TYPE_DATA_LEAF;
        } else {
            type = Page.TYPE_DATA_LEAF_WITH_OVERFLOW;
        }
        data.writeByte((byte) type);
        data.writeShortInt(entryCount);
        if (overflowPageId != 0) {
            data.writeInt(overflowPageId);
        }
        for (int i = 0; i < entryCount; i++) {
            data.writeInt(keys[i]);
            data.writeShortInt(offsets[i]);
        }
        for (int i = 0; i < entryCount; i++) {
            data.setPos(offsets[i]);
            rows[i].write(data);
        }
        int pageSize = index.getPageStore().getPageSize();
        if (data.length() > pageSize) {
            if (overflowPageId == 0) {
                throw Message.getInternalError();
            }
            int todoWriteOverflow;
        } else {
            if (overflowPageId != 0) {
                throw Message.getInternalError();
            }
        }
        index.getPageStore().writePage(pageId, data);
    }

    /**
     * Add a row if possible. If it is possible this method returns 0, otherwise
     * the split point. It is always possible to add one row.
     * 
     * @return the split point of this page, or 0 if no split is required
     */
    int addRow(Row row) throws SQLException {
        int rowLength = row.getByteCount(data);
        int last = entryCount == 0 ? index.getPageStore().getPageSize() : offsets[entryCount - 1];
        int offset = last - rowLength;
        int[] newOffsets = new int[entryCount + 1];
        int[] newKeys = new int[entryCount + 1];
        Row[] newRows = new Row[entryCount + 1];
        int x;
        if (entryCount == 0) {
            x = 0;
        } else {
            x = find(row.getPos());
            System.arraycopy(offsets, 0, newOffsets, 0, x);
            System.arraycopy(keys, 0, newKeys, 0, x);
            System.arraycopy(rows, 0, newRows, 0, x);
            if (x < entryCount) {
                System.arraycopy(offsets, x, newOffsets, x + 1, entryCount - x);
                System.arraycopy(keys, x, newKeys, x + 1, entryCount - x);
                System.arraycopy(rows, x, newRows, x + 1, entryCount - x);
            }
        }
        entryCount++;
        start += 6;
        newOffsets[x] = offset;
        newKeys[x] = row.getPos();
        newRows[x] = row;
        offsets = newOffsets;
        keys = newKeys;
        rows = newRows;
        if (offset < start) {
            if (entryCount > 0) {
                int todoSplitAtLastInsertionPoint;
                return entryCount / 2;
            }
            offset = start + 6;
            overflowPageId = index.getPageStore().allocatePage();
            int todoWriteOverflow;
        }
        write();
        return 0;
    }
    
    private void removeRow(int i) throws SQLException {
        entryCount--;
        if (entryCount <= 0) {
            Message.getInternalError();
        }
        int[] newOffsets = new int[entryCount];
        int[] newKeys = new int[entryCount];
        Row[] newRows = new Row[entryCount];
        System.arraycopy(offsets, 0, newOffsets, 0, i);
        System.arraycopy(keys, 0, newKeys, 0, i);
        System.arraycopy(rows, 0, newRows, 0, i);
        System.arraycopy(offsets, i + 1, newOffsets, i, entryCount - i);
        System.arraycopy(keys, i + 1, newKeys, i, entryCount - i);
        System.arraycopy(rows, i + 1, newRows, i, entryCount - i);
        start -= 6;
        offsets = newOffsets;
        keys = newKeys;
        rows = newRows;
    }

    Cursor find() {
        return new PageScanCursor(this, 0);
    }
    
    /**
     * Get the row at the given index.
     * 
     * @param index the index
     * @return the row
     */
    Row getRow(int index) throws SQLException {
        Row r = rows[index];
        if (r == null) {
            data.setPos(offsets[index]);
            r = this.index.readRow(data);
            r.setPos(keys[index]);
            rows[index] = r;
        }
        return r;
    }

    int getEntryCount() {
        return entryCount;
    }
    
    PageData split(int splitPoint) throws SQLException {
        int newPageId = index.getPageStore().allocatePage();
        PageDataLeaf p2 = new PageDataLeaf(index, newPageId, parentPageId, index.getPageStore().createDataPage());
        for (int i = splitPoint; i < entryCount;) {
            p2.addRow(getRow(splitPoint));
            removeRow(splitPoint);
        }
        return p2;
    }

    int getLastKey() throws SQLException {
        int todoRemove;
        return getRow(entryCount - 1).getPos();
    }

    public PageDataLeaf getNextPage() throws SQLException {
        if (parentPageId == Page.ROOT) {
            return null;
        }
        PageDataNode next = (PageDataNode) index.getPage(parentPageId);
        return next.getNextPage(keys[entryCount - 1]);
    }

    PageDataLeaf getFirstLeaf() {
        return this;
    }

    boolean remove(int key) throws SQLException {
        int i = find(key);
        if (keys[i] != key) {
            throw Message.getSQLException(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, index.getSQL());
        }
        if (entryCount == 1) {
            return true;
        }
//                if (pageData.size() == 1 && !root) {
//                    // the last row has been deleted
//                    return oldRow;
//                }
//                pageData.remove(i);
//                updateRealByteCount(false, row);
//                index.updatePage(session, this);
//                if (i > 0) {
//                    // the first row didn't change
//                    return null;
//                }
//                if (pageData.size() == 0) {
//                    return null;
//                }
//                return getData(0);
//            }
//            if (comp > 0) {
//                r = i;
//            } else {
//                l = i + 1;
//            }
//        }
        throw Message.getSQLException(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, index.getSQL());
    }

}
