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
import org.h2.result.SearchRow;
import org.h2.store.DataPage;
import org.h2.store.PageStore;

/**
 * A leaf page that contains index data.
 * Format:
 * <ul><li>0-3: parent page id (0 for root)
 * </li><li>4-4: page type
 * </li><li>5-8: table id
 * </li><li>9-10: entry count
 * </li><li>overflow: 11-14: the row key
 * </li><li>11-: list of key / offset pairs (4 bytes key, 2 bytes offset)
 * </li><li>data
 * </li></ul>
 */
class PageBtreeLeaf extends PageBtree {

    private static final int KEY_OFFSET_PAIR_LENGTH = 6;
    private static final int KEY_OFFSET_PAIR_START = 11;

    PageBtreeLeaf(PageBtreeIndex index, int pageId, int parentPageId, DataPage data) {
        super(index, pageId, parentPageId, data);
        start = KEY_OFFSET_PAIR_START;
    }

    void read() throws SQLException {
        data.setPos(4);
        data.readByte();
        int tableId = data.readInt();
        if (tableId != index.getId()) {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1,
                    "page:" + getPageId() + " expected table:" + index.getId() +
                    "got:" + tableId);
        }
        entryCount = data.readShortInt();
        offsets = new int[entryCount];
        rows = new SearchRow[entryCount];
        for (int i = 0; i < entryCount; i++) {
            offsets[i] = data.readShortInt();
        }
        start = data.length();
    }

    /**
     * Add a row if possible. If it is possible this method returns 0, otherwise
     * the split point. It is always possible to add one row.
     *
     * @param row the now to add
     * @return the split point of this page, or 0 if no split is required
     */
    int addRow(SearchRow row) throws SQLException {
        int rowLength = index.getRowSize(data, row);
        int pageSize = index.getPageStore().getPageSize();
        int last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
        if (entryCount > 0 && last - rowLength < start + KEY_OFFSET_PAIR_LENGTH) {
            int todoSplitAtLastInsertionPoint;
            return (entryCount / 2) + 1;
        }
        written = false;
        int offset = last - rowLength;
        int[] newOffsets = new int[entryCount + 1];
        SearchRow[] newRows = new SearchRow[entryCount + 1];
        int x;
        if (entryCount == 0) {
            x = 0;
        } else {
            x = find(row, false, true);
            System.arraycopy(offsets, 0, newOffsets, 0, x);
            System.arraycopy(rows, 0, newRows, 0, x);
            if (x < entryCount) {
                for (int j = x; j < entryCount; j++) {
                    newOffsets[j + 1] = offsets[j] - rowLength;
                }
                offset = (x == 0 ? pageSize : offsets[x - 1]) - rowLength;
                System.arraycopy(rows, x, newRows, x + 1, entryCount - x);
            }
        }
        entryCount++;
        start += KEY_OFFSET_PAIR_LENGTH;
        newOffsets[x] = offset;
        newRows[x] = row;
        offsets = newOffsets;
        rows = newRows;
        index.getPageStore().updateRecord(this, true, data);
        if (offset < start) {
            if (entryCount > 1) {
                Message.throwInternalError();
            }
            // need to write the overflow page id
            start += 4;
            int remaining = rowLength - (pageSize - start);
            // fix offset
            offset = start;
            offsets[x] = offset;
        }
        return 0;
    }

    private void removeRow(int i) {
        entryCount--;
        written = false;
        if (entryCount <= 0) {
            Message.throwInternalError();
        }
        int[] newOffsets = new int[entryCount];
        SearchRow[] newRows = new SearchRow[entryCount];
        System.arraycopy(offsets, 0, newOffsets, 0, i);
        System.arraycopy(rows, 0, newRows, 0, i);

        int startNext = i > 0 ? offsets[i - 1] : index.getPageStore().getPageSize();
        int rowLength = startNext - offsets[i];
        for (int j = i; j < entryCount; j++) {
            newOffsets[j] = offsets[j + 1] + rowLength;
        }
        System.arraycopy(rows, i + 1, newRows, i, entryCount - i);
        start -= KEY_OFFSET_PAIR_LENGTH;
        offsets = newOffsets;
        rows = newRows;
    }

    int getEntryCount() {
        return entryCount;
    }

    PageBtree split(int splitPoint) throws SQLException {
        int newPageId = index.getPageStore().allocatePage();
        PageBtreeLeaf p2 = new PageBtreeLeaf(index, newPageId, parentPageId, index.getPageStore().createDataPage());
        for (int i = splitPoint; i < entryCount;) {
            p2.addRow(getRow(splitPoint));
            removeRow(splitPoint);
        }
        return p2;
    }

    PageBtreeLeaf getFirstLeaf() {
        return this;
    }

    boolean remove(SearchRow row) throws SQLException {
        int at = find(row, false, false);
        if (index.compareRows(row, getRow(at)) != 0) {
            throw Message.getSQLException(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, index.getSQL() + ": " + row);
        }
        if (entryCount == 1) {
            return true;
        }
        removeRow(at);
        index.getPageStore().updateRecord(this, true, data);
        return false;
    }

    int getRowCount() {
        return entryCount;
    }

    void setRowCountStored(int rowCount) {
        // ignore
    }

    public int getByteCount(DataPage dummy) {
        return index.getPageStore().getPageSize();
    }

    public void write(DataPage buff) throws SQLException {
        write();
        index.getPageStore().writePage(getPos(), data);
    }

    PageStore getPageStore() {
        return index.getPageStore();
    }

    private void write() throws SQLException {
        if (written) {
            return;
        }
        // make sure rows are read
        for (int i = 0; i < entryCount; i++) {
            getRow(i);
        }
        data.reset();
        data.writeInt(parentPageId);
        data.writeByte((byte) Page.TYPE_BTREE_LEAF);
        data.writeInt(index.getId());
        data.writeShortInt(entryCount);
        for (int i = 0; i < entryCount; i++) {
            data.writeShortInt(offsets[i]);
        }
        for (int i = 0; i < entryCount; i++) {
            index.writeRow(data, offsets[i], rows[i]);
        }
        written = true;
    }

    DataPage getDataPage() throws SQLException {
        write();
        return data;
    }

    void find(PageBtreeCursor cursor, SearchRow first, boolean bigger) throws SQLException {
        int i = find(first, bigger, false);
        if (i > entryCount) {
            if (parentPageId == Page.ROOT) {
                return;
            }
            PageBtreeNode next = (PageBtreeNode) index.getPage(parentPageId);
            next.find(cursor, first, bigger);
            return;
        }
        cursor.setCurrent(this, i);
    }

    void remapChildren() {
    }

    /**
     * Set the cursor to the first row of the next page.
     *
     * @param cursor the cursor
     */
    void nextPage(PageBtreeCursor cursor) throws SQLException {
        if (parentPageId == Page.ROOT) {
            cursor.setCurrent(null, 0);
            return;
        }
        PageBtreeNode next = (PageBtreeNode) index.getPage(parentPageId);
        next.nextPage(cursor, getRow(entryCount - 1));
    }

    public String toString() {
        return "page[" + getPos() + "] btree leaf table:" + index.getId() + " entries:" + entryCount;
    }

}
