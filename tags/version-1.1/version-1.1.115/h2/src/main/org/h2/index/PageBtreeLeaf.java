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
 * A b-tree leaf page that contains index data.
 * Format:
 * <ul><li>0-3: parent page id (0 for root)
 * </li><li>4-4: page type
 * </li><li>5-8: table id
 * </li><li>9-10: entry count
 * </li><li>11-: list of key / offset pairs (4 bytes key, 2 bytes offset)
 * </li><li>data
 * </li></ul>
 */
class PageBtreeLeaf extends PageBtree {

    private static final int OFFSET_LENGTH = 2;
    private static final int OFFSET_START = 11;

    PageBtreeLeaf(PageBtreeIndex index, int pageId, int parentPageId, DataPage data) {
        super(index, pageId, parentPageId, data);
        start = OFFSET_START;
    }

    void read() throws SQLException {
        data.setPos(4);
        int type = data.readByte();
        onlyPosition = (type & Page.FLAG_LAST) == 0;
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
    int addRowTry(SearchRow row) throws SQLException {
        int rowLength = index.getRowSize(data, row, onlyPosition);
        int pageSize = index.getPageStore().getPageSize();
        int last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
        if (last - rowLength < start + OFFSET_LENGTH) {
            if (entryCount > 1) {
                int todoSplitAtLastInsertionPoint;
                return (entryCount / 2) + 1;
            }
            onlyPosition = true;
            rowLength = index.getRowSize(data, row, onlyPosition);
        }
        written = false;
        int offset = last - rowLength;
        int[] newOffsets = new int[entryCount + 1];
        SearchRow[] newRows = new SearchRow[entryCount + 1];
        int x;
        if (entryCount == 0) {
            x = 0;
        } else {
            readAllRows();
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
        start += OFFSET_LENGTH;
        newOffsets[x] = offset;
        newRows[x] = row;
        offsets = newOffsets;
        rows = newRows;
        index.getPageStore().updateRecord(this, true, data);
        return 0;
    }

    private void removeRow(int i) throws SQLException {
        readAllRows();
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
        start -= OFFSET_LENGTH;
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
            p2.addRowTry(getRow(splitPoint));
            removeRow(splitPoint);
        }
        return p2;
    }

    PageBtreeLeaf getFirstLeaf() {
        return this;
    }

    PageBtreeLeaf getLastLeaf() {
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
        readAllRows();
        data.reset();
        data.writeInt(parentPageId);
        data.writeByte((byte) (Page.TYPE_BTREE_LEAF | (onlyPosition ? 0 : Page.FLAG_LAST)));
        data.writeInt(index.getId());
        data.writeShortInt(entryCount);
        for (int i = 0; i < entryCount; i++) {
            data.writeShortInt(offsets[i]);
        }
        for (int i = 0; i < entryCount; i++) {
            index.writeRow(data, offsets[i], rows[i], onlyPosition);
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

    void last(PageBtreeCursor cursor) {
        cursor.setCurrent(this, entryCount - 1);
    }

    void remapChildren() {
        // nothing to do
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
        next.nextPage(cursor, getPos());
    }

    /**
     * Set the cursor to the last row of the previous page.
     *
     * @param cursor the cursor
     */
    void previousPage(PageBtreeCursor cursor) throws SQLException {
        if (parentPageId == Page.ROOT) {
            cursor.setCurrent(null, 0);
            return;
        }
        PageBtreeNode next = (PageBtreeNode) index.getPage(parentPageId);
        next.previousPage(cursor, getPos());
    }

    public String toString() {
        return "page[" + getPos() + "] btree leaf table:" + index.getId() + " entries:" + entryCount;
    }

}
