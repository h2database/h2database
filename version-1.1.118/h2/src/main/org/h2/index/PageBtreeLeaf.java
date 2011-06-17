/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.store.DataPage;
import org.h2.store.Page;
import org.h2.store.PageStore;

/**
 * A b-tree leaf page that contains index data.
 * Format:
 * <ul><li>0-3: parent page id (0 for root)
 * </li><li>4-4: page type
 * </li><li>5-8: index id
 * </li><li>9-10: entry count
 * </li><li>11-: list of key / offset pairs (4 bytes key, 2 bytes offset)
 * </li><li>data
 * </li></ul>
 */
public class PageBtreeLeaf extends PageBtree {

    private static final int OFFSET_LENGTH = 2;
    private static final int OFFSET_START = 11;

    PageBtreeLeaf(PageBtreeIndex index, int pageId, Data data) {
        super(index, pageId, data);
        start = OFFSET_START;
    }

    /**
     * Read a b-tree leaf page.
     *
     * @param index the index
     * @param data the data
     * @param pageId the page id
     * @return the page
     */
    public static Page read(PageBtreeIndex index, Data data, int pageId) throws SQLException {
        PageBtreeLeaf p = new PageBtreeLeaf(index, pageId, data);
        p.read();
        return p;
    }

    private void read() throws SQLException {
        data.reset();
        this.parentPageId = data.readInt();
        int type = data.readByte();
        onlyPosition = (type & Page.FLAG_LAST) == 0;
        int indexId = data.readInt();
        if (indexId != index.getId()) {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1,
                    "page:" + getPos() + " expected index:" + index.getId() +
                    "got:" + indexId);
        }
        entryCount = data.readShortInt();
        offsets = new int[entryCount];
        rows = new SearchRow[entryCount];
        for (int i = 0; i < entryCount; i++) {
            offsets[i] = data.readShortInt();
        }
        start = data.length();
    }

    int addRowTry(SearchRow row) throws SQLException {
        int rowLength = index.getRowSize(data, row, onlyPosition);
        int pageSize = index.getPageStore().getPageSize();
        int last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
        if (last - rowLength < start + OFFSET_LENGTH) {
            if (entryCount > 1) {
                // split at the insertion point to better fill pages
                // split in half would be:
                // return entryCount / 2;
                int x = find(row, false, true, true);
                return x < 2 ? 2 : x >= entryCount - 3 ? entryCount - 3 : x;
            }
            onlyPosition = true;
            // change the offsets (now storing only positions)
            int o = pageSize;
            for (int i = 0; i < entryCount; i++) {
                o -= index.getRowSize(data, getRow(i), true);
                offsets[i] = o;
            }
            last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
            rowLength = index.getRowSize(data, row, true);
            if (SysProperties.CHECK && last - rowLength < start + OFFSET_LENGTH) {
                throw Message.throwInternalError();
            }
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
            x = find(row, false, true, true);
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
        return -1;
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
        PageBtreeLeaf p2 = new PageBtreeLeaf(index, newPageId, index.getPageStore().createData());
        p2.parentPageId = parentPageId;
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

    SearchRow remove(SearchRow row) throws SQLException {
        int at = find(row, false, false, true);
        SearchRow delete = getRow(at);
        if (index.compareRows(row, delete) != 0 || delete.getPos() != row.getPos()) {
            throw Message.getSQLException(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, index.getSQL() + ": " + row);
        }
        if (entryCount == 1) {
            // the page is now empty
            return row;
        }
        removeRow(at);
        index.getPageStore().updateRecord(this, true, data);
        if (at == entryCount) {
            // the last row changed
            return getRow(at - 1);
        }
        // the last row didn't change
        return null;
    }

    void freeChildren() {
        // nothing to do
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

    void find(PageBtreeCursor cursor, SearchRow first, boolean bigger) throws SQLException {
        int i = find(first, bigger, false, false);
        if (i > entryCount) {
            if (parentPageId == PageBtree.ROOT) {
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
        if (parentPageId == PageBtree.ROOT) {
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
        if (parentPageId == PageBtree.ROOT) {
            cursor.setCurrent(null, 0);
            return;
        }
        PageBtreeNode next = (PageBtreeNode) index.getPage(parentPageId);
        next.previousPage(cursor, getPos());
    }

    public String toString() {
        return "page[" + getPos() + "] b-tree leaf table:" + index.getId() + " entries:" + entryCount;
    }

    public void moveTo(Session session, int newPos) throws SQLException {
        PageStore store = index.getPageStore();
        PageBtreeLeaf p2 = new PageBtreeLeaf(index, newPos, store.createData());
        readAllRows();
        p2.rows = rows;
        p2.entryCount = entryCount;
        p2.offsets = offsets;
        p2.onlyPosition = onlyPosition;
        p2.parentPageId = parentPageId;
        p2.start = start;
        store.updateRecord(p2, false, null);
        if (parentPageId == ROOT) {
            index.setRootPageId(session, newPos);
        } else {
            PageBtreeNode p = (PageBtreeNode) store.getPage(parentPageId);
            p.moveChild(getPos(), newPos);
        }
        store.freePage(getPos(), true, data);
    }

}
