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
import org.h2.result.Row;
import org.h2.store.DataPage;
import org.h2.store.PageStore;
import org.h2.store.Record;

/**
 * A leaf page that contains data of one or multiple rows.
 * Format:
 * <ul><li>0-3: parent page id (0 for root)
 * </li><li>4-4: page type
 * </li><li>5-8: table id
 * </li><li>9-10: entry count
 * </li><li>with overflow: 11-14: the first overflow page id
 * </li><li>11- or 15-: list of key / offset pairs (4 bytes key, 2 bytes offset)
 * </li><li>data
 * </li></ul>
 */
class PageDataLeaf extends PageData {

    private static final int KEY_OFFSET_PAIR_LENGTH = 6;
    private static final int KEY_OFFSET_PAIR_START = 11;

    /**
     * The row offsets.
     */
    int[] offsets;

    /**
     * The rows.
     */
    Row[] rows;

    /**
     * The page id of the first overflow page (0 if no overflow).
     */
    int firstOverflowPageId;

    /**
     * The start of the data area.
     */
    int start;

    private boolean written;

    PageDataLeaf(PageScanIndex index, int pageId, int parentPageId, DataPage data) {
        super(index, pageId, parentPageId, data);
        start = KEY_OFFSET_PAIR_START;
    }

    void read() throws SQLException {
        data.setPos(4);
        int type = data.readByte();
        int tableId = data.readInt();
        if (tableId != index.getId()) {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1,
                    "page:" + getPageId() + " expected table:" + index.getId() +
                    " got:" + tableId + " type:" + type);
        }
        entryCount = data.readShortInt();
        offsets = new int[entryCount];
        keys = new int[entryCount];
        rows = new Row[entryCount];
        if (type == Page.TYPE_DATA_LEAF) {
            firstOverflowPageId = data.readInt();
        }
        for (int i = 0; i < entryCount; i++) {
            keys[i] = data.readInt();
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
    int addRowTry(Row row) throws SQLException {
        int rowLength = row.getByteCount(data);
        int pageSize = index.getPageStore().getPageSize();
        int last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
        if (entryCount > 0 && last - rowLength < start + KEY_OFFSET_PAIR_LENGTH) {
            int todoSplitAtLastInsertionPoint;
            return (entryCount / 2) + 1;
        }
        int offset = last - rowLength;
        int[] newOffsets = new int[entryCount + 1];
        int[] newKeys = new int[entryCount + 1];
        Row[] newRows = new Row[entryCount + 1];
        int x;
        if (entryCount == 0) {
            x = 0;
        } else {
            readAllRows();
            x = find(row.getPos());
            System.arraycopy(offsets, 0, newOffsets, 0, x);
            System.arraycopy(keys, 0, newKeys, 0, x);
            System.arraycopy(rows, 0, newRows, 0, x);
            if (x < entryCount) {
                for (int j = x; j < entryCount; j++) {
                    newOffsets[j + 1] = offsets[j] - rowLength;
                }
                System.arraycopy(keys, x, newKeys, x + 1, entryCount - x);
                System.arraycopy(rows, x, newRows, x + 1, entryCount - x);
            }
        }
        written = false;
        last = x == 0 ? pageSize : offsets[x - 1];
        offset = last - rowLength;
        entryCount++;
        start += KEY_OFFSET_PAIR_LENGTH;
        newOffsets[x] = offset;
        newKeys[x] = row.getPos();
        newRows[x] = row;
        offsets = newOffsets;
        keys = newKeys;
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
            int previous = getPos();
            int dataOffset = pageSize;
            int page = index.getPageStore().allocatePage();
            do {
                if (firstOverflowPageId == 0) {
                    firstOverflowPageId = page;
                }
                int type, size, next;
                if (remaining <= pageSize - PageDataLeafOverflow.START_LAST) {
                    type = Page.TYPE_DATA_OVERFLOW | Page.FLAG_LAST;
                    size = remaining;
                    next = 0;
                } else {
                    type = Page.TYPE_DATA_OVERFLOW;
                    size = pageSize - PageDataLeafOverflow.START_MORE;
                    next = index.getPageStore().allocatePage();
                }
                PageDataLeafOverflow overflow = new PageDataLeafOverflow(this, page, type, previous, next, dataOffset, size);
                index.getPageStore().updateRecord(overflow, true, null);
                dataOffset += size;
                remaining -= size;
                previous = page;
                page = next;
            } while (remaining > 0);
        }
        return 0;
    }

    private void removeRow(int i) throws SQLException {
        written = false;
        readAllRows();
        entryCount--;
        if (entryCount <= 0) {
            Message.throwInternalError();
        }
        int[] newOffsets = new int[entryCount];
        int[] newKeys = new int[entryCount];
        Row[] newRows = new Row[entryCount];
        System.arraycopy(offsets, 0, newOffsets, 0, i);
        System.arraycopy(keys, 0, newKeys, 0, i);
        System.arraycopy(rows, 0, newRows, 0, i);
        int startNext = i > 0 ? offsets[i - 1] : index.getPageStore().getPageSize();
        int rowLength = startNext - offsets[i];
        for (int j = i; j < entryCount; j++) {
            newOffsets[j] = offsets[j + 1] + rowLength;
        }
        System.arraycopy(keys, i + 1, newKeys, i, entryCount - i);
        System.arraycopy(rows, i + 1, newRows, i, entryCount - i);
        start -= KEY_OFFSET_PAIR_LENGTH;
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
     * @param at the index
     * @return the row
     */
    Row getRowAt(int at) throws SQLException {
        Row r = rows[at];
        if (r == null) {
            if (firstOverflowPageId != 0) {
                PageStore store = index.getPageStore();
                int pageSize = store.getPageSize();
                data.setPos(pageSize);
                int next = firstOverflowPageId;
                int offset = pageSize;
                data.setPos(pageSize);
                do {
                    Record record = store.getRecord(next);
                    PageDataLeafOverflow page;
                    if (record == null) {
                        DataPage data = store.readPage(next);
                        page = new PageDataLeafOverflow(this, next, data, offset);
                    } else {
                        if (!(record instanceof PageDataLeafOverflow)) {
                            throw Message.getInternalError("page:"+ next + " " + record, null);
                        }
                        page = (PageDataLeafOverflow) record;
                    }
                    next = page.readInto(data);
                } while (next != 0);
            }
            data.setPos(offsets[at]);
            r = index.readRow(data);
            r.setPos(keys[at]);
            rows[at] = r;
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
            p2.addRowTry(getRowAt(splitPoint));
            removeRow(splitPoint);
        }
        return p2;
    }

    int getLastKey() throws SQLException {
        int todoRemove;
        if (entryCount == 0) {
            return 0;
        }
        return getRowAt(entryCount - 1).getPos();
    }

    PageDataLeaf getNextPage() throws SQLException {
        if (parentPageId == Page.ROOT) {
            return null;
        }
        PageDataNode next = (PageDataNode) index.getPage(parentPageId);
        return next.getNextPage(keys[entryCount - 1]);
    }

    PageDataLeaf getFirstLeaf() {
        return this;
    }

    protected void remapChildren() {
        if (firstOverflowPageId == 0) {
            return;
        }
        int testIfReallyNotRequired;
//        PageStore store = index.getPageStore();
//        store.updateRecord(firstOverflowPageId);
//        DataPage overflow = store.readPage(firstOverflowPageId);
//        overflow.reset();
//        overflow.writeInt(getPos());
//        store.writePage(firstOverflowPageId, overflow);
    }

    boolean remove(int key) throws SQLException {
        int i = find(key);
        if (keys[i] != key) {
            throw Message.getSQLException(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, index.getSQL() + ": " + key);
        }
        if (entryCount == 1) {
            return true;
        }
        removeRow(i);
        index.getPageStore().updateRecord(this, true, data);
        return false;
    }

    Row getRow(int key) throws SQLException {
        int index = find(key);
        return getRowAt(index);
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

    private void readAllRows() throws SQLException {
        for (int i = 0; i < entryCount; i++) {
            getRowAt(i);
        }
    }

    private void write() throws SQLException {
        if (written) {
            return;
        }
        readAllRows();
        data.reset();
        data.writeInt(parentPageId);
        int type;
        if (firstOverflowPageId == 0) {
            type = Page.TYPE_DATA_LEAF | Page.FLAG_LAST;
        } else {
            type = Page.TYPE_DATA_LEAF;
        }
        data.writeByte((byte) type);
        data.writeInt(index.getId());
        data.writeShortInt(entryCount);
        if (firstOverflowPageId != 0) {
            data.writeInt(firstOverflowPageId);
        }
        for (int i = 0; i < entryCount; i++) {
            data.writeInt(keys[i]);
            data.writeShortInt(offsets[i]);
        }
        for (int i = 0; i < entryCount; i++) {
            data.setPos(offsets[i]);
            rows[i].write(data);
        }
        written = true;
    }

    DataPage getDataPage() throws SQLException {
        write();
        return data;
    }

    public String toString() {
        return "page[" + getPos() + "] data leaf table:" + index.getId() + " entries:" + entryCount;
    }

}
