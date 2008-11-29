/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.store.DataPageBinary;
import org.h2.store.PageStore;
import org.h2.util.IntArray;

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
 * </li><li>if there is more data: 5-8: next overflow page id
 * </li><li>otherwise: 5-6: remaining size
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
     * The page id of the first overflow page (0 if no overflow).
     */
    int firstOverflowPageId;
    
    /**
     * The page ids of all overflow pages (null if no overflow).
     */
    int[] overflowPageIds;
    
    /**
     * The start of the data area.
     */
    int start;
    
    PageDataLeaf(PageScanIndex index, int pageId, int parentPageId, DataPageBinary data) {
        super(index, pageId, parentPageId, data);
        start = 7;
    }

    void read() throws SQLException {
        data.setPos(4);
        int type = data.readByte();
        entryCount = data.readShortInt();
        offsets = new int[entryCount];
        keys = new int[entryCount];
        rows = new Row[entryCount];
        if (type == Page.TYPE_DATA_LEAF_WITH_OVERFLOW) {
            firstOverflowPageId = data.readInt();
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
            getRowAt(i);
        }
        data.reset();
        data.writeInt(parentPageId);
        int type;
        if (firstOverflowPageId == 0) {
            type = Page.TYPE_DATA_LEAF;
        } else {
            type = Page.TYPE_DATA_LEAF_WITH_OVERFLOW;
        }
        data.writeByte((byte) type);
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
        PageStore store = index.getPageStore();
        int pageSize = store.getPageSize();
        store.writePage(pageId, data);
        // don't need to write overflow if we just update the parent page id
        if (data.length() > pageSize && overflowPageIds != null) {
            if (firstOverflowPageId == 0) {
                throw Message.getInternalError();
            }
            DataPageBinary overflow = store.createDataPage();
            int parent = pageId;
            int pos = pageSize;
            int remaining = data.length() - pageSize;
            for (int i = 0; i < overflowPageIds.length; i++) {
                overflow.reset();
                overflow.writeInt(parent);
                int size;
                if (remaining > pageSize - 7) {
                    overflow.writeByte((byte) Page.TYPE_DATA_OVERFLOW_WITH_MORE);
                    overflow.writeInt(overflowPageIds[i + 1]);
                    size = pageSize - overflow.length();
                } else {
                    overflow.writeByte((byte) Page.TYPE_DATA_OVERFLOW_LAST);
                    size = remaining;
                    overflow.writeShortInt(remaining);
                }
                overflow.write(data.getBytes(), pos, size);
                remaining -= size;
                pos += size;
                int id = overflowPageIds[i];
                store.writePage(id, overflow);
                parent = id;
            }
        }
    }

    /**
     * Add a row if possible. If it is possible this method returns 0, otherwise
     * the split point. It is always possible to add one row.
     * 
     * @return the split point of this page, or 0 if no split is required
     */
    int addRow(Row row) throws SQLException {
        int rowLength = row.getByteCount(data);
        int pageSize = index.getPageStore().getPageSize();
        int last = entryCount == 0 ? pageSize : offsets[entryCount - 1];
        if (entryCount > 0 && last - rowLength < start + 6) {
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
            if (entryCount > 1) {
                throw Message.getInternalError();
            }
            // need to write the overflow page id
            start += 4;
            int remaining = rowLength - (pageSize - start);
            // fix offset
            offset = start;
            offsets[x] = offset;
            IntArray array = new IntArray();
            do {
                int next = index.getPageStore().allocatePage();
                array.add(next);
                remaining -= pageSize - 7;
                if (remaining > 0) {
                    remaining += 2;
                }
            } while (remaining > 0);
            overflowPageIds = new int[array.size()];
            array.toArray(overflowPageIds);
            firstOverflowPageId = overflowPageIds[0];
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
     * @param at the index
     * @return the row
     */
    Row getRowAt(int at) throws SQLException {
        if (at >= rows.length) {
            int test;
            System.out.println("test");
        }
        Row r = rows[at];
        if (r == null) {
            if (firstOverflowPageId != 0) {
                PageStore store = index.getPageStore();
                int pageSize = store.getPageSize();
                data.setPos(pageSize);
                int next = firstOverflowPageId;
                while (true) {
                    DataPageBinary page = store.readPage(next);
                    page.setPos(4);
                    int type = page.readByte();
                    if (type == Page.TYPE_DATA_OVERFLOW_LAST) {
                        int size = page.readShortInt();
                        data.write(page.getBytes(), 7, size);
                        break;
                    } else {
                        next = page.readInt();
                        int size = pageSize - 9;
                        data.write(page.getBytes(), 9, size);
                    }
                }
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
            p2.addRow(getRowAt(splitPoint));
            removeRow(splitPoint);
        }
        return p2;
    }

    int getLastKey() throws SQLException {
        int todoRemove;
        return getRowAt(entryCount - 1).getPos();
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
    
    protected void remapChildren() throws SQLException {
        int todoUpdateOverflowPages;
    }

    boolean remove(int key) throws SQLException {
        int i = find(key);
        if (keys[i] != key) {
            throw Message.getSQLException(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, index.getSQL());
        }
        if (entryCount == 1) {
            return true;
        }
        removeRow(i);
        write();
        return false;
    }

    Row getRow(Session session, int key) throws SQLException {
        int index = find(key);
        return getRowAt(index);
    }

}
