/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.store.DataPageBinary;

/**
 * A leaf page that contains data of one or multiple rows.
 * Format:
 * <ul><li>0-3: parent page id
 * </li><li>4-4: page type
 * </li><li>5-5: entry count
 * </li><li>only if there is overflow: 6-9: overflow page id
 * </li><li>list of offsets (2 bytes each)
 * </li></ul>
 * The format of an overflow page is:
 * <ul><li>0-3: parent page id
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
        entryCount = data.readByte() & 255;
        offsets = new int[entryCount];
        rows = new Row[entryCount];
        if (type == Page.TYPE_DATA_LEAF_WITH_OVERFLOW) {
            overflowPageId = data.readInt();
        }
        for (int i = 0; i < entryCount; i++) {
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
        data.writeByte((byte) entryCount);
        if (overflowPageId != 0) {
            data.writeInt(overflowPageId);
        }
        for (int i = 0; i < entryCount; i++) {
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
        if (entryCount >= 255) {
            return entryCount / 2;
        }
        int rowLength = row.getByteCount(data);
        int last = entryCount == 0 ? index.getPageStore().getPageSize() : offsets[entryCount - 1];
        int offset = last - rowLength;
        if (offset < start + 2) {
            if (entryCount > 0) {
                return entryCount / 2;
            }
            offset = start + 2;
            overflowPageId = index.getPageStore().allocatePage();
        }
        changed = true;
        entryCount++;
        int[] newOffsets = new int[entryCount];
        Row[] newRows = new Row[entryCount];
        System.arraycopy(offsets, 0, newOffsets, 0, entryCount - 1);
        System.arraycopy(rows, 0, newRows, 0, entryCount - 1);
        start += 2;
        newOffsets[entryCount - 1] = offset;
        newRows[entryCount - 1] = row;
        offsets = newOffsets;
        rows = newRows;
        write();
        return 0;
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
            rows[index] = r;
        }
        return r;
    }

    int getEntryCount() {
        return entryCount;
    }

}
