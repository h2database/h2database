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
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.DataPage;
import org.h2.store.DiskFile;
import org.h2.table.Column;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

/**
 * An outer page of a b-tree index.
 *
 * Page format:
 * <pre>
 * L { P(pointers) | D(data) } data.len { data[0].pos [data[0]], ...  }
 * </pre>
 */
public class BtreeLeaf extends BtreePage {

    private boolean writePos;
    private int cachedRealByteCount;

    BtreeLeaf(BtreeIndex index, Session session, DataPage s) throws SQLException {
        super(index);
        writePos = s.readByte() == 'P';
        if (writePos) {
            int size = s.readInt();
            // should be 1, but may not be 1
            pageData = ObjectArray.newInstance(size);
            for (int i = 0; i < size; i++) {
                Row r = index.getRow(session, s.readInt());
                pageData.add(r);
            }
        } else {
            pageData = index.readRowArray(s);
        }
    }

    BtreeLeaf(BtreeIndex index, ObjectArray<SearchRow> pageData) {
        super(index);
        this.pageData = pageData;
    }

    int add(Row newRow, Session session) throws SQLException {
        int l = 0, r = pageData.size();
        while (l < r) {
            int i = (l + r) >>> 1;
            SearchRow row = pageData.get(i);
            int comp = index.compareRows(row, newRow);
            if (comp == 0) {
                if (index.indexType.isUnique()) {
                    if (!index.containsNullAndAllowMultipleNull(newRow)) {
                        throw index.getDuplicateKeyException();
                    }
                }
                comp = index.compareKeys(row, newRow);
            }
            if (comp > 0) {
                r = i;
            } else {
                l = i + 1;
            }
        }
        index.deletePage(session, this);
        int at = l;
        // safe memory
        SearchRow row = index.getSearchRow(newRow);
        pageData.add(at, row);
        updateRealByteCount(true, newRow);
        int splitPoint = getSplitPoint();
        if (splitPoint == 0) {
            index.updatePage(session, this);
        }
        return splitPoint;
    }

    SearchRow remove(Session session, Row oldRow) throws SQLException {
        int l = 0, r = pageData.size();
        while (l < r) {
            int i = (l + r) >>> 1;
            SearchRow row = pageData.get(i);
            if (SysProperties.CHECK && row == null) {
                Message.throwInternalError("b-tree corrupt");
            }
            int comp = index.compareRows(row, oldRow);
            if (comp == 0) {
                comp = index.compareKeys(row, oldRow);
            }
            if (comp == 0) {
                index.deletePage(session, this);
                if (pageData.size() == 1 && !root) {
                    // the last row has been deleted
                    return oldRow;
                }
                pageData.remove(i);
                updateRealByteCount(false, row);
                index.updatePage(session, this);
                if (i > 0) {
                    // the first row didn't change
                    return null;
                }
                if (pageData.size() == 0) {
                    return null;
                }
                return getData(0);
            }
            if (comp > 0) {
                r = i;
            } else {
                l = i + 1;
            }
        }
        throw Message.getSQLException(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, index.getSQL());
    }

    BtreePage split(Session session, int splitPoint) throws SQLException {
        ObjectArray<SearchRow> data = ObjectArray.newInstance();
        int max = pageData.size();
        for (int i = splitPoint; i < max; i++) {
            data.add(getData(splitPoint));
            pageData.remove(splitPoint);
        }
        cachedRealByteCount = 0;
        BtreeLeaf n2 = new BtreeLeaf(index, data);
        index.updatePage(session, this);
        index.addPage(session, n2);
        return n2;
    }

    boolean findFirst(BtreeCursor cursor, SearchRow compare, boolean bigger) throws SQLException {
        int l = 0, r = pageData.size();
        while (l < r) {
            int i = (l + r) >>> 1;
            SearchRow row = pageData.get(i);
            int comp = index.compareRows(row, compare);
            if (comp > 0 || (!bigger && comp == 0)) {
                r = i;
            } else {
                l = i + 1;
            }
        }
        if (l >= pageData.size()) {
            return false;
        }
        cursor.push(this, l);
        SearchRow row = pageData.get(l);
        cursor.setCurrentRow(row);
        return true;
    }

    void next(BtreeCursor cursor, int i) throws SQLException {
        i++;
        if (i < pageData.size()) {
            SearchRow r = pageData.get(i);
            cursor.setCurrentRow(r);
            cursor.setStackPosition(i);
            return;
        }
        cursor.pop();
        nextUpper(cursor);
    }

    void previous(BtreeCursor cursor, int i) throws SQLException {
        i--;
        if (i >= 0) {
            SearchRow r = pageData.get(i);
            cursor.setCurrentRow(r);
            cursor.setStackPosition(i);
            return;
        }
        cursor.pop();
        previousUpper(cursor);
    }

    void first(BtreeCursor cursor) throws SQLException {
        if (pageData.size() == 0) {
            nextUpper(cursor);
            return;
        }
        cursor.push(this, 0);
        SearchRow row = pageData.get(0);
        cursor.setCurrentRow(row);
    }

    void last(BtreeCursor cursor) throws SQLException {
        int last = pageData.size() - 1;
        if (last < 0) {
            previousUpper(cursor);
            return;
        }
        cursor.push(this, last);
        SearchRow row = pageData.get(last);
        cursor.setCurrentRow(row);
    }

    private void nextUpper(BtreeCursor cursor) throws SQLException {
        BtreePosition upper = cursor.pop();
        if (upper == null) {
            cursor.setCurrentRow(null);
        } else {
            cursor.push(upper.page, upper.position);
            upper.page.next(cursor, upper.position);
        }
    }

    private void previousUpper(BtreeCursor cursor) throws SQLException {
        BtreePosition upper = cursor.pop();
        if (upper == null) {
            cursor.setCurrentRow(null);
        } else {
            cursor.push(upper.page, upper.position);
            upper.page.previous(cursor, upper.position);
        }
    }

    public void prepareWrite() throws SQLException {
        if (getRealByteCount() >= DiskFile.BLOCK_SIZE * BLOCKS_PER_PAGE) {
            writePos = true;
        } else {
            writePos = false;
        }
    }

    public void write(DataPage buff) throws SQLException {
        buff.writeByte((byte) 'L');
        int len = pageData.size();
        if (writePos) {
            buff.writeByte((byte) 'P');
        } else {
            buff.writeByte((byte) 'D');
        }
        buff.writeInt(len);
        Column[] columns = index.getColumns();
        for (int i = 0; i < len; i++) {
            SearchRow row = pageData.get(i);
            buff.writeInt(row.getPos());
            if (!writePos) {
                for (int j = 0; j < columns.length; j++) {
                    Value v = row.getValue(columns[j].getColumnId());
                    buff.writeValue(v);
                }
            }
        }
    }

    private void updateRealByteCount(boolean add, SearchRow row) throws SQLException {
        if (cachedRealByteCount == 0) {
            return;
        }
        DataPage dummy = index.getDatabase().getDataPage();
        int diff = getRowSize(dummy, row) + DataPage.LENGTH_INT;
        cachedRealByteCount += add ? diff : -diff;
        if (cachedRealByteCount + index.getRecordOverhead() >= DiskFile.BLOCK_SIZE * BLOCKS_PER_PAGE) {
            cachedRealByteCount = 0;
        }
    }

    int getRealByteCount() throws SQLException {
        DataPage dummy = index.getDatabase().getDataPage();
        int len = pageData.size();
        int size = 2 + DataPage.LENGTH_INT * (len + 1);
        for (int i = 0; i < len; i++) {
            SearchRow row = pageData.get(i);
            size += getRowSize(dummy, row);
        }
        size += index.getRecordOverhead();
        cachedRealByteCount = size;
        return size;
    }

    SearchRow getFirst(Session session) {
        if (pageData.size() == 0) {
            return null;
        }
        return pageData.get(0);
    }

}
