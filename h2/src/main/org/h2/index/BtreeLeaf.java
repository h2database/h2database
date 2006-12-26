/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.engine.Constants;
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
 * Page format:
 * L { P(pointers) | D(data) } data.len { data[0].pos [data[0]], ...  }
 *
 * @author Thomas
 */
public class BtreeLeaf extends BtreePage {

    private boolean writePos;
    private int cachedRealByteCount;

    BtreeLeaf(BtreeIndex index, DataPage s) throws SQLException {
        super(index);
        writePos = s.readByte() == 'P';
        if(writePos) {
            int size = s.readInt();
            // should be 1, but may not be 1
            pageData = new ObjectArray(size);
            for(int i=0; i<size; i++) {
                Row r = index.getRow(s.readInt());
                pageData.add(r);
            }
        } else {
            pageData = index.readRowArray(s);
        }
    }

    BtreeLeaf(BtreeIndex index, ObjectArray pageData) {
        super(index);
        this.pageData = pageData;
    }

    public int add(Row newRow, Session session) throws SQLException {
        int l = 0, r = pageData.size();
        while (l < r) {
            int i = (l + r) >>> 1;
            SearchRow row = (SearchRow) pageData.get(i);
            int comp = index.compareRows(row, newRow);
            if (comp == 0) {
                if(index.indexType.isUnique()) {
                    if(!index.isNull(newRow)) {
                        throw index.getDuplicateKeyException();
                    }
                }
                comp = index.compareKeys(row, newRow);
            }
            if(comp > 0) {
                r = i;
            } else {
                l = i + 1;
            }
        }
        index.deletePage(session, this);
        int at = l;
        pageData.add(at, newRow);
        updateRealByteCount(true, newRow);
        int splitPoint = getSplitPoint();
        if(splitPoint == 0) {
            index.updatePage(session, this);
        }
        return splitPoint;
    }

    public SearchRow remove(Session session, Row oldRow, int level) throws SQLException {
        int l = 0, r = pageData.size();
        if (r == 0) {
            if(!Constants.ALLOW_EMTPY_BTREE_PAGES && !root) {
                throw Message.getInternalError("Empty btree page");
            }
        }
        while (l < r) {
            int i = (l + r) >>> 1;
            SearchRow row = (SearchRow) pageData.get(i);
            if(Constants.CHECK && row == null) {
                throw Message.getInternalError("btree currupted");
            }
            int comp = index.compareRows(row, oldRow);
            if (comp == 0) {
                comp = index.compareKeys(row, oldRow);
            }
            if(comp == 0) {
                index.deletePage(session, this);
                if(pageData.size()==1 && !root) {
                    // the last row has been deleted
                    return oldRow;
                }
                pageData.remove(i);
                updateRealByteCount(false, row);
                index.updatePage(session, this);
                if(i > 0) {
                    // the first row didn't change
                    return null;
                } else {
                    if(pageData.size() == 0) {
                        return null;
                    } else {
                        return getData(0);
                    }
                }
            }
            if(comp > 0) {
                r = i;
            } else {
                l = i + 1;
            }
        }
        throw Message.getSQLException(Message.ROW_NOT_FOUND_WHEN_DELETING_1, index.getSQL());
    }

    public BtreePage split(Session session, int splitPoint) throws SQLException {
        ObjectArray data = new ObjectArray();
        int max = pageData.size();
        if(Constants.CHECK && index.getDatabase().getLogIndexChanges() && !getDeleted()) {
            // page must have been deleted already before calling getSplitPoint()
            throw Message.getInternalError();
        }
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

    public boolean findFirst(BtreeCursor cursor, SearchRow compare) throws SQLException {
        int l = 0, r = pageData.size();
        if (r == 0 && !Constants.ALLOW_EMTPY_BTREE_PAGES && !root) {
            throw Message.getInternalError("Empty btree page");
        }
        while (l < r) {
            int i = (l + r) >>> 1;
            SearchRow row = (SearchRow) pageData.get(i);
            int comp = index.compareRows(row, compare);
            if(comp >= 0) {
                r = i;
            } else {
                l = i + 1;
            }
        }
        if(l>=pageData.size()) {
            return false;
        }
        cursor.push(this, l);
        SearchRow row = (SearchRow) pageData.get(l);
        cursor.setCurrentRow(row.getPos());
        return true;
    }

    public void next(BtreeCursor cursor, int i) throws SQLException {
        i++;
        if (i < pageData.size()) {
            SearchRow r = (SearchRow) pageData.get(i);
            cursor.setCurrentRow(r.getPos());
            cursor.setStackPosition(i);
            return;
        }
        cursor.pop();
        nextUpper(cursor);
    }

    public void first(BtreeCursor cursor) throws SQLException {
        if (pageData.size() == 0) {
            if (!Constants.ALLOW_EMTPY_BTREE_PAGES && !root) {
                throw Message.getInternalError("Empty btree page");
            }
            nextUpper(cursor);
            return;
        }
        cursor.push(this, 0);
        SearchRow row = (SearchRow) pageData.get(0);
        cursor.setCurrentRow(row.getPos());
    }

    private void nextUpper(BtreeCursor cursor) throws SQLException  {
        BtreePosition upper = cursor.pop();
        if (upper == null) {
            cursor.setCurrentRow(Cursor.POS_NO_ROW);
        } else {
            cursor.push(upper.page, upper.position);
            upper.page.next(cursor, upper.position);
        }
    }

    public void prepareWrite() throws SQLException {
        if(getRealByteCount() >= DiskFile.BLOCK_SIZE*BLOCKS_PER_PAGE) {
            writePos = true;
        } else {
            writePos = false;
        }
    }

    public void write(DataPage buff) throws SQLException {
        buff.writeByte((byte)'L');
        int len = pageData.size();
        if(writePos) {
            buff.writeByte((byte)'P');
        } else {
            buff.writeByte((byte)'D');
        }
        buff.writeInt(len);
        Column[] columns = index.getColumns();
        for (int i = 0; i < len; i++) {
            SearchRow row = (SearchRow) pageData.get(i);
            buff.writeInt(row.getPos());
            if(!writePos) {
                for (int j = 0; j < columns.length; j++) {
                    Value v = row.getValue(columns[j].getColumnId());
                    buff.writeValue(v);
                }
            }
        }
    }

    private void updateRealByteCount(boolean add, SearchRow row) throws SQLException {
        if(cachedRealByteCount == 0) {
            return;
        }
        DataPage dummy = index.getDatabase().getDataPage();
        cachedRealByteCount += getRowSize(dummy, row) + dummy.getIntLen();
        if(cachedRealByteCount+index.getRecordOverhead() >= DiskFile.BLOCK_SIZE*BLOCKS_PER_PAGE) {
            cachedRealByteCount = 0;
        }
    }

    public int getRealByteCount() throws SQLException {
        if(cachedRealByteCount > 0) {
            return cachedRealByteCount;
        }
        DataPage dummy = index.getDatabase().getDataPage();
        int len = pageData.size();
        int size = 2 + dummy.getIntLen() * (len+1);
        for (int i = 0; i < len; i++) {
            SearchRow row = (SearchRow) pageData.get(i);
            size += getRowSize(dummy, row);
        }
        size += index.getRecordOverhead();
        cachedRealByteCount = size;
        return size;
    }

    SearchRow getLast() throws SQLException {
        if(pageData.size()==0) {
            if(!Constants.ALLOW_EMTPY_BTREE_PAGES && !root) {
                throw Message.getInternalError("Empty btree page");
            }
            return null;
        }
        return (SearchRow)pageData.get(pageData.size()-1);
    }

    SearchRow getFirst() throws SQLException {
        if(pageData.size()==0) {
            if(!Constants.ALLOW_EMTPY_BTREE_PAGES && !root) {
                throw Message.getInternalError("Empty btree page");
            }
            return null;
        }
        return (SearchRow)pageData.get(0);
    }

    public String print(String indent) throws SQLException {
        System.out.println(indent + "leaf:");
        for(int i=0; i<pageData.size(); i++) {
            System.out.println(indent + "  " + getData(i).getValue(1).getString().substring(4150));
        }
        return getData(0).getValue(1).getString().substring(4150);
    }

}
