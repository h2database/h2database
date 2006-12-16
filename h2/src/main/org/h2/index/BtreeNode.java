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
import org.h2.util.IntArray;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

/**
 * Page format:
 * N children.len children[0..len] data.len { data[0].pos [data[0]], ...  }
 *
 * @author Thomas
 */
public class BtreeNode extends BtreePage {

    private boolean writePos;
    private IntArray pageChildren;

    BtreeNode(BtreeIndex index, DataPage s) throws SQLException {
        super(index);
        int len = s.readInt();
        int[] array = new int[len];
        for(int i=0; i<array.length; i++) {
            array[i] = s.readInt();
        }
        pageChildren = new IntArray(array);
        pageData = index.readRowArray(s);
    }

    BtreeNode(BtreeIndex index, BtreePage left, SearchRow pivot, BtreePage right) {
        super(index);
        pageChildren = new IntArray();
        pageChildren.add(left.getPos());
        pageChildren.add(right.getPos());
        pageData = new ObjectArray();
        pageData.add(pivot);
    }

    BtreeNode(BtreeIndex index, IntArray pageChildren, ObjectArray pageData) {
        super(index);
        this.pageChildren = pageChildren;
        this.pageData = pageData;
    }

    protected SearchRow getData(int i) throws SQLException {
        SearchRow r = (SearchRow) pageData.get(i);
        if(r == null) {
            int p = pageChildren.get(i+1);
            BtreePage page = index.getPage(p);
            r = page.getFirst();
            pageData.set(i, r);
        }
        return r;
    }

    public int add(Row newRow, Session session) throws SQLException {
        int l = 0, r = pageData.size();
        if (!Constants.ALLOW_EMTPY_BTREE_PAGES && pageChildren.size() == 0) {
            throw Message.getInternalError("Empty btree page");
        }
        while (l < r) {
            int i = (l + r) >>> 1;
            SearchRow row = getData(i);
            int comp = index.compareRows(row, newRow);
            if(comp == 0) {
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
        int at = l;
        BtreePage page = index.getPage(pageChildren.get(at));
        int splitPoint = page.add(newRow, session);
        if (splitPoint == 0) {
            return 0;
        }
        SearchRow pivot = page.getData(splitPoint);
        BtreePage page2 = page.split(session, splitPoint);
        index.deletePage(session, this);
        pageChildren.add(at + 1, page2.getPos());
        pageData.add(at, pivot);
        splitPoint = getSplitPoint();
        if(splitPoint > 1) {
            return splitPoint;
        }
        index.updatePage(session, this);
        return 0;
    }

    public SearchRow remove(Session session, Row oldRow, int level) throws SQLException {
        int l = 0, r = pageData.size();
        if (!Constants.ALLOW_EMTPY_BTREE_PAGES && pageChildren.size() == 0) {
            throw Message.getInternalError("Empty btree page");
        }
        int comp = 0;
        while (l < r) {
            int i = (l + r) >>> 1;
            SearchRow row = getData(i);
            comp = index.compareRows(row, oldRow);
            if(comp == 0) {
                comp = index.compareKeys(row, oldRow);
            }
            if(comp > 0) {
                r = i;
            } else {
                l = i + 1;
            }
        }
        int at = l;
        // merge is not implemented to allow concurrent usage of btrees
        BtreePage page = index.getPage(pageChildren.get(at));
        SearchRow first = page.remove(session, oldRow, level+1);
        if(first == null) {
            // the first row didn't change - nothing to do here
            return null;
        }
        if(first == oldRow) {
            // this child is now empty
            index.deletePage(session, this);
            pageChildren.remove(at);
            if(pageChildren.size()==0) {
                // no more children - this page is empty as well
                // it can't be the root otherwise the index would have been truncated
                return oldRow;
            }
            if(at == 0) {
                // the first child is empty - then the first row of this subtree has changed
                first = getData(0);
                pageData.remove(0);
            } else {
                // otherwise the first row didn't change
                first = null;
                pageData.remove(at==0 ? 0 : at-1);
            }
            index.updatePage(session, this);
            return first;
        } else {
            // the child still exists, but the first element has changed
            if(at == 0) {
                // no change in this page, but there is a new first row for this subtree
                return first;
            } else {
                // the first row of another child has changed - need to update
                index.deletePage(session, this);
                pageData.set(at-1, first);
                index.updatePage(session, this);
                // but then the first row of this subtree didn't change
                return null;
            }
        }
    }

    public BtreePage split(Session session, int splitPoint) throws SQLException {
        ObjectArray data = new ObjectArray();
        IntArray children = new IntArray();
        splitPoint++;
        int max = pageData.size();
        int test;
        // if(Constants.CHECK  && index.getDatabase().getLogIndexChanges() && !getDeleted()) {
            // page must have been deleted already before calling getSplitPoint()
        //    throw Message.getInternalError();
        // }
        for (int i = splitPoint; i < max; i++) {
            data.add(getData(splitPoint));
            children.add(getChild(splitPoint));
            pageData.remove(splitPoint);
            pageChildren.remove(splitPoint);
        }
        children.add(getChild(splitPoint));
        pageData.remove(splitPoint-1);
        pageChildren.remove(splitPoint);
        BtreeNode n2 = new BtreeNode(index, children, data);
        index.updatePage(session, this);
        index.addPage(session, n2);
        return n2;
    }

    int getChild(int i) {
        return pageChildren.get(i);
    }

    public boolean findFirst(BtreeCursor cursor, SearchRow compare) throws SQLException {
        int l = 0, r = pageData.size();
        if (!Constants.ALLOW_EMTPY_BTREE_PAGES && pageChildren.size() == 0) {
            throw Message.getInternalError("Empty btree page");
        }
        while (l < r) {
            int i = (l + r) >>> 1;
            SearchRow row = getData(i);
            int comp = index.compareRows(row, compare);
            if(comp >= 0) {
                r = i;
            } else {
                l = i + 1;
            }
        }
        if(l>=pageData.size()) {
            BtreePage page = index.getPage(pageChildren.get(l));
            cursor.push(this, l);
            boolean result = page.findFirst(cursor, compare);
            if(result) {
                return true;
            }
            cursor.pop();
            return false;
        }
        BtreePage page = index.getPage(pageChildren.get(l));
        cursor.push(this, l);
        if(page.findFirst(cursor, compare)) {
            return true;
        }
        cursor.pop();
        int i=l+1;
        for (; i < pageData.size(); i++) {
            SearchRow row = getData(i);
            int comp = index.compareRows(row, compare);
            if (comp >=0) {
                page = index.getPage(pageChildren.get(i));
                cursor.push(this, i);
                if(page.findFirst(cursor, compare)) {
                    return true;
                }
                cursor.pop();
            }
        }
        page = index.getPage(pageChildren.get(i));
        cursor.push(this, i);
        boolean result = page.findFirst(cursor, compare);
        if(result) {
            return true;
        }
        cursor.pop();
        return false;
    }

    public void next(BtreeCursor cursor, int i) throws SQLException {
        i++;
        if (i <= pageData.size()) {
            cursor.setStackPosition(i);
            BtreePage page = index.getPage(pageChildren.get(i));
            page.first(cursor);
            return;
        }
        nextUpper(cursor);
    }

    private void nextUpper(BtreeCursor cursor)  throws SQLException {
        cursor.pop();
        BtreePosition upper = cursor.pop();
        if (upper == null) {
            cursor.setCurrentRow(Cursor.POS_NO_ROW);
        } else {
            cursor.push(upper.page, upper.position);
            upper.page.next(cursor, upper.position);
        }
    }

    public void first(BtreeCursor cursor) throws SQLException {
        cursor.push(this, 0);
        BtreePage page = index.getPage(pageChildren.get(0));
        page.first(cursor);
    }

    public void prepareWrite() throws SQLException {
        if(getRealByteCount() >= DiskFile.BLOCK_SIZE*BLOCKS_PER_PAGE) {
            writePos = true;
        } else {
            writePos = false;
        }
    }

    public void write(DataPage buff) throws SQLException {
        buff.writeByte((byte)'N');
        int len = pageChildren.size();
        buff.writeInt(len);
        for (int i = 0; i < len; i++) {
            buff.writeInt(pageChildren.get(i));
        }
        len = pageData.size();
        buff.writeInt(len);
        Column[] columns = index.getColumns();
        for (int i = 0; i < len; i++) {
            if(writePos) {
                buff.writeInt(-1);
            } else {
                SearchRow row = getData(i);
                buff.writeInt(row.getPos());
                for (int j = 0; j < columns.length; j++) {
                    Value v = row.getValue(columns[j].getColumnId());
                    buff.writeValue(v);
                }
            }
        }
        if(buff.length() > BtreePage.BLOCKS_PER_PAGE*DiskFile.BLOCK_SIZE) {
            throw Message.getInternalError("indexed data overflow");
        }
    }

    int getRealByteCount() throws SQLException {
        DataPage dummy = index.getDatabase().getDataPage();
        int len = pageChildren.size();
        int size = 2 + dummy.getIntLen() + dummy.getIntLen() * len;
        len = pageData.size();
        size += dummy.getIntLen();
        size += pageData.size() * dummy.getIntLen();
        for (int i = 0; i < pageData.size(); i++) {
            SearchRow row = getData(i);
            size += getRowSize(dummy, row);
        }
        return size + index.getRecordOverhead();
    }

    SearchRow getLast() throws SQLException {
        if (!Constants.ALLOW_EMTPY_BTREE_PAGES && pageChildren.size() == 0) {
            throw Message.getInternalError("Empty btree page");
        }
        for(int i=pageChildren.size()-1; i>=0; i--) {
            BtreePage page = index.getPage(pageChildren.get(i));
            if(page != null) {
                return page.getLast();
            }
        }
        return null;
    }

    SearchRow getFirst() throws SQLException {
        for(int i=0; i<pageChildren.size(); i++) {
            BtreePage page = index.getPage(pageChildren.get(i));
            if(page != null) {
                return page.getFirst();
            }
        }
        return null;
    }

    public String print(String indent) throws SQLException {
        System.out.println(indent + "node");
        String first = null;
        String last = null;
        for(int i=0; i<pageChildren.size(); i++) {
            String firstnow = index.getPage(pageChildren.get(i)).print(indent + "    ");
            if(first == null) {
                first = firstnow;
            }
            if(last != null && !last.equals(firstnow)) {
                System.out.println("STOP!!! " + last + " firstnow:" + firstnow);
            }
            if(i<pageData.size()) {
                String now = getData(i).getValue(1).getString().substring(4150);
                System.out.println(indent + "  " + now);
                last = now;
            }
        }
        return first;
    }

}
