/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import org.h2.constant.SysProperties;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.DataPage;
import org.h2.store.DiskFile;
import org.h2.table.Column;
import org.h2.util.IntArray;
import org.h2.util.MemoryUtils;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

/**
 * An inner page of a b-tree index.
 *
 * Page format:
 * <pre>
 * N children.len children[0..len] data.len { data[0].pos [data[0]], ...  }
 *</pre>
 */
public class BtreeNode extends BtreePage {

    private boolean writePos;
    private IntArray pageChildren;

    BtreeNode(BtreeIndex index, DataPage s) throws SQLException {
        super(index);
        int len = s.readInt();
        int[] array = MemoryUtils.newInts(len);
        for (int i = 0; i < array.length; i++) {
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
        pageData = ObjectArray.newInstance();
        pageData.add(pivot);
    }

    BtreeNode(BtreeIndex index, IntArray pageChildren, ObjectArray<SearchRow> pageData) {
        super(index);
        this.pageChildren = pageChildren;
        this.pageData = pageData;
    }

    SearchRow getData(int i) throws SQLException {
        SearchRow r = pageData.get(i);
        if (r == null) {
            int p = pageChildren.get(i + 1);
            Session sysSession = index.getDatabase().getSystemSession();
            BtreePage page = index.getPage(sysSession, p);
            r = page.getFirst(sysSession);
            pageData.set(i, r);
        }
        return r;
    }

    int add(Row newRow, Session session) throws SQLException {
        int l = 0, r = pageData.size();
        while (l < r) {
            int i = (l + r) >>> 1;
            SearchRow row = getData(i);
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
        int at = l;
        if (pageChildren.size() == 0) {
            ObjectArray<SearchRow> empty = ObjectArray.newInstance();
            BtreeLeaf newLeaf = new BtreeLeaf(index, empty);
            index.addPage(session, newLeaf);
            index.deletePage(session, this);
            pageChildren.add(newLeaf.getPos());
            index.updatePage(session, this);
        }
        BtreePage page = index.getPage(session, pageChildren.get(at));
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
        if (splitPoint > 1) {
            return splitPoint;
        }
        index.updatePage(session, this);
        return 0;
    }

    SearchRow remove(Session session, Row oldRow) throws SQLException {
        int l = 0, r = pageData.size();
        int comp = 0;
        while (l < r) {
            int i = (l + r) >>> 1;
            SearchRow row = getData(i);
            comp = index.compareRows(row, oldRow);
            if (comp == 0) {
                comp = index.compareKeys(row, oldRow);
            }
            if (comp > 0) {
                r = i;
            } else {
                l = i + 1;
            }
        }
        int at = l;
        // merge is not implemented to allow concurrent usage
        BtreePage page = index.getPage(session, pageChildren.get(at));
        SearchRow first = page.remove(session, oldRow);
        if (first == null) {
            // the first row didn't change - nothing to do here
            return null;
        }
        if (first == oldRow) {
            // this child is now empty
            index.deletePage(session, this);
            pageChildren.remove(at);
            if (pageChildren.size() == 0) {
                if (root) {
                    // root page: save as it (empty)
                    index.updatePage(session, this);
                    return first;
                }
                // no more children - this page is empty as well
                return oldRow;
            }
            if (at == 0) {
                // the first child is empty - then the first row of this subtree
                // has changed
                first = getData(0);
                pageData.remove(0);
            } else {
                // otherwise the first row didn't change
                first = null;
                pageData.remove(at == 0 ? 0 : at - 1);
            }
            index.updatePage(session, this);
            return first;
        }
        // the child still exists, but the first element has changed
        if (at == 0) {
            // no change in this page, but there is a new first row for this
            // subtree
            return first;
        }
        // the first row of another child has changed - need to update
        index.deletePage(session, this);
        pageData.set(at - 1, first);
        index.updatePage(session, this);
        // but then the first row of this subtree didn't change
        return null;
    }

    BtreePage split(Session session, int splitPoint) throws SQLException {
        ObjectArray<SearchRow> data = ObjectArray.newInstance();
        IntArray children = new IntArray();
        splitPoint++;
        int max = pageData.size();
        if (SysProperties.CHECK && index.getDatabase().getLogIndexChanges() && !isDeleted()) {
            // page must have been deleted already before calling
            // getSplitPoint()
            Message.throwInternalError();
        }
        for (int i = splitPoint; i < max; i++) {
            data.add(getData(splitPoint));
            children.add(getChild(splitPoint));
            pageData.remove(splitPoint);
            pageChildren.remove(splitPoint);
        }
        children.add(getChild(splitPoint));
        pageData.remove(splitPoint - 1);
        pageChildren.remove(splitPoint);
        BtreeNode n2 = new BtreeNode(index, children, data);
        index.updatePage(session, this);
        index.addPage(session, n2);
        return n2;
    }

    private int getChild(int i) {
        return pageChildren.get(i);
    }

    boolean findFirst(BtreeCursor cursor, SearchRow compare, boolean bigger) throws SQLException {
        int l = 0, r = pageData.size();
        while (l < r) {
            int i = (l + r) >>> 1;
            SearchRow row = getData(i);
            int comp = index.compareRows(row, compare);
            if (comp > 0 || (!bigger && comp == 0)) {
                r = i;
            } else {
                l = i + 1;
            }
        }
        if (l >= pageData.size()) {
            BtreePage page = index.getPage(cursor.getSession(), pageChildren.get(l));
            cursor.push(this, l);
            boolean result = page.findFirst(cursor, compare, bigger);
            if (result) {
                return true;
            }
            cursor.pop();
            return false;
        }
        BtreePage page = index.getPage(cursor.getSession(), pageChildren.get(l));
        cursor.push(this, l);
        if (page.findFirst(cursor, compare, bigger)) {
            return true;
        }
        cursor.pop();
        int i = l + 1;
        for (; i < pageData.size(); i++) {
            SearchRow row = getData(i);
            int comp = index.compareRows(row, compare);
            if (comp >= 0) {
                page = index.getPage(cursor.getSession(), pageChildren.get(i));
                cursor.push(this, i);
                if (page.findFirst(cursor, compare, bigger)) {
                    return true;
                }
                cursor.pop();
            }
        }
        page = index.getPage(cursor.getSession(), pageChildren.get(i));
        cursor.push(this, i);
        boolean result = page.findFirst(cursor, compare, bigger);
        if (result) {
            return true;
        }
        cursor.pop();
        return false;
    }

    void next(BtreeCursor cursor, int i) throws SQLException {
        i++;
        if (i <= pageData.size()) {
            cursor.setStackPosition(i);
            BtreePage page = index.getPage(cursor.getSession(), pageChildren.get(i));
            page.first(cursor);
            return;
        }
        nextUpper(cursor);
    }

    void previous(BtreeCursor cursor, int i) throws SQLException {
        i--;
        if (i >= 0) {
            cursor.setStackPosition(i);
            BtreePage page = index.getPage(cursor.getSession(), pageChildren.get(i));
            page.last(cursor);
            return;
        }
        previousUpper(cursor);
    }

    private void nextUpper(BtreeCursor cursor) throws SQLException {
        cursor.pop();
        BtreePosition upper = cursor.pop();
        if (upper == null) {
            cursor.setCurrentRow(null);
        } else {
            cursor.push(upper.page, upper.position);
            upper.page.next(cursor, upper.position);
        }
    }

    private void previousUpper(BtreeCursor cursor) throws SQLException {
        cursor.pop();
        BtreePosition upper = cursor.pop();
        if (upper == null) {
            cursor.setCurrentRow(null);
        } else {
            cursor.push(upper.page, upper.position);
            upper.page.previous(cursor, upper.position);
        }
    }

    void first(BtreeCursor cursor) throws SQLException {
        if (pageChildren.size() == 0) {
            nextUpper(cursor);
            return;
        }
        cursor.push(this, 0);
        BtreePage page = index.getPage(cursor.getSession(), pageChildren.get(0));
        page.first(cursor);
    }

    void last(BtreeCursor cursor) throws SQLException {
        int last = pageChildren.size() - 1;
        if (last < 0) {
            previousUpper(cursor);
            return;
        }
        cursor.push(this, last);
        BtreePage page = index.getPage(cursor.getSession(), pageChildren.get(last));
        page.last(cursor);
    }

    public void prepareWrite() throws SQLException {
        if (getRealByteCount() >= DiskFile.BLOCK_SIZE * BLOCKS_PER_PAGE) {
            writePos = true;
        } else {
            writePos = false;
        }
    }

    public void write(DataPage buff) throws SQLException {
        buff.writeByte((byte) 'N');
        int len = pageChildren.size();
        buff.writeInt(len);
        for (int i = 0; i < len; i++) {
            buff.writeInt(pageChildren.get(i));
        }
        len = pageData.size();
        buff.writeInt(len);
        Column[] columns = index.getColumns();
        for (int i = 0; i < len; i++) {
            if (writePos) {
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
        if (buff.length() > BtreePage.BLOCKS_PER_PAGE * DiskFile.BLOCK_SIZE) {
            Message.throwInternalError("indexed data overflow");
        }
    }

    int getRealByteCount() throws SQLException {
        DataPage dummy = index.getDatabase().getDataPage();
        int len = pageChildren.size();
        int size = 2 + DataPage.LENGTH_INT + DataPage.LENGTH_INT * len;
        len = pageData.size();
        size += DataPage.LENGTH_INT;
        size += len * DataPage.LENGTH_INT;
        for (int i = 0; i < len; i++) {
            SearchRow row = getData(i);
            size += getRowSize(dummy, row);
        }
        return size + index.getRecordOverhead();
    }

    SearchRow getFirst(Session session) throws SQLException {
        for (int i = 0; i < pageChildren.size(); i++) {
            BtreePage page = index.getPage(session, pageChildren.get(i));
            if (page != null) {
                return page.getFirst(session);
            }
        }
        return null;
    }

}
