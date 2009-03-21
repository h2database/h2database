/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.message.TraceSystem;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.DataPage;
import org.h2.store.PageStore;
import org.h2.store.Record;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.value.Value;
import org.h2.value.ValueLob;

/**
 * The scan index allows to access a row by key. It can be used to iterate over
 * all rows of a table. Each regular table has one such object, even if no
 * primary key or indexes are defined.
 */
public class PageScanIndex extends BaseIndex implements RowIndex {

    private PageStore store;
    private TableData tableData;
    private final int headPos;
    private int lastKey;
    private long rowCount;

    public PageScanIndex(TableData table, int id, IndexColumn[] columns, IndexType indexType, int headPos) throws SQLException {
        initBaseIndex(table, id, table.getName() + "_TABLE_SCAN", columns, indexType);
        int test;
        // trace.setLevel(TraceSystem.DEBUG);
        if (database.isMultiVersion()) {
            int todoMvcc;
        }
        tableData = table;
        this.store = database.getPageStore();
        if (!database.isPersistent()) {
            int todo;
            this.headPos = 0;
            return;
        }
        if (headPos == Index.EMPTY_HEAD) {
            // new table
            this.headPos = headPos = store.allocatePage();
            store.addMeta(this);
            PageDataLeaf root = new PageDataLeaf(this, headPos, Page.ROOT, store.createDataPage());
            store.updateRecord(root, true, root.data);

//        } else if (store.isNew()) {
//            // the system table for a new database
//            PageDataLeaf root = new PageDataLeaf(this, headPos,
//                    Page.ROOT, store.createDataPage());
//            store.updateRecord(root, true, root.data);
        } else {
            this.headPos = headPos;
            PageData root = getPage(headPos);
            lastKey = root.getLastKey();
            rowCount = root.getRowCount();
            // could have been created before, but never committed
            store.updateRecord(root, false, null);
            int reuseKeysIfManyDeleted;
        }
        if (trace.isDebugEnabled()) {
            trace.debug("opened " + getName() + " rows:" + rowCount);
        }
        table.setRowCount(rowCount);
    }

    public int getHeadPos() {
        return headPos;
    }

    public void add(Session session, Row row) throws SQLException {
        if (row.getPos() == 0) {
            row.setPos(++lastKey);
        }
        if (trace.isDebugEnabled()) {
            trace.debug("add " + row.getPos());
        }
        if (tableData.getContainsLargeObject()) {
            for (int i = 0; i < row.getColumnCount(); i++) {
                Value v = row.getValue(i);
                Value v2 = v.link(database, getId());
                if (v2.isLinked()) {
                    session.unlinkAtCommitStop(v2);
                }
                if (v != v2) {
                    row.setValue(i, v2);
                }
            }
        }
        while (true) {
            PageData root = getPage(headPos);
            int splitPoint = root.addRow(row);
            if (splitPoint == 0) {
                break;
            }
            if (trace.isDebugEnabled()) {
                trace.debug("split " + splitPoint);
            }
            int pivot = root.getKey(splitPoint - 1);
            PageData page1 = root;
            PageData page2 = root.split(splitPoint);
            int rootPageId = root.getPageId();
            int id = store.allocatePage();
            page1.setPageId(id);
            page1.setParentPageId(headPos);
            page2.setParentPageId(headPos);
            PageDataNode newRoot = new PageDataNode(this, rootPageId, Page.ROOT, store.createDataPage());
            newRoot.init(page1, pivot, page2);
            store.updateRecord(page1, true, page1.data);
            store.updateRecord(page2, true, page2.data);
            store.updateRecord(newRoot, true, null);
            root = newRoot;
        }
        rowCount++;
        store.logAddOrRemoveRow(session, tableData.getId(), row, true);
    }

    /**
     * Read the given page.
     *
     * @param id the page id
     * @return the page
     */
    PageData getPage(int id) throws SQLException {
        Record rec = store.getRecord(id);
        if (rec != null) {
            if (rec instanceof PageDataLeafOverflow) {
                int test;
                System.out.println("stop");
            }
            return (PageData) rec;
        }
        DataPage data = store.readPage(id);
        data.reset();
        int parentPageId = data.readInt();
        int type = data.readByte() & 255;
        PageData result;
        switch (type & ~Page.FLAG_LAST) {
        case Page.TYPE_DATA_LEAF:
            result = new PageDataLeaf(this, id, parentPageId, data);
            break;
        case Page.TYPE_DATA_NODE:
            result = new PageDataNode(this, id, parentPageId, data);
            break;
        case Page.TYPE_EMPTY:
            PageDataLeaf empty = new PageDataLeaf(this, id, parentPageId, data);
            return empty;
        default:
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, "page=" + id + " type=" + type);
        }
        result.read();
        return result;
    }

    public boolean canGetFirstOrLast() {
        return false;
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        PageData root = getPage(headPos);
        return root.find();
    }

    public Cursor findFirstOrLast(Session session, boolean first) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public double getCost(Session session, int[] masks) throws SQLException {
        long cost = 10 * (tableData.getRowCountApproximation() + Constants.COST_ROW_OFFSET);
        return cost;
    }

    public boolean needRebuild() {
        return false;
    }

    public void remove(Session session, Row row) throws SQLException {
        if (trace.isDebugEnabled()) {
            trace.debug("remove " + row.getPos());
        }
        if (tableData.getContainsLargeObject()) {
            for (int i = 0; i < row.getColumnCount(); i++) {
                Value v = row.getValue(i);
                if (v.isLinked()) {
                    session.unlinkAtCommit((ValueLob) v);
                }
            }
        }
        int invalidateRowCount;
        // setChanged(session);
        if (rowCount == 1) {
            int todoMaybeImprove;
            removeAllRows();
        } else {
            int key = row.getPos();
            PageData root = getPage(headPos);
            root.remove(key);
            rowCount--;
            int todoReuseKeys;
//            if (key == lastKey - 1) {
//                lastKey--;
//            }
        }
        store.logAddOrRemoveRow(session, tableData.getId(), row, false);
    }

    public void remove(Session session) throws SQLException {
        if (trace.isDebugEnabled()) {
            trace.debug("remove");
        }
        store.removeMeta(this);
    }

    public void truncate(Session session) throws SQLException {
        if (trace.isDebugEnabled()) {
            trace.debug("truncate");
        }
        removeAllRows();
        if (tableData.getContainsLargeObject() && tableData.getPersistent()) {
            ValueLob.removeAllForTable(database, table.getId());
        }
        tableData.setRowCount(0);
    }

    private void removeAllRows() throws SQLException {
        store.removeRecord(headPos);
        int todoLogOldData;
        int freePages;
        PageDataLeaf root = new PageDataLeaf(this, headPos, Page.ROOT, store.createDataPage());
        store.updateRecord(root, true, null);
        rowCount = 0;
        lastKey = 0;
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException();
    }

    public Row getRow(Session session, int key) throws SQLException {
        PageData root = getPage(headPos);
        return root.getRow(session, key);
    }

    PageStore getPageStore() {
        return store;
    }

    /**
     * Read a row from the data page at the given position.
     *
     * @param data the data page
     * @return the row
     */
    Row readRow(DataPage data) throws SQLException {
        return tableData.readRow(data);
    }

    public long getRowCountApproximation() {
        return rowCount;
    }

    public long getRowCount(Session session) {
        return rowCount;
    }

    public String getCreateSQL() {
        return null;
    }

    public int getColumnIndex(Column col) {
        // the scan index cannot use any columns
        // TODO it can if there is an INT primary key
        return -1;
    }

    public void close(Session session) throws SQLException {
        if (trace.isDebugEnabled()) {
            trace.debug("close");
        }
        int todoWhyNotClose;
        // store = null;
        int writeRowCount;
    }

}
