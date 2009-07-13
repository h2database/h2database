/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.log.UndoLogRecord;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.store.PageStore;
import org.h2.store.Record;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.util.New;
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
    private HashSet<Row> delta;
    private int rowCountDiff;
    private HashMap<Integer, Integer> sessionRowCount;

    public PageScanIndex(TableData table, int id, IndexColumn[] columns, IndexType indexType, int headPos, Session session) throws SQLException {
        initBaseIndex(table, id, table.getName() + "_TABLE_SCAN", columns, indexType);
        // trace.setLevel(TraceSystem.DEBUG);
        if (database.isMultiVersion()) {
            sessionRowCount = New.hashMap();
            isMultiVersion = true;
        }
        tableData = table;
        this.store = database.getPageStore();
        if (!database.isPersistent()) {
            this.headPos = 0;
            throw Message.throwInternalError(table.getName());
        }
        if (headPos == Index.EMPTY_HEAD) {
            // new table
            this.headPos = headPos = store.allocatePage();
            // TODO currently the head position is stored in the log
            // it should not for new tables, otherwise redo of other operations
            // must ensure this page is not used for other things
            store.addMeta(this, session, headPos);
            PageDataLeaf root = new PageDataLeaf(this, headPos, Page.ROOT, store.createData());
            store.updateRecord(root, true, root.data);
        } else {
            this.headPos = headPos;
            PageData root = getPage(headPos, 0);
            lastKey = root.getLastKey();
            rowCount = root.getRowCount();
            // could have been created before, but never committed
            if (!database.isReadOnly()) {
                // TODO check if really required
                store.updateRecord(root, false, null);
            }
            // TODO re-use keys after many rows are deleted
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
        } else {
            lastKey = Math.max(lastKey, row.getPos() + 1);
        }
        if (trace.isDebugEnabled()) {
            trace.debug("add table:" + table.getId() + " " + row);
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
            PageData root = getPage(headPos, 0);
            int splitPoint = root.addRowTry(row);
            if (splitPoint == 0) {
                break;
            }
            if (trace.isDebugEnabled()) {
                trace.debug("split " + splitPoint);
            }
            int pivot = root.getKey(splitPoint - 1);
            PageData page1 = root;
            PageData page2 = root.split(splitPoint);
            int rootPageId = root.getPos();
            int id = store.allocatePage();
            page1.setPageId(id);
            page1.setParentPageId(headPos);
            page2.setParentPageId(headPos);
            PageDataNode newRoot = new PageDataNode(this, rootPageId, Page.ROOT, store.createData());
            newRoot.init(page1, pivot, page2);
            store.updateRecord(page1, true, page1.data);
            store.updateRecord(page2, true, page2.data);
            store.updateRecord(newRoot, true, null);
            root = newRoot;
        }
        row.setDeleted(false);
        if (database.isMultiVersion()) {
            if (delta == null) {
                delta = New.hashSet();
            }
            boolean wasDeleted = delta.remove(row);
            if (!wasDeleted) {
                delta.add(row);
            }
            incrementRowCount(session.getId(), 1);
        }
        rowCount++;
        store.logAddOrRemoveRow(session, tableData.getId(), row, true);
    }

    /**
     * Read an overflow page page.
     *
     * @param id the page id
     * @param leaf the leaf page
     * @param offset the offset in bytes
     * @return the page
     */
    PageDataLeafOverflow getPageOverflow(int id, PageDataLeaf leaf, int offset) throws SQLException {
        Record rec = store.getRecord(id);
        if (rec != null) {
            return (PageDataLeafOverflow) rec;
        }
        Data data = store.readPage(id);
        data.reset();
        PageDataLeafOverflow result = new PageDataLeafOverflow(leaf, id, data, offset);
        result.read();
        return result;
    }

    /**
     * Read the given page.
     *
     * @param id the page id
     * @param parent the parent, or -1 if unknown
     * @return the page
     */
    PageData getPage(int id, int parent) throws SQLException {
        Record rec = store.getRecord(id);
        if (rec != null) {
            if (SysProperties.CHECK) {
                PageData result = (PageData) rec;
                if (result.index.headPos != this.headPos) {
                    throw Message.throwInternalError("Wrong index: " + result.index + " " + this);
                }
            }
            return (PageData) rec;
        }
        Data data = store.readPage(id);
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
        if (parent != -1) {
            if (result.getParentPageId() != parent) {
                throw Message.throwInternalError(result.getParentPageId() + " " + parent + " " + result);
            }
        }
        return result;
    }

    public boolean canGetFirstOrLast() {
        return false;
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        PageData root = getPage(headPos, 0);
        return root.find(session);
    }

    public Cursor findFirstOrLast(Session session, boolean first) throws SQLException {
        throw Message.getUnsupportedException("PAGE");
    }

    public double getCost(Session session, int[] masks) {
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
        // TODO invalidate the row count
        // setChanged(session);
        if (rowCount == 1) {
            removeAllRows();
        } else {
            int key = row.getPos();
            PageData root = getPage(headPos, 0);
            root.remove(key);
            rowCount--;
            // TODO re-use keys
        }
        if (database.isMultiVersion()) {
            // if storage is null, the delete flag is not yet set
            row.setDeleted(true);
            if (delta == null) {
                delta = New.hashSet();
            }
            boolean wasAdded = delta.remove(row);
            if (!wasAdded) {
                delta.add(row);
            }
            incrementRowCount(session.getId(), -1);
        }
        store.logAddOrRemoveRow(session, tableData.getId(), row, false);
    }

    public void remove(Session session) throws SQLException {
        if (trace.isDebugEnabled()) {
            trace.debug("remove");
        }
        removeAllRows();
        store.freePage(headPos, false, null);
        store.removeMeta(this, session);
    }

    public void truncate(Session session) throws SQLException {
        if (trace.isDebugEnabled()) {
            trace.debug("truncate");
        }
        removeAllRows();
        if (tableData.getContainsLargeObject() && tableData.isPersistData()) {
            ValueLob.removeAllForTable(database, table.getId());
        }
        if (database.isMultiVersion()) {
            sessionRowCount.clear();
        }
        tableData.setRowCount(0);
    }

    private void removeAllRows() throws SQLException {
        PageData root = getPage(headPos, 0);
        root.freeChildren();
        root = new PageDataLeaf(this, headPos, Page.ROOT, store.createData());
        store.removeRecord(headPos);
        store.updateRecord(root, true, null);
        rowCount = 0;
        lastKey = 0;
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException("PAGE");
    }

    public Row getRow(Session session, int key) throws SQLException {
        PageData root = getPage(headPos, 0);
        return root.getRow(key);
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
    Row readRow(Data data) throws SQLException {
        return tableData.readRow(data);
    }

    public long getRowCountApproximation() {
        return rowCount;
    }

    public long getRowCount(Session session) {
        if (database.isMultiVersion()) {
            Integer i = sessionRowCount.get(session.getId());
            long count = i == null ? 0 : i.intValue();
            count += rowCount;
            count -= rowCountDiff;
            return count;
        }
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

    public void close(Session session) {
        if (trace.isDebugEnabled()) {
            trace.debug("close");
        }
        if (delta != null) {
            delta.clear();
        }
        rowCountDiff = 0;
        if (sessionRowCount != null) {
            sessionRowCount.clear();
        }
        // can not close the index because it might get used afterwards,
        // for example after running recovery
        // TODO write the row count
    }

    Iterator<Row> getDelta() {
        if (delta == null) {
            List<Row> e = Collections.emptyList();
            return e.iterator();
        }
        return delta.iterator();
    }

    private void incrementRowCount(int sessionId, int count) {
        if (database.isMultiVersion()) {
            Integer id = sessionId;
            Integer c = sessionRowCount.get(id);
            int current = c == null ? 0 : c.intValue();
            sessionRowCount.put(id, current + count);
            rowCountDiff += count;
        }
    }

    public void commit(int operation, Row row) {
        if (database.isMultiVersion()) {
            if (delta != null) {
                delta.remove(row);
            }
            incrementRowCount(row.getSessionId(), operation == UndoLogRecord.DELETE ? 1 : -1);
        }
    }

}
