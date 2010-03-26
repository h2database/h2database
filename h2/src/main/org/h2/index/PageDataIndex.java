/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.engine.UndoLogRecord;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.store.Page;
import org.h2.store.PageStore;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * The scan index allows to access a row by key. It can be used to iterate over
 * all rows of a table. Each regular table has one such object, even if no
 * primary key or indexes are defined.
 */
public class PageDataIndex extends PageIndex {

    private PageStore store;
    private RegularTable tableData;
    private long lastKey;
    private long rowCount;
    private HashSet<Row> delta;
    private int rowCountDiff;
    private HashMap<Integer, Integer> sessionRowCount;
    private int mainIndexColumn = -1;
    private DbException fastDuplicateKeyException;
    private int memorySizePerPage;

    public PageDataIndex(RegularTable table, int id, IndexColumn[] columns, IndexType indexType, boolean create, Session session) {
        initBaseIndex(table, id, table.getName() + "_DATA", columns, indexType);

        // trace = database.getTrace(Trace.PAGE_STORE + "_di");
        // trace.setLevel(TraceSystem.DEBUG);
        if (database.isMultiVersion()) {
            sessionRowCount = New.hashMap();
            isMultiVersion = true;
        }
        tableData = table;
        this.store = database.getPageStore();
        store.addIndex(this);
        if (!database.isPersistent()) {
            throw DbException.throwInternalError(table.getName());
        }
        if (create) {
            rootPageId = store.allocatePage();
            store.addMeta(this, session);
            PageDataLeaf root = PageDataLeaf.create(this, rootPageId, PageData.ROOT);
            store.update(root);
        } else {
            rootPageId = store.getRootPageId(id);
            PageData root = getPage(rootPageId, 0);
            lastKey = root.getLastKey();
            rowCount = root.getRowCount();
        }
        if (trace.isDebugEnabled()) {
            trace.debug(this + " opened rows:" + rowCount);
        }
        table.setRowCount(rowCount);
        // estimate the memory usage as follows:
        // the less column, the more memory is required,
        // because the more rows fit on a page
        memorySizePerPage = store.getPageSize();
        int estimatedRowsPerPage =  store.getPageSize() / ((1 + columns.length) * 8);
        memorySizePerPage += estimatedRowsPerPage * 64;
    }

    public DbException getDuplicateKeyException() {
        if (fastDuplicateKeyException == null) {
            fastDuplicateKeyException = super.getDuplicateKeyException();
        }
        return fastDuplicateKeyException;
    }

    public void add(Session session, Row row) {
        boolean retry = false;
        if (mainIndexColumn != -1) {
            row.setKey(row.getValue(mainIndexColumn).getLong());
        } else {
            if (row.getKey() == 0) {
                row.setKey((int) ++lastKey);
                retry = true;
            }
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
        // when using auto-generated values, it's possible that multiple
        // tries are required (specially if there was originally a primary key)
        if (trace.isDebugEnabled()) {
            trace.debug(getName() + " add " + row);
        }
        long add = 0;
        while (true) {
            try {
                addTry(session, row);
                break;
            } catch (DbException e) {
                if (e != fastDuplicateKeyException) {
                    throw e;
                }
                if (!retry) {
                    throw super.getDuplicateKeyException();
                }
                if (add == 0) {
                    // in the first re-try add a small random number,
                    // to avoid collisions after a re-start
                    row.setKey((long) (row.getKey() + Math.random() * 10000));
                } else {
                    row.setKey(row.getKey() + add);
                }
                add++;
            } finally {
                store.incrementChangeCount();
            }
        }
        lastKey = Math.max(lastKey, row.getKey() + 1);
    }

    private void addTry(Session session, Row row) {
        while (true) {
            PageData root = getPage(rootPageId, 0);
            int splitPoint = root.addRowTry(row);
            if (splitPoint == -1) {
                break;
            }
            if (trace.isDebugEnabled()) {
                trace.debug(this + " split");
            }
            long pivot = splitPoint == 0 ? row.getKey() : root.getKey(splitPoint - 1);
            PageData page1 = root;
            PageData page2 = root.split(splitPoint);
            int id = store.allocatePage();
            page1.setPageId(id);
            page1.setParentPageId(rootPageId);
            page2.setParentPageId(rootPageId);
            PageDataNode newRoot = PageDataNode.create(this, rootPageId, PageData.ROOT);
            newRoot.init(page1, pivot, page2);
            store.update(page1);
            store.update(page2);
            store.update(newRoot);
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
        invalidateRowCount();
        rowCount++;
        store.logAddOrRemoveRow(session, tableData.getId(), row, true);
    }

    /**
     * Read an overflow page page.
     *
     * @param id the page id
     * @return the page
     */
    PageDataOverflow getPageOverflow(int id) {
        Page p = store.getPage(id);
        if (p instanceof PageDataOverflow) {
            return (PageDataOverflow) p;
        }
        throw DbException.get(ErrorCode.FILE_CORRUPTED_1, p.toString());
    }

    /**
     * Read the given page.
     *
     * @param id the page id
     * @param parent the parent, or -1 if unknown
     * @return the page
     */
    PageData getPage(int id, int parent) {
        Page pd = store.getPage(id);
        if (pd == null) {
            PageDataLeaf empty = PageDataLeaf.create(this, id, parent);
            // could have been created before, but never committed
            store.logUndo(empty, null);
            store.update(empty);
            return empty;
        } else if (!(pd instanceof PageData)) {
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "" + pd);
        }
        PageData p = (PageData) pd;
        if (parent != -1) {
            if (p.getParentPageId() != parent) {
                throw DbException.throwInternalError(p + " parent " + p.getParentPageId() + " expected " + parent);
            }
        }
        return p;
    }

    public boolean canGetFirstOrLast() {
        return false;
    }

    /**
     * Get the key from the row
     *
     * @param row the row
     * @param ifEmpty the value to use if the row is empty
     * @return the key
     */
    long getLong(SearchRow row, long ifEmpty) {
        if (row == null) {
            return ifEmpty;
        }
        Value v = row.getValue(mainIndexColumn);
        if (v == null || v == ValueNull.INSTANCE) {
            return ifEmpty;
        }
        return v.getLong();
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) {
        if (first != null || last != null) {
            // this index is a table scan, must not use it for lookup
            throw DbException.throwInternalError(getSQL() + " " + first + " " + last);
        }
        PageData root = getPage(rootPageId, 0);
        return root.find(session, Long.MIN_VALUE, Long.MAX_VALUE, isMultiVersion);
    }

    /**
     * Search for a specific row or a set of rows.
     *
     * @param session the session
     * @param first the key of the first row
     * @param last the key of the last row
     * @param multiVersion if mvcc should be used
     * @return the cursor
     */
    Cursor find(Session session, long first, long last, boolean multiVersion) {
        PageData root = getPage(rootPageId, 0);
        return root.find(session, first, last, multiVersion);
    }

    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.throwInternalError();
    }

    long getLastKey() {
        PageData root = getPage(rootPageId, 0);
        return root.getLastKey();
    }

    public double getCost(Session session, int[] masks) {
        long cost = 10 * (tableData.getRowCountApproximation() + Constants.COST_ROW_OFFSET);
        return cost;
    }

    public boolean needRebuild() {
        return false;
    }

    public void remove(Session session, Row row) {
        if (tableData.getContainsLargeObject()) {
            for (int i = 0; i < row.getColumnCount(); i++) {
                Value v = row.getValue(i);
                if (v.isLinked()) {
                    session.unlinkAtCommit(v);
                }
            }
        }
        if (trace.isDebugEnabled()) {
            trace.debug(getName() + " remove " + row);
        }
        if (rowCount == 1) {
            removeAllRows();
        } else {
            try {
                long key = row.getKey();
                PageData root = getPage(rootPageId, 0);
                root.remove(key);
                invalidateRowCount();
                rowCount--;
            } finally {
                store.incrementChangeCount();
            }
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

    public void remove(Session session) {
        if (trace.isDebugEnabled()) {
            trace.debug(this + " remove");
        }
        removeAllRows();
        store.free(rootPageId);
        store.removeMeta(this, session);
    }

    public void truncate(Session session) {
        if (trace.isDebugEnabled()) {
            trace.debug(this + " truncate");
        }
        store.logTruncate(session, tableData.getId());
        removeAllRows();
        if (tableData.getContainsLargeObject() && tableData.isPersistData()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
        if (database.isMultiVersion()) {
            sessionRowCount.clear();
        }
        tableData.setRowCount(0);
    }

    private void removeAllRows() {
        try {
            PageData root = getPage(rootPageId, 0);
            root.freeRecursive();
            root = PageDataLeaf.create(this, rootPageId, PageData.ROOT);
            store.removeRecord(rootPageId);
            store.update(root);
            rowCount = 0;
            lastKey = 0;
        } finally {
            store.incrementChangeCount();
        }
    }

    public void checkRename() {
        throw DbException.getUnsupportedException("PAGE");
    }

    public Row getRow(Session session, long key) {
        return getRow(key);
    }

    /**
     * Get the row with the given key.
     *
     * @param key the key
     * @return the row
     */
    public Row getRow(long key) {
        PageData root = getPage(rootPageId, 0);
        return root.getRow(key);
    }

    PageStore getPageStore() {
        return store;
    }

    /**
     * Read a row from the data page at the given position.
     *
     * @param data the data page
     * @param columnCount the number of columns
     * @return the row
     */
    Row readRow(Data data, int columnCount) {
        Value[] values = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            values[i] = data.readValue();
        }
        return tableData.createRow(values);
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
        // can not use this index - use the PageDelegateIndex instead
        return -1;
    }

    public void close(Session session) {
        if (trace.isDebugEnabled()) {
            trace.debug(this + " close");
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
        writeRowCount();
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

    /**
     * The root page has changed.
     *
     * @param session the session
     * @param newPos the new position
     */
    void setRootPageId(Session session, int newPos) {
        store.removeMeta(this, session);
        this.rootPageId = newPos;
        store.addMeta(this, session);
        store.addIndex(this);
    }

    public void setMainIndexColumn(int mainIndexColumn) {
        this.mainIndexColumn = mainIndexColumn;
    }

    public int getMainIndexColumn() {
        return mainIndexColumn;
    }

    int getMemorySizePerPage() {
        return memorySizePerPage;
    }

    public String toString() {
        return getName();
    }

    private void invalidateRowCount() {
        PageData root = getPage(rootPageId, 0);
        root.setRowCountStored(PageData.UNKNOWN_ROWCOUNT);
    }

    public void writeRowCount() {
        try {
            PageData root = getPage(rootPageId, 0);
            root.setRowCountStored(MathUtils.convertLongToInt(rowCount));
        } finally {
            store.incrementChangeCount();
        }
    }

    public String getPlanSQL() {
        return table.getSQL() + ".tableScan";
    }

}
