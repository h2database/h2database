/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Session;
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
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This is the most common type of index, a b tree index.
 * Only the data of the indexed columns are stored in the index.
 */
public class PageBtreeIndex extends PageIndex {

    private PageStore store;
    private RegularTable tableData;
    private boolean needRebuild;
    private long rowCount;

    public PageBtreeIndex(RegularTable table, int id, String indexName, IndexColumn[] columns,
            IndexType indexType, boolean create, Session session) {
        initBaseIndex(table, id, indexName, columns, indexType);
        // int test;
        // trace.setLevel(TraceSystem.DEBUG);
        tableData = table;
        if (!database.isPersistent() || id < 0) {
            throw DbException.throwInternalError("" + indexName);
        }
        this.store = database.getPageStore();
        store.addIndex(this);
        if (create) {
            // new index
            rootPageId = store.allocatePage();
            needRebuild = true;
            // TODO currently the head position is stored in the log
            // it should not for new tables, otherwise redo of other operations
            // must ensure this page is not used for other things
            store.addMeta(this, session);
            PageBtreeLeaf root = PageBtreeLeaf.create(this, rootPageId, PageBtree.ROOT);
            store.logUndo(root, null);
            store.update(root);
        } else {
            rootPageId = store.getRootPageId(id);
            PageBtree root = getPage(rootPageId);
            rowCount = root.getRowCount();
            if (rowCount == 0 && store.isRecoveryRunning()) {
                needRebuild = true;
            }
        }
        if (trace.isDebugEnabled()) {
            trace.debug("opened " + getName() +" rows:"+ rowCount);
        }
    }

    public void add(Session session, Row row) {
        if (trace.isDebugEnabled()) {
            trace.debug(getName() + " add " + row);
        }
        // safe memory
        SearchRow newRow = getSearchRow(row);
        try {
            addRow(newRow);
        } finally {
            store.incrementChangeCount();
        }
    }

    private void addRow(SearchRow newRow) {
        while (true) {
            PageBtree root = getPage(rootPageId);
            int splitPoint = root.addRowTry(newRow);
            if (splitPoint == -1) {
                break;
            }
            if (trace.isDebugEnabled()) {
                trace.debug("split " + splitPoint);
            }
            SearchRow pivot = root.getRow(splitPoint - 1);
            store.logUndo(root, root.data);
            PageBtree page1 = root;
            PageBtree page2 = root.split(splitPoint);
            store.logUndo(page2, null);
            int id = store.allocatePage();
            page1.setPageId(id);
            page1.setParentPageId(rootPageId);
            page2.setParentPageId(rootPageId);
            PageBtreeNode newRoot = PageBtreeNode.create(this, rootPageId, PageBtree.ROOT);
            store.logUndo(newRoot, null);
            newRoot.init(page1, pivot, page2);
            store.update(page1);
            store.update(page2);
            store.update(newRoot);
            root = newRoot;
        }
        invalidateRowCount();
        rowCount++;
    }

    /**
     * Create a search row for this row.
     *
     * @param row the row
     * @return the search row
     */
    private SearchRow getSearchRow(Row row) {
        SearchRow r = table.getTemplateSimpleRow(columns.length == 1);
        r.setKeyAndVersion(row);
        for (int j = 0; j < columns.length; j++) {
            int idx = columns[j].getColumnId();
            r.setValue(idx, row.getValue(idx));
        }
        return r;
    }

    /**
     * Read the given page.
     *
     * @param id the page id
     * @return the page
     */
    PageBtree getPage(int id) {
        Page p = store.getPage(id);
        if (p == null) {
            PageBtreeLeaf empty = PageBtreeLeaf.create(this, id, PageBtree.ROOT);
            // could have been created before, but never committed
            store.logUndo(empty, null);
            store.update(empty);
            return empty;
        } else if (!(p instanceof PageBtree)) {
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "" + p);
        }
        return (PageBtree) p;
    }

    public boolean canGetFirstOrLast() {
        return true;
    }

    public Cursor findNext(Session session, SearchRow first, SearchRow last) {
        return find(session, first, true, last);
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return find(session, first, false, last);
    }

    private Cursor find(Session session, SearchRow first, boolean bigger, SearchRow last) {
        if (SysProperties.CHECK && store == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
        PageBtree root = getPage(rootPageId);
        PageBtreeCursor cursor = new PageBtreeCursor(session, this, last);
        root.find(cursor, first, bigger);
        return cursor;
    }

    public Cursor findFirstOrLast(Session session, boolean first) {
        if (first) {
            // TODO optimization: this loops through NULL elements
            Cursor cursor = find(session, null, false, null);
            while (cursor.next()) {
                SearchRow row = cursor.getSearchRow();
                Value v = row.getValue(columnIds[0]);
                if (v != ValueNull.INSTANCE) {
                    return cursor;
                }
            }
            return cursor;
        }
        PageBtree root = getPage(rootPageId);
        PageBtreeCursor cursor = new PageBtreeCursor(session, this, null);
        root.last(cursor);
        cursor.previous();
        // TODO optimization: this loops through NULL elements
        do {
            SearchRow row = cursor.getSearchRow();
            if (row == null) {
                break;
            }
            Value v = row.getValue(columnIds[0]);
            if (v != ValueNull.INSTANCE) {
                return cursor;
            }
        } while (cursor.previous());
        return cursor;
    }

    public double getCost(Session session, int[] masks) {
        return 10 * getCostRangeIndex(masks, tableData.getRowCount(session));
    }

    public boolean needRebuild() {
        return needRebuild;
    }

    public void remove(Session session, Row row) {
        if (trace.isDebugEnabled()) {
            trace.debug(getName() + " remove " + row);
        }
        if (tableData.getContainsLargeObject()) {
            for (int i = 0; i < row.getColumnCount(); i++) {
                Value v = row.getValue(i);
                if (v.isLinked()) {
                    session.unlinkAtCommit(v);
                }
            }
        }
        // TODO invalidate row count
        // setChanged(session);
        if (rowCount == 1) {
            removeAllRows();
        } else {
            try {
                PageBtree root = getPage(rootPageId);
                root.remove(row);
                invalidateRowCount();
                rowCount--;
            } finally {
                store.incrementChangeCount();
            }
        }
    }

    public void remove(Session session) {
        if (trace.isDebugEnabled()) {
            trace.debug("remove");
        }
        removeAllRows();
        store.free(rootPageId);
        store.removeMeta(this, session);
    }

    public void truncate(Session session) {
        if (trace.isDebugEnabled()) {
            trace.debug("truncate");
        }
        removeAllRows();
        if (tableData.getContainsLargeObject()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
        tableData.setRowCount(0);
    }

    private void removeAllRows() {
        try {
            PageBtree root = getPage(rootPageId);
            root.freeRecursive();
            root = PageBtreeLeaf.create(this, rootPageId, PageBtree.ROOT);
            store.removeRecord(rootPageId);
            store.update(root);
            rowCount = 0;
        } finally {
            store.incrementChangeCount();
        }
    }

    public void checkRename() {
        // ok
    }

    /**
     * Get a row from the main index.
     *
     * @param session the session
     * @param key the row key
     * @return the row
     */
    public Row getRow(Session session, long key) {
        return tableData.getRow(session, key);
    }

    PageStore getPageStore() {
        return store;
    }

    public long getRowCountApproximation() {
        return tableData.getRowCountApproximation();
    }

    public long getRowCount(Session session) {
        return rowCount;
    }

    public void close(Session session) {
        if (trace.isDebugEnabled()) {
            trace.debug("close");
        }
        // can not close the index because it might get used afterwards,
        // for example after running recovery
        try {
            writeRowCount();
        } finally {
            store.incrementChangeCount();
        }
    }

    /**
     * Read a row from the data page at the given offset.
     *
     * @param data the data
     * @param offset the offset
     * @param onlyPosition whether only the position of the row is stored
     * @param needData whether the row data is required
     * @return the row
     */
    SearchRow readRow(Data data, int offset, boolean onlyPosition, boolean needData) {
        data.setPos(offset);
        long key = data.readVarLong();
        if (onlyPosition) {
            if (needData) {
                return tableData.getRow(null, key);
            }
            SearchRow row = table.getTemplateSimpleRow(true);
            row.setKey(key);
            return row;
        }
        SearchRow row = table.getTemplateSimpleRow(columns.length == 1);
        row.setKey(key);
        for (Column col : columns) {
            int idx = col.getColumnId();
            row.setValue(idx, data.readValue());
        }
        return row;
    }

    /**
     * Get the complete row from the data index.
     *
     * @param key the key
     * @return the row
     */
    SearchRow readRow(long key) {
        return tableData.getRow(null, key);
    }

    /**
     * Write a row to the data page at the given offset.
     *
     * @param data the data
     * @param offset the offset
     * @param onlyPosition whether only the position of the row is stored
     * @param row the row to write
     */
    void writeRow(Data data, int offset, SearchRow row, boolean onlyPosition) {
        data.setPos(offset);
        data.writeVarLong(row.getKey());
        if (!onlyPosition) {
            for (Column col : columns) {
                int idx = col.getColumnId();
                data.writeValue(row.getValue(idx));
            }
        }
    }

    /**
     * Get the size of a row (only the part that is stored in the index).
     *
     * @param dummy a dummy data page to calculate the size
     * @param row the row
     * @param onlyPosition whether only the position of the row is stored
     * @return the number of bytes
     */
    int getRowSize(Data dummy, SearchRow row, boolean onlyPosition) {
        int rowsize = Data.getVarLongLen(row.getKey());
        if (!onlyPosition) {
            for (Column col : columns) {
                Value v = row.getValue(col.getColumnId());
                rowsize += dummy.getValueLen(v);
            }
        }
        return rowsize;
    }

    public boolean canFindNext() {
        return true;
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

    private void invalidateRowCount() {
        PageBtree root = getPage(rootPageId);
        root.setRowCountStored(PageData.UNKNOWN_ROWCOUNT);
    }

    public void writeRowCount() {
        PageBtree root = getPage(rootPageId);
        root.setRowCountStored(MathUtils.convertLongToInt(rowCount));
    }

    /**
     * Check whether the given row contains data.
     *
     * @param row the row
     * @return true if it contains data
     */
    boolean hasData(SearchRow row) {
        return row.getValue(columns[0].getColumnId()) != null;
    }

}
