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
import org.h2.store.Data;
import org.h2.store.DataPage;
import org.h2.store.PageStore;
import org.h2.store.Record;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueNull;

/**
 * This is the most common type of index, a b tree index.
 * Only the data of the indexed columns are stored in the index.
 */
public class PageBtreeIndex extends BaseIndex {

    private PageStore store;
    private TableData tableData;
    private final int headPos;
    private boolean needRebuild;
    private long rowCount;

    public PageBtreeIndex(TableData table, int id, String indexName, IndexColumn[] columns,
            IndexType indexType, int headPos, Session session) throws SQLException {
        initBaseIndex(table, id, indexName, columns, indexType);
        // trace.setLevel(TraceSystem.DEBUG);
        tableData = table;
        if (!database.isPersistent() || id < 0) {
            this.headPos = 0;
            throw Message.throwInternalError("" + indexName);
        }
        this.store = database.getPageStore();
        if (headPos == Index.EMPTY_HEAD) {
            // new index
            needRebuild = true;
            this.headPos = headPos = store.allocatePage();
            // TODO currently the head position is stored in the log
            // it should not for new tables, otherwise redo of other operations
            // must ensure this page is not used for other things
            store.addMeta(this, session, headPos);
            PageBtreeLeaf root = new PageBtreeLeaf(this, headPos, Page.ROOT, store.createData());
            store.updateRecord(root, true, root.data);
        } else {
            this.headPos = headPos;
            PageBtree root = getPage(headPos);
            rowCount = root.getRowCount();
            if (rowCount == 0 && store.isRecoveryRunning()) {
                needRebuild = true;
            }
            if (!database.isReadOnly()) {
                // could have been created before, but never committed
                // TODO test if really required
                store.updateRecord(root, false, null);
            }
        }
        if (trace.isDebugEnabled()) {
            trace.debug("opened " + getName() +" rows:"+ rowCount);
        }
    }

    public int getHeadPos() {
        return headPos;
    }

    public void add(Session session, Row row) throws SQLException {
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
        // safe memory
        SearchRow newRow = getSearchRow(row);
        while (true) {
            PageBtree root = getPage(headPos);
            int splitPoint = root.addRowTry(newRow);
            if (splitPoint == 0) {
                break;
            }
            if (trace.isDebugEnabled()) {
                trace.debug("split " + splitPoint);
            }
            SearchRow pivot = root.getRow(splitPoint - 1);
            PageBtree page1 = root;
            PageBtree page2 = root.split(splitPoint);
            int rootPageId = root.getPos();
            int id = store.allocatePage();
            page1.setPageId(id);
            page1.setParentPageId(headPos);
            page2.setParentPageId(headPos);
            PageBtreeNode newRoot = new PageBtreeNode(this, rootPageId, Page.ROOT, store.createData());
            newRoot.init(page1, pivot, page2);
            store.updateRecord(page1, true, page1.data);
            store.updateRecord(page2, true, page2.data);
            store.updateRecord(newRoot, true, null);
            root = newRoot;
        }
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
        r.setPosAndVersion(row);
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
    PageBtree getPage(int id) throws SQLException {
        Record rec = store.getRecord(id);
        if (rec != null) {
            if (SysProperties.CHECK) {
                if (!(rec instanceof PageBtree)) {
                    throw Message.throwInternalError("Wrong page: " + rec + " " + this);
                }
                PageBtree result = (PageBtree) rec;
                if (result.index.headPos != this.headPos) {
                    throw Message.throwInternalError("Wrong index: " + result.index + " " + this);
                }
            }
            return (PageBtree) rec;
        }
        Data data = store.readPage(id);
        data.reset();
        int parentPageId = data.readInt();
        int type = data.readByte() & 255;
        PageBtree result;
        switch (type & ~Page.FLAG_LAST) {
        case Page.TYPE_BTREE_LEAF:
            result = new PageBtreeLeaf(this, id, parentPageId, data);
            break;
        case Page.TYPE_BTREE_NODE:
            result = new PageBtreeNode(this, id, parentPageId, data);
            break;
        case Page.TYPE_EMPTY:
            PageBtreeLeaf empty = new PageBtreeLeaf(this, id, parentPageId, data);
            return empty;
        default:
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, "page=" + id + " type=" + type);
        }
        result.read();
        return result;
    }

    public boolean canGetFirstOrLast() {
        return true;
    }

    public Cursor findNext(Session session, SearchRow first, SearchRow last) throws SQLException {
        return find(session, first, true, last);
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        return find(session, first, false, last);
    }

    private Cursor find(Session session, SearchRow first, boolean bigger, SearchRow last) throws SQLException {
        if (SysProperties.CHECK && store == null) {
            throw Message.getSQLException(ErrorCode.OBJECT_CLOSED);
        }
        PageBtree root = getPage(headPos);
        PageBtreeCursor cursor = new PageBtreeCursor(session, this, last);
        root.find(cursor, first, bigger);
        return cursor;
    }

    public Cursor findFirstOrLast(Session session, boolean first) throws SQLException {
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
        PageBtree root = getPage(headPos);
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
        // TODO invalidate row count
        // setChanged(session);
        if (rowCount == 1) {
            removeAllRows();
        } else {
            PageBtree root = getPage(headPos);
            root.remove(row);
            rowCount--;
        }
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
        if (tableData.getContainsLargeObject()) {
            ValueLob.removeAllForTable(database, table.getId());
        }
        tableData.setRowCount(0);
    }

    private void removeAllRows() throws SQLException {
        PageBtree root = getPage(headPos);
        root.freeChildren();
        root = new PageBtreeLeaf(this, headPos, Page.ROOT, store.createData());
        store.removeRecord(headPos);
        store.updateRecord(root, true, null);
        rowCount = 0;
    }

    public void checkRename() {
        // ok
    }

    /**
     * Get a row from the data file.
     *
     * @param session the session
     * @param key the row key
     * @return the row
     */
    Row getRow(Session session, int key) throws SQLException {
        return tableData.getRow(session, key);
    }

    PageStore getPageStore() {
        return store;
    }

    public long getRowCountApproximation() {
        return tableData.getRowCountApproximation();
    }

    public long getRowCount(Session session) {
        return tableData.getRowCount(session);
    }

    public void close(Session session) {
        if (trace.isDebugEnabled()) {
            trace.debug("close");
        }
        // TODO write the row count
    }

    /**
     * Read a row from the data page at the given offset.
     *
     * @param data the data
     * @param offset the offset
     * @param onlyPosition whether only the position of the row is stored
     * @return the row
     */
    SearchRow readRow(Data data, int offset, boolean onlyPosition) throws SQLException {
        data.setPos(offset);
        int pos = data.readInt();
        if (onlyPosition) {
            return tableData.getRow(null, pos);
        }
        SearchRow row = table.getTemplateSimpleRow(columns.length == 1);
        row.setPos(pos);
        for (Column col : columns) {
            int idx = col.getColumnId();
            row.setValue(idx, data.readValue());
        }
        return row;
    }

    /**
     * Write a row to the data page at the given offset.
     *
     * @param data the data
     * @param offset the offset
     * @param onlyPosition whether only the position of the row is stored
     * @param row the row to write
     */
    void writeRow(Data data, int offset, SearchRow row, boolean onlyPosition) throws SQLException {
        data.setPos(offset);
        data.writeInt(row.getPos());
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
    int getRowSize(Data dummy, SearchRow row, boolean onlyPosition) throws SQLException {
        int rowsize = DataPage.LENGTH_INT;
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

}
