/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
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
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.DataPage;
import org.h2.store.PageStore;
import org.h2.store.Record;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;

/**
 * The scan index allows to access a row by key. It can be used to iterate over
 * all rows of a table. Each regular table has one such object, even if no
 * primary key or indexes are defined.
 */
public class PageScanIndex extends BaseIndex implements RowIndex {

    private PageStore store;
    private TableData tableData;
    private int headPos;

    // TODO test that setPageId updates parent, overflow parent
    // TODO remember last page with deleted keys (in the root page?),
    // and chain such pages
    // TODO order pages so that searching for a key
    // doesn't seek backwards in the file
    // TODO use an undo log and maybe redo log (for performance)
    // TODO file position, content checksums
    // TODO completely re-use keys of deleted rows
    private int lastKey;
    private long rowCount;
    private long rowCountApproximation;

    public PageScanIndex(TableData table, int id, IndexColumn[] columns, IndexType indexType, int headPos) throws SQLException {
        initBaseIndex(table, id, table.getName() + "_TABLE_SCAN", columns, indexType);
        if (database.isMultiVersion()) {
            int todoMvcc;
        }
        tableData = table;
        if (!database.isPersistent() || id < 0) {
            int todo;
            return;
        }
        this.store = database.getPageStorage();
        if (headPos == Index.EMPTY_HEAD || store.isNew()) {
            // new table, or the system table for a new database
            headPos = store.allocatePage();
            PageDataLeaf root = new PageDataLeaf(this, headPos, Page.ROOT, store.createDataPage());
            store.updateRecord(root, root.data);
        } else {
            lastKey = getPage(headPos).getLastKey();
            rowCount = getPage(headPos).getRowCount();
            int reuseKeysIfManyDeleted;
        }
        this.headPos = headPos;
        trace("open " + rowCount);
        table.setRowCount(rowCount);
    }

    public int getHeadPos() {
        return headPos;
    }

    public void add(Session session, Row row) throws SQLException {
        row.setPos(++lastKey);
        trace("add " + row.getPos());
        while (true) {
            PageData root = getPage(headPos);
            int splitPoint = root.addRow(row);
            if (splitPoint == 0) {
                break;
            }
            trace("split " + splitPoint);
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
            store.updateRecord(page1, page1.data);
            store.updateRecord(page2, page2.data);
            store.updateRecord(newRoot, null);
            root = newRoot;
        }
        rowCount++;
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
        default:
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, "page=" + id + " type=" + type);
        }
        result.read();
        return result;
    }

    public boolean canGetFirstOrLast() {
        return false;
    }

    public void close(Session session) throws SQLException {
        trace("close");
        int writeRowCount;
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
        trace("remove " + row.getPos());
        int invalidateRowCount;
        // setChanged(session);
        if (rowCount == 1) {
            truncate(session);
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
    }

    public void remove(Session session) throws SQLException {
        trace("remove");
        int todo;
    }

    public void truncate(Session session) throws SQLException {
        trace("truncate");
        store.removeRecord(headPos);
        int todoLogOldData;
        int freePages;
        PageDataLeaf root = new PageDataLeaf(this, headPos, Page.ROOT, store.createDataPage());
        store.updateRecord(root, null);
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
        return rowCountApproximation;
    }

    public long getRowCount(Session session) {
        return rowCount;
    }

    public String getCreateSQL() {
        return null;
    }

    private void trace(String message) {
        if (headPos != 1) {
            int test;
//            System.out.println(message);
        }
    }

    public int getColumnIndex(Column col) {
        // the scan index cannot use any columns
        // TODO it can if there is an INT primary key
        return -1;
    }

}
