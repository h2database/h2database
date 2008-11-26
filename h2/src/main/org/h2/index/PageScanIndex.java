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
import org.h2.store.DataPageBinary;
import org.h2.store.PageStore;
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
    
    // TODO remember last page with deleted keys (in the root page?), 
    // and chain such pages
    // TODO order pages so that searching for a key 
    // doesn't seek backwards in the file
    private int nextKey;
    
    // TODO remember the row count (in the root page?)
    
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
        if (headPos == Index.EMPTY_HEAD || headPos >= store.getPageCount()) {
            // new table
            headPos = store.allocatePage();
            PageDataLeaf root = new PageDataLeaf(this, headPos, Page.ROOT, store.createDataPage());
            root.write();
        } else {
            int todoRowCount;
            rowCount = getPage(headPos).getLastKey();
        }
        this.headPos = headPos;
        table.setRowCount(rowCount);
    }

    public void add(Session session, Row row) throws SQLException {
        row.setPos((int) rowCount);
        PageData root = getPage(headPos);
        int splitPoint = root.addRow(row);
        if (splitPoint != 0) {
            int pivot = root.getKey(splitPoint);
            PageData page1 = root;
            PageData page2 = root.split(splitPoint);
            int rootPageId = root.getPageId();
            int id = store.allocatePage();
            page1.setPageId(id);
            page1.setParentPageId(headPos);
            page2.setParentPageId(headPos);
            PageDataNode newRoot = new PageDataNode(this, rootPageId, Page.ROOT, store.createDataPage());
            newRoot.init(page1, pivot, page2);
            page1.write();
            page2.write();
            newRoot.write();
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
        DataPageBinary data = store.readPage(id);
        data.reset();
        int parentPageId = data.readInt();
        int type = data.readByte() & 255;
        PageData result;
        switch (type) {
        case Page.TYPE_DATA_LEAF:
        case Page.TYPE_DATA_LEAF_WITH_OVERFLOW:
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
        int writeRowCount;
        if (store != null) {
            store = null;
        }
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        PageData root = getPage(headPos);
        return root.find();
    }

    public Cursor findFirstOrLast(Session session, boolean first) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public double getCost(Session session, int[] masks) throws SQLException {
        long cost = 10 * tableData.getRowCount(session) + Constants.COST_ROW_OFFSET;
        return cost;
    }

    public boolean needRebuild() {
        return false;
    }

    public void remove(Session session, Row row) throws SQLException {
        int invalidateRowCount;
        // setChanged(session);
        if (rowCount == 1) {
            truncate(session);
        } else {
            PageData root = getPage(headPos);
            root.remove(row.getPos());
            rowCount--;
        }
    }

    public void remove(Session session) throws SQLException {
        int todo;
    }

    public void truncate(Session session) throws SQLException {
        int invalidateRowCount;
        PageDataLeaf root = new PageDataLeaf(this, headPos, Page.ROOT, store.createDataPage());
        root.write();
        rowCount = 0;
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
    Row readRow(DataPageBinary data) throws SQLException {
        return tableData.readRow(data);
    }

}
