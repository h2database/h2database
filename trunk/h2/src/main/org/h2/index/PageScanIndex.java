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
            PageDataLeaf root = new PageDataLeaf(this, headPos, 1, store.createDataPage());
            root.write();
        } else {
            int todo;
            rowCount = 10;
        }
        this.headPos = headPos;
        table.setRowCount(rowCount);
    }


    public void add(Session session, Row row) throws SQLException {
        int invalidateRowCount;
        PageData root = getPage(headPos);
        root.addRow(row);
        rowCount++;
    }

    private PageData getPage(int id) throws SQLException {
        DataPageBinary data = store.readPage(id);
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
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, "type=" + type);
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
        int todo;
        rowCount++;
    }

    public void remove(Session session) throws SQLException {
        int todo;
    }

    public void truncate(Session session) throws SQLException {
        int invalidateRowCount;
        int todo;
        rowCount = 0;
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException();
    }

    public Row getRow(Session session, int key) throws SQLException {
        int todo;
        return null;
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
