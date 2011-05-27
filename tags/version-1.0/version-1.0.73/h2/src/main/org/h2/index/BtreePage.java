/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.DataPage;
import org.h2.store.DiskFile;
import org.h2.store.Record;
import org.h2.table.Column;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

/**
 * An abstract b-tree page.
 */
public abstract class BtreePage extends Record {
    // TODO btree: make sure the indexed data is at most half this size!
    // (and find a solution to work around this problem!)
    // TODO memory: the btree page needs a lot of memory (in the cache) -
    // probably better not use ObjectArray but array;
    // not Row but ValueList / Value (for single key index), int array for row
    // pos

    protected static final int BLOCKS_PER_PAGE = 1024 / DiskFile.BLOCK_SIZE;

    protected BtreeIndex index;
    protected ObjectArray pageData;
    protected boolean root;
    
    BtreePage(BtreeIndex index) {
        this.index = index;
    }

    /**
     * Add a row to the index.
     *
     * @param row the row
     * @param session the session
     * @return the split point of this page, or 0 if no split is required
     */
    abstract int add(Row row, Session session) throws SQLException;

    /**
     * Remove a row from the page.
     * 
     * @param session the session
     * @param row the row
     * @return the new first row in the list; null if no change; the deleted row
     *         if not empty
     */
    abstract SearchRow remove(Session session, Row row) throws SQLException;

    abstract BtreePage split(Session session, int splitPoint) throws SQLException;
    abstract boolean findFirst(BtreeCursor cursor, SearchRow row, boolean bigger) throws SQLException;
    abstract SearchRow getFirst(Session session) throws SQLException;
    abstract void next(BtreeCursor cursor, int i) throws SQLException;
    abstract void previous(BtreeCursor cursor, int i) throws SQLException;
    abstract void first(BtreeCursor cursor) throws SQLException;
    abstract void last(BtreeCursor cursor) throws SQLException;
    abstract int getRealByteCount() throws SQLException;

    SearchRow getData(int i) throws SQLException {
        return (SearchRow) pageData.get(i);
    }

    public int getByteCount(DataPage dummy) {
        return DiskFile.BLOCK_SIZE * BLOCKS_PER_PAGE;
    }

    int getSplitPoint() throws SQLException {
        if (pageData.size() == 1) {
            return 0;
        }
        int size = getRealByteCount();
        if (size >= DiskFile.BLOCK_SIZE * BLOCKS_PER_PAGE) {
            return pageData.size() / 2;
        }
        return 0;
    }

    public boolean isEmpty() {
        return false;
    }

    int getRowSize(DataPage dummy, SearchRow row) throws SQLException {
        int rowsize = dummy.getIntLen();
        Column[] columns = index.getColumns();
        for (int j = 0; j < columns.length; j++) {
            Value v = row.getValue(columns[j].getColumnId());
            rowsize += dummy.getValueLen(v);
        }
        return rowsize;
    }

    void setRoot(boolean root) {
        this.root = root;
    }

    public boolean isPinned() {
        return root;
    }

}
