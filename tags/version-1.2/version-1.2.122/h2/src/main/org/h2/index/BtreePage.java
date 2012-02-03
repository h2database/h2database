/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
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

    /**
     * The maximum number of blocks occupied by a b-tree page.
     */
    protected static final int BLOCKS_PER_PAGE = 1024 / DiskFile.BLOCK_SIZE;

    /**
     * The b-tree index object
     */
    protected BtreeIndex index;

    // TODO memory: the b-tree page needs a lot of memory (in the cache) -
    // probably better not use ObjectArray but array

    /**
     * The list of data pages.
     */
    protected ObjectArray<SearchRow> pageData;

    /**
     * If this is the root page of the index.
     */
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
     *         if now empty
     */
    abstract SearchRow remove(Session session, Row row) throws SQLException;

    /**
     * Split the index page at the given point.
     *
     * @param session the session
     * @param splitPoint the index where to split
     * @return the new page that contains about half the entries
     */
    abstract BtreePage split(Session session, int splitPoint) throws SQLException;

    /**
     * Add the first found row to the cursor if a row is found.
     *
     * @param cursor the cursor
     * @param row the first row to find or null
     * @param bigger if a row bigger or equal to the row is needed
     * @return true if a row was found
     */
    abstract boolean findFirst(BtreeCursor cursor, SearchRow row, boolean bigger) throws SQLException;

    /**
     * Get the first row.
     *
     * @param session the session
     * @return the first row or null
     */
    abstract SearchRow getFirst(Session session) throws SQLException;

    /**
     * Get the next row.
     *
     * @param cursor the cursor
     * @param i the index in the row list
     */
    abstract void next(BtreeCursor cursor, int i) throws SQLException;

    /**
     * Get the previous row.
     *
     * @param cursor the cursor
     * @param i the index in the row list
     */
    abstract void previous(BtreeCursor cursor, int i) throws SQLException;

    /**
     * Get the first row.
     *
     * @param cursor the cursor
     */
    abstract void first(BtreeCursor cursor) throws SQLException;

    /**
     * Get the last row.
     *
     * @param cursor the cursor
     */
    abstract void last(BtreeCursor cursor) throws SQLException;

    /**
     * Calculate the number of bytes that contain data if the page is stored.
     *
     * @return the number of bytes
     */
    abstract int getRealByteCount() throws SQLException;

    /**
     * Get the row at the given index in the row list.
     *
     * @param i the index
     * @return the row
     * @throws SQLException
     */
    SearchRow getData(int i) throws SQLException {
        return pageData.get(i);
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

    /**
     * Get the size of a row (only the part that is stored in the index).
     *
     * @param dummy a dummy data page to calculate the size
     * @param row the row
     * @return the number of bytes
     */
    int getRowSize(DataPage dummy, SearchRow row) throws SQLException {
        int rowsize = DataPage.LENGTH_INT;
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
