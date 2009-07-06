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
import org.h2.store.DataPage;
import org.h2.store.Record;

/**
 * A page that contains data rows.
 */
abstract class PageData extends Record {

    /**
     * Indicator that the row count is not known.
     */
    static final int UNKNOWN_ROWCOUNT = -1;

    /**
     * The index.
     */
    protected final PageScanIndex index;

    /**
     * The page number of the parent.
     */
    protected int parentPageId;

    /**
     * The data page.
     */
    protected final DataPage data;

    /**
     * The number of entries.
     */
    protected int entryCount;

    /**
     * The row keys.
     */
    protected int[] keys;

    /**
     * Whether the data page is up-to-date.
     */
    protected boolean written;

    PageData(PageScanIndex index, int pageId, int parentPageId, DataPage data) {
        this.index = index;
        this.parentPageId = parentPageId;
        this.data = data;
        setPos(pageId);
    }

    /**
     * Get the real row count. If required, this will read all child pages.
     *
     * @return the row count
     */
    abstract int getRowCount() throws SQLException;

    /**
     * Set the stored row count. This will write the page.
     *
     * @param rowCount the stored row count
     */
    abstract void setRowCountStored(int rowCount) throws SQLException;

    /**
     * Find an entry by key.
     *
     * @param key the key (may not exist)
     * @return the matching or next index
     */
    int find(int key) {
        int l = 0, r = entryCount;
        while (l < r) {
            int i = (l + r) >>> 1;
            int k = keys[i];
            if (k > key) {
                r = i;
            } else if (k == key) {
                return i;
            } else {
                l = i + 1;
            }
        }
        return l;
    }

    /**
     * Read the data.
     */
    abstract void read() throws SQLException;

    /**
     * Try to add a row.
     *
     * @param row the row
     * @return 0 if successful, or the split position if the page needs to be
     *         split
     */
    abstract int addRowTry(Row row) throws SQLException;

    /**
     * Get a cursor.
     *
     * @param session the session
     * @return the cursor
     */
    abstract Cursor find(Session session) throws SQLException;

    /**
     * Get the key at this position.
     *
     * @param index the index
     * @return the key
     */
    int getKey(int index) {
        return keys[index];
    }

    /**
     * Split the index page at the given point.
     *
     * @param splitPoint the index where to split
     * @return the new page that contains about half the entries
     */
    abstract PageData split(int splitPoint) throws SQLException;

    /**
     * Change the page id.
     *
     * @param id the new page id
     */
    void setPageId(int id) throws SQLException {
        written = false;
        index.getPageStore().removeRecord(getPos());
        setPos(id);
        remapChildren();
    }

    /**
     * Get the last key of a page.
     *
     * @return the last key
     */
    abstract int getLastKey() throws SQLException;

    /**
     * Get the first child leaf page of a page.
     *
     * @return the page
     */
    abstract PageDataLeaf getFirstLeaf() throws SQLException;

    /**
     * Change the parent page id.
     *
     * @param id the new parent page id
     */
    void setParentPageId(int id) {
        written = false;
        parentPageId = id;
    }

    /**
     * Update the parent id of all children.
     */
    abstract void remapChildren() throws SQLException;

    /**
     * Remove a row.
     *
     * @param key the key of the row to remove
     * @return true if this page is now empty
     */
    abstract boolean remove(int key) throws SQLException;

    /**
     * Free up all child pages.
     */
    abstract void freeChildren() throws SQLException;

    /**
     * Get the row for the given key.
     *
     * @param key the key
     * @return the row
     */
    abstract Row getRow(int key) throws SQLException;

    /**
     * Get the estimated memory size.
     *
     * @return number of double words (4 bytes)
     */
    public int getMemorySize() {
        return index.getPageStore().getPageSize() >> 2;
    }

    int getParentPageId() {
        return parentPageId;
    }

}
