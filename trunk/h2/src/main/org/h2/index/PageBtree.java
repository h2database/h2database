/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import org.h2.result.SearchRow;
import org.h2.store.DataPage;
import org.h2.store.Record;

/**
 * A page that contains index data.
 */
abstract class PageBtree extends Record {

    /**
     * Indicator that the row count is not known.
     */
    static final int UNKNOWN_ROWCOUNT = -1;

    /**
     * The index.
     */
    protected final PageBtreeIndex index;

    /**
     * The page number of the parent.
     */
    protected int parentPageId;

    /**
     * The data page.
     */
    protected final DataPage data;

    /**
     * The row offsets.
     */
    protected int[] offsets;

    /**
     * The number of entries.
     */
    protected int entryCount;

    /**
     * The index data
     */
    protected SearchRow[] rows;

    /**
     * The start of the data area.
     */
    protected int start;

    /**
     * If the page was already written to the buffer.
     */
    protected boolean written;

    PageBtree(PageBtreeIndex index, int pageId, int parentPageId, DataPage data) {
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
     * Find an entry.
     *
     * @param compare the row
     * @param bigger if looking for a larger row
     * @param add if the row should be added (check for duplicate keys)
     * @return the index of the found row
     */
    int find(SearchRow compare, boolean bigger, boolean add) throws SQLException {
        if (compare == null) {
            return 0;
        }
        int l = 0, r = entryCount;
        int comp = 1;
        while (l < r) {
            int i = (l + r) >>> 1;
            SearchRow row = getRow(i);
            comp = index.compareRows(row, compare);
            if (comp == 0 && add) {
                if (index.indexType.getUnique()) {
                    if (!index.containsNullAndAllowMultipleNull(compare)) {
                        throw index.getDuplicateKeyException();
                    }
                }
                comp = index.compareKeys(row, compare);
            }
            if (comp > 0 || (!bigger && comp == 0)) {
                r = i;
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
     * Add a row.
     *
     * @param row the row
     * @return 0 if successful, or the split position if the page needs to be
     *         split
     */
    abstract int addRow(SearchRow row) throws SQLException;

    /**
     * Find the first row.
     *
     * @param cursor the cursor
     * @param first the row to find
     * @param if the row should be bigger
     */
    abstract void find(PageBtreeCursor cursor, SearchRow first, boolean bigger) throws SQLException;

    /**
     * Get the row at this position.
     *
     * @param at the index
     * @return the row
     */
    SearchRow getRow(int at) throws SQLException {
int test;
if (at < 0) {
    System.out.println("stop");
}
        SearchRow row = rows[at];
        if (row == null) {
            row = index.readRow(data, offsets[at]);
            rows[at] = row;
        }
        return row;
    }

    /**
     * Split the index page at the given point.
     *
     * @param splitPoint the index where to split
     * @return the new page that contains about half the entries
     */
    abstract PageBtree split(int splitPoint) throws SQLException;

    /**
     * Change the page id.
     *
     * @param id the new page id
     */
    void setPageId(int id) throws SQLException {
        index.getPageStore().removeRecord(getPos());
        setPos(id);
        remapChildren();
    }

    int getPageId() {
        return getPos();
    }

    /**
     * Get the first child leaf page of a page.
     *
     * @return the page
     */
    abstract PageBtreeLeaf getFirstLeaf() throws SQLException;

    /**
     * Change the parent page id.
     *
     * @param id the new parent page id
     */
    void setParentPageId(int id) {
        this.parentPageId = id;
    }

    /**
     * Update the parent id of all children.
     */
    abstract void remapChildren() throws SQLException;

    /**
     * Remove a row.
     *
     * @param row the row to remove
     * @return true if this page is now empty
     */
    abstract boolean remove(SearchRow row) throws SQLException;

    /**
     * Ensure all rows are read in memory.
     */
    protected void readAllRows() throws SQLException {
        for (int i = 0; i < entryCount; i++) {
            getRow(i);
        }
    }

}
