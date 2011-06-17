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

/**
 * The cursor implementation for the page b-tree index.
 */
public class PageBtreeCursor implements Cursor {

    private final Session session;
    private final PageBtreeIndex index;
    private final SearchRow last;
    private PageBtreeLeaf current;
    private int i;
    private SearchRow currentSearchRow;
    private Row currentRow;

    PageBtreeCursor(Session session, PageBtreeIndex index, SearchRow last) {
        this.session = session;
        this.index = index;
        this.last = last;
    }

    /**
     * Set the position of the current row.
     *
     * @param current the leaf page
     * @param i the index within the page
     */
    void setCurrent(PageBtreeLeaf current, int i) {
        this.current = current;
        this.i = i;
    }

    public Row get() throws SQLException {
        if (currentRow == null && currentSearchRow != null) {
            currentRow = index.getRow(session, currentSearchRow.getPos());
        }
        return currentRow;
    }

    public int getPos() {
        return currentSearchRow.getPos();
    }

    public SearchRow getSearchRow() {
        return currentSearchRow;
    }

    public boolean next() throws SQLException {
        if (current == null) {
            return false;
        }
        if (i >= current.getEntryCount()) {
            current.nextPage(this);
            if (current == null) {
                return false;
            }
        }
        currentSearchRow = current.getRow(i);
        currentRow = null;
        if (last != null && index.compareRows(currentSearchRow, last) > 0) {
            currentSearchRow = null;
            return false;
        }
        i++;
        return true;
    }

    public boolean previous() throws SQLException {
        if (current == null) {
            return false;
        }
        if (i < 0) {
            current.previousPage(this);
            if (current == null) {
                return false;
            }
        }
        currentSearchRow = current.getRow(i);
        currentRow = null;
        i--;
        return true;
    }

}
