/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * The cursor implementation for the page scan index.
 */
class PageScanCursor implements Cursor {

    private PageDataLeaf current;
    private int index;
    private Row row;
    
    PageScanCursor(PageDataLeaf current, int index) {
        this.current = current;
        this.index = index;
    }

    public Row get() throws SQLException {
        return row;
    }

    public int getPos() {
        return row.getPos();
    }

    public SearchRow getSearchRow() throws SQLException {
        return get();
    }

    public boolean next() throws SQLException {
        int todo;
        if (index < current.getEntryCount()) {
            row = current.getRow(index);
            index++;
            return true;
        }
        return false;
    }

    public boolean previous() throws SQLException {
        index--;
        int todo;
        return true;
    }

}
