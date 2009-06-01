/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
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

    public Row get() {
        return row;
    }

    public int getPos() {
        return row.getPos();
    }

    public SearchRow getSearchRow() {
        return get();
    }

    public boolean next() throws SQLException {
        if (index >= current.getEntryCount()) {
            current = current.getNextPage();
            index = 0;
            if (current == null) {
                return false;
            }
        }
        row = current.getRowAt(index);
        index++;
        return true;
    }

    public boolean previous() {
        index--;
        int todo;
        return true;
    }

}
