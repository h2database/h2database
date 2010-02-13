/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.ArrayList;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * An index for a meta data table.
 * This index can only scan through all rows, search is not supported.
 */
public class MetaCursor implements Cursor {

    private Row current;
    private ArrayList<Row> rows;
    private int index;

    MetaCursor(ArrayList<Row> rows) {
        this.rows = rows;
    }

    public Row get() {
        return current;
    }

    public SearchRow getSearchRow() {
        return current;
    }

    public boolean next() {
        current = index >= rows.size() ? null : rows.get(index++);
        return current != null;
    }

    public boolean previous() {
        throw DbException.throwInternalError();
    }

}
