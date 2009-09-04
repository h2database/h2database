/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * A cursor for a function that returns a result set.
 */
public class FunctionCursor implements Cursor {

    private LocalResult result;
    private Value[] values;
    private Row row;

    FunctionCursor(LocalResult result) {
        this.result = result;
    }

    public Row get() {
        if (values == null) {
            return null;
        }
        if (row == null) {
            row = new Row(values, 0);
        }
        return row;
    }

    public SearchRow getSearchRow() {
        return get();
    }

    public int getPos() {
        throw Message.throwInternalError();
    }

    public boolean next() throws SQLException {
        row = null;
        if (result.next()) {
            values = result.currentRow();
        } else {
            values = null;
        }
        return values != null;
    }

    public boolean previous() {
        throw Message.throwInternalError();
    }

}
