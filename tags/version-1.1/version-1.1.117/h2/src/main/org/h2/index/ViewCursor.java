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
import org.h2.table.Table;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * The cursor implementation of a view index.
 */
public class ViewCursor implements Cursor {

    private Table table;
    private LocalResult result;
    private Row current;

    ViewCursor(Table table, LocalResult result) {
        this.table = table;
        this.result = result;
    }

    public Row get() {
        return current;
    }

    public SearchRow getSearchRow() {
        return current;
    }

    public int getPos() {
        throw Message.throwInternalError();
    }

    public boolean next() throws SQLException {
        boolean res = result.next();
        if (!res) {
            result.close();
            current = null;
            return false;
        }
        current = table.getTemplateRow();
        Value[] values = result.currentRow();
        for (int i = 0; i < current.getColumnCount(); i++) {
            Value v = i < values.length ? values[i] : ValueNull.INSTANCE;
            current.setValue(i, v);
        }
        return true;
    }

    public boolean previous() {
        throw Message.throwInternalError();
    }

}
