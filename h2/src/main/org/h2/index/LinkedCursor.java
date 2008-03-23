/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.value.DataType;
import org.h2.value.Value;

/**
 * The cursor implementation for the linked index.
 */
public class LinkedCursor implements Cursor {

    private Session session;
    private Row current;
    private ResultSet rs;
    private Table table;

    LinkedCursor(Table table, ResultSet rs, Session session) {
        this.session = session;
        this.table = table;
        this.rs = rs;
    }

    public Row get() {
        return current;
    }

    public SearchRow getSearchRow() throws SQLException {
        return current;
    }

    public int getPos() {
        throw Message.getInternalError();
    }


    public boolean next() throws SQLException {
        boolean result = rs.next();
        if (!result) {
            rs.close();
            current = null;
            return false;
        }
        current = table.getTemplateRow();
        for (int i = 0; i < current.getColumnCount(); i++) {
            Column col = table.getColumn(i);
            Value v = DataType.readValue(session, rs, i + 1, col.getType());
            current.setValue(i, v);
        }
        return true;
    }
    
    public boolean previous() {
        throw Message.getInternalError();
    }

}
