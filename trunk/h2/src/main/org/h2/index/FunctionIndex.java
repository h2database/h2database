/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.expression.FunctionCall;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.FunctionTable;
import org.h2.value.Value;

public class FunctionIndex extends Index {
    
    private FunctionTable functionTable;
    private LocalResult result;
    
    public FunctionIndex(FunctionTable functionTable, Column[] columns, FunctionCall function) {
        super(functionTable, 0, null, columns, IndexType.createNonUnique(true));
        this.functionTable = functionTable;
    }

    public void close(Session session) throws SQLException {
    }

    public void add(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void remove(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        if(result == null) {
            result = functionTable.getResult(session);
        } else {
            result.reset();
        }
        return new FunctionCursor(result);
    }

    public int getCost(int[] masks) throws SQLException {
        if(masks != null) {
            throw Message.getUnsupportedException();
        }
        return Integer.MAX_VALUE;
    }

    public void remove(Session session) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void truncate(Session session) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public boolean needRebuild() {
        return false;
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException();
    }

    public boolean canGetFirstOrLast(boolean first) {
        return false;
    }

    public Value findFirstOrLast(Session session, boolean first) throws SQLException {
        throw Message.getUnsupportedException();
    }

}
