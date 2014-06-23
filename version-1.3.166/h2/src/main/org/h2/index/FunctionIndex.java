/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.FunctionTable;
import org.h2.table.IndexColumn;

/**
 * An index for a function that returns a result set. This index can only scan
 * through all rows, search is not supported.
 */
public class FunctionIndex extends BaseIndex {

    private final FunctionTable functionTable;

    public FunctionIndex(FunctionTable functionTable, IndexColumn[] columns) {
        initBaseIndex(functionTable, 0, null, columns, IndexType.createNonUnique(true));
        this.functionTable = functionTable;
    }

    public void close(Session session) {
        // nothing to do
    }

    public void add(Session session, Row row) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    public void remove(Session session, Row row) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) {
        if (functionTable.isFast()) {
            return new FunctionCursorResultSet(session, functionTable.getResultSet(session));
        }
        return new FunctionCursor(functionTable.getResult(session));
    }

    public double getCost(Session session, int[] masks) {
        if (masks != null) {
            throw DbException.getUnsupportedException("ALIAS");
        }
        long expectedRows;
        if (functionTable.canGetRowCount()) {
            expectedRows = functionTable.getRowCountApproximation();
        } else {
            expectedRows = database.getSettings().estimatedFunctionTableRows;
        }
        return expectedRows * 10;
    }

    public void remove(Session session) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    public void truncate(Session session) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    public boolean needRebuild() {
        return false;
    }

    public void checkRename() {
        throw DbException.getUnsupportedException("ALIAS");
    }

    public boolean canGetFirstOrLast() {
        return false;
    }

    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    public long getRowCount(Session session) {
        return functionTable.getRowCount(session);
    }

    public long getRowCountApproximation() {
        return functionTable.getRowCountApproximation();
    }

    public String getPlanSQL() {
        return "function";
    }

    public boolean canScan() {
        return false;
    }

}
