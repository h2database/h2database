/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.ArrayList;

import org.h2.command.query.Query;
import org.h2.engine.SessionLocal;
import org.h2.expression.Parameter;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.table.QueryExpressionTable;

/**
 * This object represents a virtual index for a query expression.
 */
public abstract class QueryExpressionIndex extends Index {

    final QueryExpressionTable table;
    final String querySQL;
    final ArrayList<Parameter> originalParameters;
    Query query;

    QueryExpressionIndex(QueryExpressionTable table, String querySQL, ArrayList<Parameter> originalParameters) {
        super(table, 0, null, null, 0, IndexType.createNonUnique(false));
        this.table = table;
        this.querySQL = querySQL;
        this.originalParameters = originalParameters;
        columns = new Column[0];
    }

    public abstract boolean isExpired();

    @Override
    public String getPlanSQL() {
        return query == null ? null : query.getPlanSQL(TRACE_SQL_FLAGS | ADD_PLAN_INFORMATION);
    }

    public Query getQuery() {
        return query;
    }

    @Override
    public void close(SessionLocal session) {
        // nothing to do
    }

    @Override
    public void add(SessionLocal session, Row row) {
        throw DbException.getUnsupportedException(getClass().getSimpleName() + ".add");
    }

    @Override
    public void remove(SessionLocal session, Row row) {
        throw DbException.getUnsupportedException(getClass().getSimpleName() + ".remove");
    }

    @Override
    public void remove(SessionLocal session) {
        throw DbException.getUnsupportedException(getClass().getSimpleName() + ".remove");
    }

    @Override
    public void truncate(SessionLocal session) {
        throw DbException.getUnsupportedException(getClass().getSimpleName() + ".truncate");
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException(getClass().getSimpleName() + ".checkRename");
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public long getRowCount(SessionLocal session) {
        return 0L;
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return 0L;
    }

}
