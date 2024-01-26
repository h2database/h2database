/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * Lazy execution support for queries.
 *
 * @author Sergi Vladykin
 */
public abstract class LazyResult extends FetchedResult {

    private final SessionLocal session;
    private final Expression[] expressions;
    private boolean closed;
    private long limit;

    public LazyResult(SessionLocal session, Expression[] expressions) {
        this.session = session;
        this.expressions = expressions;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public boolean isLazy() {
        return true;
    }

    @Override
    public void reset() {
        if (closed) {
            throw DbException.getInternalError();
        }
        rowId = -1L;
        afterLast = false;
        currentRow = null;
        nextRow = null;
    }

    /**
     * Go to the next row and skip it.
     *
     * @return true if a row exists
     */
    public boolean skip() {
        if (closed || afterLast) {
            return false;
        }
        currentRow = null;
        if (nextRow != null) {
            nextRow = null;
            return true;
        }
        if (skipNextRow()) {
            return true;
        }
        afterLast = true;
        return false;
    }

    @Override
    public boolean hasNext() {
        if (closed || afterLast) {
            return false;
        }
        if (nextRow == null && (limit <= 0 || rowId + 1 < limit)) {
            nextRow = fetchNextRow();
        }
        return nextRow != null;
    }

    /**
     * Fetch next row or null if none available.
     *
     * @return next row or null
     */
    protected abstract Value[] fetchNextRow();

    /**
     * Skip next row.
     *
     * @return true if next row was available
     */
    protected boolean skipNextRow() {
        return fetchNextRow() != null;
    }

    @Override
    public long getRowCount() {
        throw DbException.getUnsupportedException("Row count is unknown for lazy result.");
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public String getAlias(int i) {
        return expressions[i].getAlias(session, i);
    }

    @Override
    public String getSchemaName(int i) {
        return expressions[i].getSchemaName();
    }

    @Override
    public String getTableName(int i) {
        return expressions[i].getTableName();
    }

    @Override
    public String getColumnName(int i) {
        return expressions[i].getColumnName(session, i);
    }

    @Override
    public TypeInfo getColumnType(int i) {
        return expressions[i].getType();
    }

    @Override
    public boolean isIdentity(int i) {
        return expressions[i].isIdentity();
    }

    @Override
    public int getNullable(int i) {
        return expressions[i].getNullable();
    }

    @Override
    public void setFetchSize(int fetchSize) {
        // ignore
    }

    @Override
    public int getFetchSize() {
        // We always fetch rows one by one.
        return 1;
    }

}
