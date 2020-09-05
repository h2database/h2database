/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.io.IOException;

import org.h2.engine.SessionRemote;
import org.h2.message.DbException;
import org.h2.value.Transfer;
import org.h2.value.Value;

/**
 * Result for 'lazy_query_execution=true', maps to a LazyResult in server side.
 */
public class LazyResultRemote extends ResultRemote {
    private Value[] nextRow = null;
    private boolean fetchOver = false, afterLast = false;

    LazyResultRemote(SessionRemote session, Transfer transfer, int id,
            int columnCount, int fetchSize) throws IOException {
        super(session, transfer, id, columnCount, fetchSize);
    }

    /* remote result cannot be lazy, see CommandRemote.stop()
    @Override
    public boolean isLazy() {
        return true;
    }
    */

    @Override
    public boolean next() {
        if (hasNext()) {
            rowId++;
            currentRow = nextRow;
            nextRow = null;
            return true;
        }
        if (!afterLast) {
            rowId++;
            currentRow = null;
            afterLast = true;
        }
        return false;
    }

    @Override
    public boolean isAfterLast() {
        return afterLast;
    }

    @Override
    public int getRowCount() {
        throw DbException.getUnsupportedException("Row count is unknown for lazy result.");
    }

    @Override
    public boolean hasNext() {
        if (result == null || afterLast) {
            return false;
        }
        if (nextRow != null) {
            return true;
        }
        if (rowId + 1 - rowOffset >= result.size()) {
            if (session == null) {
                return false;
            }
            fetchRows(true);
            if (fetchOver && rowId + 1 - rowOffset >= result.size()) {
                return false;
            }
        }
        nextRow = result.get(rowId + 1 - rowOffset);
        return true;
    }

    @Override
    public void reset() {
        super.reset();
        nextRow = null;
        fetchOver = false;
        afterLast = false;
    }

    @Override
    protected int remoteFetchSize() {
        return fetchSize;
    }

    @Override
    protected boolean remoteFetchOver(boolean over) {
        if (over) {
            fetchOver = true;
        }
        return over;
    }

}
