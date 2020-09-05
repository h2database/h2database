/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.io.IOException;

import org.h2.engine.SessionRemote;
import org.h2.value.Transfer;

/**
 * Result with a 'rowCount', whatever complete or a part data is kept in client side.
 */
public class RowCountResultRemote extends ResultRemote {
    private final int rowCount;

    RowCountResultRemote(SessionRemote session, Transfer transfer, int id,
            int columnCount, int fetchSize, int rowCount) throws IOException {
        super(session, transfer, id, columnCount, fetchSize);
        this.rowCount = rowCount;
    }

    @Override
    public boolean next() {
        if (rowId < rowCount) {
            rowId++;
            if (rowId < rowCount) {
                if (rowId - rowOffset >= result.size()) {
                    fetchRows(true);
                }
                currentRow = result.get(rowId - rowOffset);
                return true;
            }
            currentRow = null;
        }
        return false;
    }

    @Override
    public boolean isAfterLast() {
        return rowId >= rowCount;
    }

    @Override
    public int getRowCount() {
        return rowCount;
    }

    @Override
    public boolean hasNext() {
        return rowId < rowCount - 1;
    }

    @Override
    protected int remoteFetchSize() {
        return Math.min(fetchSize, rowCount - rowOffset);
    }

    @Override
    protected boolean remoteFetchOver(boolean over) {
        return rowOffset + result.size() >= rowCount;
    }

    @Override
    public String toString() {
        return super.toString() + " rows: " + rowCount;
    }

}
