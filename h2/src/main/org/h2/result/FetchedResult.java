/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.Session;
import org.h2.value.Value;

/**
 * Abstract fetched result.
 */
public abstract class FetchedResult implements ResultInterface {

    long rowId = -1;

    Value[] currentRow;

    Value[] nextRow;

    boolean afterLast;

    FetchedResult() {
    }

    @Override
    public final Value[] currentRow() {
        return currentRow;
    }

    @Override
    public final boolean next() {
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
    public final boolean isAfterLast() {
        return afterLast;
    }

    @Override
    public final long getRowId() {
        return rowId;
    }

    @Override
    public final boolean needToClose() {
        return true;
    }

    @Override
    public final ResultInterface createShallowCopy(Session targetSession) {
        // The operation is not supported on fetched result.
        return null;
    }

}
