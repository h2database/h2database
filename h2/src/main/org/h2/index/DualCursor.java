/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * The cursor implementation for the DUAL index.
 */
class DualCursor implements Cursor {

    private final Session session;

    private Row currentRow;

    DualCursor(Session session) {
        this.session = session;
    }

    @Override
    public Row get() {
        return currentRow;
    }

    @Override
    public SearchRow getSearchRow() {
        return currentRow;
    }

    @Override
    public boolean next() {
        if (currentRow == null) {
            currentRow = session.createRow(new Value[0], 1);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError(toString());
    }

}
