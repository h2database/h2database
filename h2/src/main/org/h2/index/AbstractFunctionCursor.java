/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;

/**
 * Abstract function cursor.
 */
abstract class AbstractFunctionCursor implements Cursor {
    private final FunctionIndex index;

    private final SearchRow first;

    private final SearchRow last;

    final Session session;

    Value[] values;

    Row row;

    /**
     * @param index
     *            index
     * @param first
     *            first row
     * @param last
     *            last row
     * @param session
     *            session
     */
    AbstractFunctionCursor(FunctionIndex index, SearchRow first, SearchRow last, Session session) {
        this.index = index;
        this.first = first;
        this.last = last;
        this.session = session;
    }

    @Override
    public Row get() {
        if (values == null) {
            return null;
        }
        if (row == null) {
            row = session.createRow(values, 1);
        }
        return row;
    }

    @Override
    public SearchRow getSearchRow() {
        return get();
    }

    @Override
    public boolean next() {
        while (nextImpl()) {
            Row current = get();
            if (first != null) {
                int comp = index.compareRows(current, first);
                if (comp < 0) {
                    continue;
                }
            }
            if (last != null) {
                int comp = index.compareRows(current, last);
                if (comp > 0) {
                    continue;
                }
            }
            return true;
        }
        return false;
    }

    abstract boolean nextImpl();

    @Override
    public boolean previous() {
        throw DbException.throwInternalError(toString());
    }

}