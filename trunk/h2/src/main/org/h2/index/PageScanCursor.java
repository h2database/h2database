/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import java.util.Iterator;

import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * The cursor implementation for the page scan index.
 */
class PageScanCursor implements Cursor {

    private PageDataLeaf current;
    private int idx;
    private Row row;
    private final boolean multiVersion;
    private final Session session;
    private Iterator<Row> delta;

    PageScanCursor(Session session, PageDataLeaf current, int idx, boolean multiVersion) {
        this.current = current;
        this.idx = idx;
        this.multiVersion = multiVersion;
        this.session = session;
        if (multiVersion) {
            delta = current.index.getDelta();
        }
    }

    public Row get() {
        return row;
    }

    public int getPos() {
        return row.getPos();
    }

    public SearchRow getSearchRow() {
        return get();
    }

    public boolean next() throws SQLException {
        if (!multiVersion) {
            return nextRow();
        }
        while (true) {
            if (delta != null) {
                if (!delta.hasNext()) {
                    delta = null;
                    row = null;
                    continue;
                }
                row = delta.next();
                if (!row.isDeleted() || row.getSessionId() == session.getId()) {
                    continue;
                }
            } else {
                nextRow();
                if (row != null && row.getSessionId() != 0 && row.getSessionId() != session.getId()) {
                    continue;
                }
            }
            break;
        }
        return row != null;
    }

    private boolean nextRow() throws SQLException {
        if (idx >= current.getEntryCount()) {
            current = current.getNextPage();
            idx = 0;
            if (current == null) {
                row = null;
                return false;
            }
        }
        row = current.getRowAt(idx);
        idx++;
        return true;
    }

    public boolean previous() {
        throw Message.throwInternalError();
    }

}
