/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.TableData;
import org.h2.util.IntArray;

/**
 * Cursor implementation for non-unique hash index
 *
 * @author Sergi Vladykin
 */
public class NonUniqueHashCursor implements Cursor {

    private final Session session;
    private final IntArray positions;
    private final TableData tableData;

    private int index = -1;

    public NonUniqueHashCursor(Session session, TableData tableData, IntArray positions) {
        this.session = session;
        this.tableData = tableData;
        this.positions = positions;
    }

    public Row get() {
        if (index < 0 || index >= positions.size()) {
            return null;
        }
        return tableData.getRow(session, positions.get(index));
    }

    public SearchRow getSearchRow() {
        return get();
    }

    public boolean next() {
        return positions != null && ++index < positions.size();
    }

    public boolean previous() {
        return positions != null && --index >= 0;
    }

}
