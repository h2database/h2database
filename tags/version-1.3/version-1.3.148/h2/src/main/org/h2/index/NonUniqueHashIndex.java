/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.ArrayList;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.util.New;
import org.h2.util.ValueHashMap;
import org.h2.value.Value;

/**
 * A non-unique index based on an in-memory hash map.
 *
 * @author Sergi Vladykin
 */
public class NonUniqueHashIndex extends HashIndex {

    private ValueHashMap<ArrayList<Long>> rows;
    private RegularTable tableData;
    private long rowCount;

    public NonUniqueHashIndex(RegularTable table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
        super(table, id, indexName, columns, indexType);
        this.tableData = table;
        reset();
    }

    private void reset() {
        rows = ValueHashMap.newInstance();
        rowCount = 0;
    }

    public void truncate(Session session) {
        reset();
    }

    public void add(Session session, Row row) {
        Value key = row.getValue(indexColumn);
        ArrayList<Long> positions = rows.get(key);
        if (positions == null) {
            positions = New.arrayList();
            rows.put(key, positions);
        }
        positions.add(row.getKey());
        rowCount++;
    }

    public void remove(Session session, Row row) {
        if (rowCount == 1) {
            // last row in table
            reset();
        } else {
            Value key = row.getValue(indexColumn);
            ArrayList<Long> positions = rows.get(key);
            if (positions.size() == 1) {
                // last row with such key
                rows.remove(key);
            } else {
                positions.remove(row.getKey());
            }
            rowCount--;
        }
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) {
        if (first == null || last == null) {
            throw DbException.throwInternalError();
        }
        if (first != last) {
            if (compareKeys(first, last) != 0) {
                throw DbException.throwInternalError();
            }
        }
        ArrayList<Long> positions = rows.get(first.getValue(indexColumn));
        return new NonUniqueHashCursor(session, tableData, positions);
    }

    public long getRowCount(Session session) {
        return rowCount;
    }

    public long getRowCountApproximation() {
        return rowCount;
    }

}
