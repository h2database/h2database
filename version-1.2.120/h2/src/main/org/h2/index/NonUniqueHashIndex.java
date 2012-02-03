/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import org.h2.util.IntArray;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.util.ValueHashMap;
import org.h2.value.Value;

/**
 * A non-unique index based on an in-memory hash map.
 *
 * @author Sergi Vladykin
 */
public class NonUniqueHashIndex extends BaseHashIndex {

    private ValueHashMap<IntArray> rows;
    private TableData tableData;
    private long rowCount;

    public NonUniqueHashIndex(TableData table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
        super(table, id, indexName, columns, indexType);
        this.tableData = table;
        reset();
    }

    private void reset() {
        rows = ValueHashMap.newInstance(table.getDatabase());
        rowCount = 0;
    }

    public void truncate(Session session) {
        reset();
    }

    public void add(Session session, Row row) throws SQLException {
        Value key = getKey(row);
        IntArray positions = rows.get(key);
        if (positions == null) {
            positions = new IntArray(1);
            rows.put(key, positions);
        }
        positions.add((int) row.getKey());
        rowCount++;
    }

    public void remove(Session session, Row row) throws SQLException {
        if (rowCount == 1) {
            // last row in table
            reset();
        } else {
            Value key = getKey(row);
            IntArray positions = rows.get(key);
            if (positions.size() == 1) {
                // last row with such key
                rows.remove(key);
            } else {
                positions.removeValue((int) row.getKey());
            }
            rowCount--;
        }
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        if (first == null || last == null) {
            throw Message.throwInternalError();
        }
        if (first != last) {
            if (compareKeys(first, last) != 0) {
                throw Message.throwInternalError();
            }
        }
        IntArray positions = rows.get(getKey(first));
        return new NonUniqueHashCursor(session, tableData, positions);
    }

    public long getRowCount(Session session) {
        return rowCount;
    }

    public long getRowCountApproximation() {
        return rowCount;
    }

}
