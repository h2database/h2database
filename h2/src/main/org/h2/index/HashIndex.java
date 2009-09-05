/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.util.IntIntHashMap;
import org.h2.util.ValueHashMap;
import org.h2.value.Value;

/**
 * An unique index based on an in-memory hash map.
 */
public class HashIndex extends BaseHashIndex {

    private ValueHashMap<Integer> rows;
    private IntIntHashMap intMap;
    private TableData tableData;

    public HashIndex(TableData table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
        super(table, id, indexName, columns, indexType);
        this.tableData = table;
        reset();
    }

    private void reset() {
        if (columns.length == 1 && columns[0].getType() == Value.INT) {
            intMap = new IntIntHashMap();
        } else {
            rows = ValueHashMap.newInstance(table.getDatabase());
        }
    }

    public void truncate(Session session) {
        reset();
    }

    public void add(Session session, Row row) throws SQLException {
        if (intMap != null) {
            int key = row.getValue(columns[0].getColumnId()).getInt();
            intMap.put(key, row.getPos());
        } else {
            Value key = getKey(row);
            Object old = rows.get(key);
            if (old != null) {
                // TODO index duplicate key for hash indexes: is this allowed?
                throw getDuplicateKeyException();
            }
            rows.put(getKey(row), row.getPos());
        }
    }

    public void remove(Session session, Row row) throws SQLException {
        if (intMap != null) {
            int key = row.getValue(columns[0].getColumnId()).getInt();
            intMap.remove(key);
        } else {
            rows.remove(getKey(row));
        }
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        if (first == null || last == null) {
            // TODO hash index: should additionally check if values are the same
            throw Message.throwInternalError();
        }
        Row result;
        if (intMap != null) {
            int key = first.getValue(columns[0].getColumnId()).getInt();
            int pos = intMap.get(key);
            if (pos != IntIntHashMap.NOT_FOUND) {
                result = tableData.getRow(session, pos);
            } else {
                result = null;
            }
        } else {
            Integer pos = rows.get(getKey(first));
            if (pos == null) {
                result = null;
            } else {
                result = tableData.getRow(session, pos.intValue());
            }
        }
        return new HashCursor(result);
    }

    public long getRowCount(Session session) {
        return getRowCountApproximation();
    }

    public long getRowCountApproximation() {
        return intMap != null ? intMap.size() : rows.size();
    }

}
