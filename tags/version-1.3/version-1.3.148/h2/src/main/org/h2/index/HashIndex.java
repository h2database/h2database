/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.util.ValueHashMap;
import org.h2.value.Value;

/**
 * An unique index based on an in-memory hash map.
 */
public class HashIndex extends BaseIndex {

    /**
     * The index of the indexed column.
     */
    protected final int indexColumn;

    private final RegularTable tableData;
    private ValueHashMap<Long> rows;

    public HashIndex(RegularTable table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, indexName, columns, indexType);
        this.indexColumn = columns[0].column.getColumnId();
        this.tableData = table;
        reset();
    }

    private void reset() {
        rows = ValueHashMap.newInstance();
    }

    public void truncate(Session session) {
        reset();
    }

    public void add(Session session, Row row) {
        Value key = row.getValue(indexColumn);
        Object old = rows.get(key);
        if (old != null) {
            // TODO index duplicate key for hash indexes: is this allowed?
            throw getDuplicateKeyException();
        }
        rows.put(key, row.getKey());
    }

    public void remove(Session session, Row row) {
        rows.remove(row.getValue(indexColumn));
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) {
        if (first == null || last == null) {
            // TODO hash index: should additionally check if values are the same
            throw DbException.throwInternalError();
        }
        Row result;
        Long pos = rows.get(first.getValue(indexColumn));
        if (pos == null) {
            result = null;
        } else {
            result = tableData.getRow(session, pos.intValue());
        }
        return new SingleRowCursor(result);
    }

    public long getRowCount(Session session) {
        return getRowCountApproximation();
    }

    public long getRowCountApproximation() {
        return rows.size();
    }

    public void close(Session session) {
        // nothing to do
    }

    public void remove(Session session) {
        // nothing to do
    }

    public double getCost(Session session, int[] masks) {
        for (Column column : columns) {
            int index = column.getColumnId();
            int mask = masks[index];
            if ((mask & IndexCondition.EQUALITY) != IndexCondition.EQUALITY) {
                return Long.MAX_VALUE;
            }
        }
        return 2;
    }

    public void checkRename() {
        // ok
    }

    public boolean needRebuild() {
        return true;
    }

    public boolean canGetFirstOrLast() {
        return false;
    }

    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("HASH");
    }

}
