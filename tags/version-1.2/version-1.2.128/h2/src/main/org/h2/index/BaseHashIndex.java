/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.value.Value;
import org.h2.value.ValueArray;

/**
 * Base of hash indexes.
 */
public abstract class BaseHashIndex extends BaseIndex {

    public BaseHashIndex(TableData table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, indexName, columns, indexType);
    }

    public void close(Session session) {
        // nothing to do
    }

    public void remove(Session session) {
        // nothing to do
    }

    /**
     * Generate the search key from a row. Single column indexes are mapped to
     * the value, multi-column indexes are mapped to an value array.
     *
     * @param row the row
     * @return the value
     */
    protected Value getKey(SearchRow row) {
        if (columns.length == 1) {
            Column column = columns[0];
            int index = column.getColumnId();
            Value v = row.getValue(index);
            return v;
        }
        Value[] list = new Value[columns.length];
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            int index = column.getColumnId();
            list[i] = row.getValue(index);
        }
        return ValueArray.get(list);
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

    public Cursor findFirstOrLast(Session session, boolean first) throws SQLException {
        throw Message.getUnsupportedException("HASH");
    }

}
