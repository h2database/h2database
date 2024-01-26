/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.HashMap;

import org.h2.result.Row;
import org.h2.value.Value;
import org.h2.value.ValueBigint;

/**
 * Column resolver for generated columns.
 */
class GeneratedColumnResolver implements ColumnResolver {

    private final Table table;

    private Column[] columns;

    private HashMap<String, Column> columnMap;

    private Row current;

    /**
     * Column resolver for generated columns.
     *
     * @param table
     *            the table
     */
    GeneratedColumnResolver(Table table) {
        this.table = table;
    }

    /**
     * Set the current row.
     *
     * @param current
     *            the current row
     */
    void set(Row current) {
        this.current = current;
    }

    @Override
    public Column[] getColumns() {
        Column[] columns = this.columns;
        if (columns == null) {
            this.columns = columns = createColumns();
        }
        return columns;
    }

    private Column[] createColumns() {
        Column[] allColumns = table.getColumns();
        int totalCount = allColumns.length, baseCount = totalCount;
        for (int i = 0; i < totalCount; i++) {
            if (allColumns[i].isGenerated()) {
                baseCount--;
            }
        }
        Column[] baseColumns = new Column[baseCount];
        for (int i = 0, j = 0; i < totalCount; i++) {
            Column c = allColumns[i];
            if (!c.isGenerated()) {
                baseColumns[j++] = c;
            }
        }
        return baseColumns;
    }

    @Override
    public Column findColumn(String name) {
        HashMap<String, Column> columnMap = this.columnMap;
        if (columnMap == null) {
            columnMap = table.getDatabase().newStringMap();
            for (Column c : getColumns()) {
                columnMap.put(c.getName(), c);
            }
            this.columnMap = columnMap;
        }
        return columnMap.get(name);
    }

    @Override
    public Value getValue(Column column) {
        int columnId = column.getColumnId();
        if (columnId == -1) {
            return ValueBigint.get(current.getKey());
        }
        return current.getValue(columnId);
    }

    @Override
    public Column getRowIdColumn() {
        return table.getRowIdColumn();
    }

}
