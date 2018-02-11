/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.tools.SimpleResultSet;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.value.DataType;

/**
 * Class for gathering and processing of generated keys.
 */
public final class GeneratedKeys {
    /**
     * Data for result set with generated keys.
     */
    private final ArrayList<Map<Column, Object>> data = New.arrayList();

    /**
     * Columns with generated keys in the current row.
     */
    private final ArrayList<Column> row = New.arrayList();

    /**
     * All columns with generated keys.
     */
    private final ArrayList<Column> allColumns = New.arrayList();

    /**
     * Request for keys gathering. {@code false} if generated keys are not needed,
     * {@code true} if generated keys should be configured automatically,
     * {@code int[]} to specify column indices to return generated keys from, or
     * {@code String[]} to specify column names to return generated keys from.
     */
    private Object generatedKeysRequest;

    /**
     * Processed table.
     */
    private Table table;

    /**
     * Remembers columns with generated keys.
     *
     * @param column
     *            table column
     */
    public void add(Column column) {
        if (Boolean.FALSE.equals(generatedKeysRequest)) {
            return;
        }
        row.add(column);
    }

    /**
     * Clears all information from previous runs and sets a new request for
     * gathering of generated keys.
     *
     * @param generatedKeysRequest
     *            {@code false} if generated keys are not needed, {@code true} if
     *            generated keys should be configured automatically, {@code int[]}
     *            to specify column indices to return generated keys from, or
     *            {@code String[]} to specify column names to return generated keys
     *            from
     */
    public void clear(Object generatedKeysRequest) {
        this.generatedKeysRequest = generatedKeysRequest;
        data.clear();
        row.clear();
        allColumns.clear();
        table = null;
    }

    /**
     * Saves row with generated keys if any.
     *
     * @param tableRow
     *            table row that was inserted
     */
    public void confirmRow(Row tableRow) {
        if (Boolean.FALSE.equals(generatedKeysRequest)) {
            return;
        }
        int size = row.size();
        if (size > 0) {
            if (size == 1) {
                Column column = row.get(0);
                data.add(Collections.singletonMap(column, tableRow.getValue(column.getColumnId()).getObject()));
                if (!allColumns.contains(column)) {
                    allColumns.add(column);
                }
            } else {
                HashMap<Column, Object> map = new HashMap<>();
                for (Column column : row) {
                    map.put(column, tableRow.getValue(column.getColumnId()).getObject());
                    if (!allColumns.contains(column)) {
                        allColumns.add(column);
                    }
                }
                data.add(map);
            }
            row.clear();
        }
    }

    /**
     * Returns generated keys.
     *
     * @return result set with generated keys
     */
    public SimpleResultSet getKeys() {
        SimpleResultSet rs = new SimpleResultSet();
        if (Boolean.FALSE.equals(generatedKeysRequest)) {
            return rs;
        }
        if (Boolean.TRUE.equals(generatedKeysRequest)) {
            for (Column column : allColumns) {
                rs.addColumn(column.getName(), DataType.convertTypeToSQLType(column.getType()),
                        MathUtils.convertLongToInt(column.getPrecision()), column.getScale());
            }
        } else if (generatedKeysRequest instanceof int[]) {
            if (table != null) {
                int[] indices = (int[]) generatedKeysRequest;
                Column[] columns = table.getColumns();
                int cnt = columns.length;
                this.allColumns.clear();
                for (int idx : indices) {
                    if (idx >= 1 && idx <= cnt) {
                        Column column = columns[idx - 1];
                        rs.addColumn(column.getName(), DataType.convertTypeToSQLType(column.getType()),
                                MathUtils.convertLongToInt(column.getPrecision()), column.getScale());
                        this.allColumns.add(column);
                    }
                }
            } else {
                return rs;
            }
        } else if (generatedKeysRequest instanceof String[]) {
            if (table != null) {
                String[] names = (String[]) generatedKeysRequest;
                this.allColumns.clear();
                for (String name : names) {
                    try {
                        Column column = table.getColumn(name);
                        rs.addColumn(column.getName(), DataType.convertTypeToSQLType(column.getType()),
                                MathUtils.convertLongToInt(column.getPrecision()), column.getScale());
                        this.allColumns.add(column);
                    } catch (DbException e) {
                    }
                }
            } else {
                return rs;
            }
        } else {
            return rs;
        }
        if (rs.getColumnCount() == 0) {
            return rs;
        }
        for (Map<Column, Object> map : data) {
            Object[] row = new Object[allColumns.size()];
            for (Map.Entry<Column, Object> entry : map.entrySet()) {
                int idx = allColumns.indexOf(entry.getKey());
                if (idx >= 0) {
                    row[idx] = entry.getValue();
                }
            }
            rs.addRow(row);
        }
        return rs;
    }

    /**
     * Initializes processing of the specified table. Should be called after
     * {@code clear()}, but before other methods.
     *
     * @param table
     *            table
     */
    public void initialize(Table table) {
        this.table = table;
    }

    /**
     * Clears unsaved information about previous row, if any. Should be called
     * before processing of a new row if previous row was not confirmed or simply
     * always before each row.
     */
    public void nextRow() {
        row.clear();
    }

    @Override
    public String toString() {
        return allColumns + ": " + data.size();
    }

}
