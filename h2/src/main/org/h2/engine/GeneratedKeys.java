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
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.tools.SimpleResultSet;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.value.DataType;
import org.h2.value.Value;

/**
 * Generated keys.
 */
public final class GeneratedKeys {
    private final ArrayList<Map<Column, Object>> data = New.arrayList();

    private final ArrayList<Object> row = New.arrayList();

    private final ArrayList<Column> columns = New.arrayList();

    private Object generatedKeysRequest;

    private Table table;

    public void add(Column column, Value value) {
        if (Boolean.FALSE.equals(generatedKeysRequest)) {
            return;
        }
        row.add(column);
        row.add(value.getObject());
    }

    public void clear(Object generatedKeysRequest) {
        this.generatedKeysRequest = generatedKeysRequest;
        data.clear();
        row.clear();
        columns.clear();
        table = null;
    }

    public void initialize(Table table) {
        this.table = table;
    }

    public void confirmRow() {
        if (Boolean.FALSE.equals(generatedKeysRequest)) {
            return;
        }
        int size = row.size();
        if (size > 0) {
            if (size == 2) {
                Column column = (Column) row.get(0);
                data.add(Collections.singletonMap(column, row.get(1)));
                if (!columns.contains(column)) {
                    columns.add(column);
                }
            } else {
                HashMap<Column, Object> map = new HashMap<>();
                for (int i = 0; i < size; i += 2) {
                    Column column = (Column) row.get(i);
                    map.put(column, row.get(i + 1));
                    if (!columns.contains(column)) {
                        columns.add(column);
                    }
                }
                data.add(map);
            }
            row.clear();
        }
    }

    public SimpleResultSet getKeys() {
        SimpleResultSet rs = new SimpleResultSet();
        if (Boolean.FALSE.equals(generatedKeysRequest)) {
            return rs;
        }
        if (Boolean.TRUE.equals(generatedKeysRequest)) {
            for (Column column : columns) {
                rs.addColumn(column.getName(), DataType.convertTypeToSQLType(column.getType()),
                        MathUtils.convertLongToInt(column.getPrecision()), column.getScale());
            }
        } else if (generatedKeysRequest instanceof int[]) {
            if (table != null) {
                int[] indices = (int[]) generatedKeysRequest;
                Column[] columns = table.getColumns();
                int cnt = columns.length;
                this.columns.clear();
                for (int idx : indices) {
                    if (idx >= 1 && idx <= cnt) {
                        Column column = columns[idx - 1];
                        rs.addColumn(column.getName(), DataType.convertTypeToSQLType(column.getType()),
                                MathUtils.convertLongToInt(column.getPrecision()), column.getScale());
                        this.columns.add(column);
                    }
                }
            } else {
                return rs;
            }
        } else if (generatedKeysRequest instanceof String[]) {
            if (table != null) {
                String[] names = (String[]) generatedKeysRequest;
                this.columns.clear();
                for (String name : names) {
                    try {
                        Column column = table.getColumn(name);
                        rs.addColumn(column.getName(), DataType.convertTypeToSQLType(column.getType()),
                                MathUtils.convertLongToInt(column.getPrecision()), column.getScale());
                        this.columns.add(column);
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
            Object[] row = new Object[columns.size()];
            for (Map.Entry<Column, Object> entry : map.entrySet()) {
                int idx = columns.indexOf(entry.getKey());
                if (idx >= 0) {
                    row[idx] = entry.getValue();
                }
            }
            rs.addRow(row);
        }
        return rs;
    }

    public void nextRow() {
        row.clear();
    }

    @Override
    public String toString() {
        return columns + ": " + data.size();
    }

}
