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

import org.h2.table.Column;
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

    public void add(Column column, Value value) {
        row.add(column);
        row.add(value.getObject());
    }

    public void clear() {
        data.clear();
        row.clear();
        columns.clear();
    }

    public void confirmRow() {
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
        for (Column column : columns) {
            rs.addColumn(column.getName(), DataType.convertTypeToSQLType(column.getType()),
                    MathUtils.convertLongToInt(column.getPrecision()), column.getScale());
        }
        for (Map<Column, Object> map : data) {
            Object[] row = new Object[columns.size()];
            for (Map.Entry<Column, Object> entry : map.entrySet()) {
                row[columns.indexOf(entry.getKey())] = entry.getValue();
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
