/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.h2.tools.SimpleResultSet;

/**
 * Merged result set.
 */
public final class MergedResultSet {
    private static final class ColumnInfo {
        final String name;

        final int type;

        final int precision;

        final int scale;

        ColumnInfo(String name, int type, int precision, int scale) {
            this.name = name;
            this.type = type;
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null || getClass() != obj.getClass())
                return false;
            ColumnInfo other = (ColumnInfo) obj;
            return name.equals(other.name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

    }

    private final ArrayList<Map<ColumnInfo, Object>> data = New.arrayList();

    private final ArrayList<ColumnInfo> columns = New.arrayList();

    public void add(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        if (cols == 0) {
            return;
        }
        ColumnInfo[] info = new ColumnInfo[cols];
        for (int i = 1; i <= cols; i++) {
            ColumnInfo ci = new ColumnInfo(meta.getColumnName(i), meta.getColumnType(i), meta.getPrecision(i),
                    meta.getScale(i));
            info[i - 1] = ci;
            if (!columns.contains(ci)) {
                columns.add(ci);
            }
        }
        while (rs.next()) {
            if (cols == 1) {
                data.add(Collections.singletonMap(info[0], rs.getObject(1)));
            } else {
                HashMap<ColumnInfo, Object> map = new HashMap<>();
                for (int i = 1; i <= cols; i++) {
                    ColumnInfo ci = info[i - 1];
                    map.put(ci, rs.getObject(i));
                }
                data.add(map);
            }
        }
    }

    public SimpleResultSet getKeys() {
        SimpleResultSet rs = new SimpleResultSet();
        for (ColumnInfo ci : columns) {
            rs.addColumn(ci.name, ci.type, ci.precision, ci.scale);
        }
        for (Map<ColumnInfo, Object> map : data) {
            Object[] row = new Object[columns.size()];
            for (Map.Entry<ColumnInfo, Object> entry : map.entrySet()) {
                row[columns.indexOf(entry.getKey())] = entry.getValue();
            }
            rs.addRow(row);
        }
        return rs;
    }

    @Override
    public String toString() {
        return columns + ": " + data.size();
    }

}
