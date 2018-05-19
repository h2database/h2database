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
 * Merged result set. Used to combine several result sets into one. Merged
 * result set will contain rows from all appended result sets. Result sets are
 * not required to have the same lists of columns, but required to have
 * compatible column definitions, for example, if one result set has a
 * {@link java.sql.Types#VARCHAR} column {@code NAME} then another results sets
 * that have {@code NAME} column should also define it with the same type.
 */
public final class MergedResultSet {
    private final ArrayList<Map<SimpleColumnInfo, Object>> data = Utils.newSmallArrayList();

    private final ArrayList<SimpleColumnInfo> columns = Utils.newSmallArrayList();

    /**
     * Appends a result set.
     *
     * @param rs
     *            result set to append
     * @throws SQLException
     *             on SQL exception
     */
    public void add(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        if (cols == 0) {
            return;
        }
        SimpleColumnInfo[] info = new SimpleColumnInfo[cols];
        for (int i = 1; i <= cols; i++) {
            SimpleColumnInfo ci = new SimpleColumnInfo(meta.getColumnName(i), meta.getColumnType(i),
                    meta.getColumnTypeName(i), meta.getPrecision(i), meta.getScale(i));
            info[i - 1] = ci;
            if (!columns.contains(ci)) {
                columns.add(ci);
            }
        }
        while (rs.next()) {
            if (cols == 1) {
                data.add(Collections.singletonMap(info[0], rs.getObject(1)));
            } else {
                HashMap<SimpleColumnInfo, Object> map = new HashMap<>();
                for (int i = 1; i <= cols; i++) {
                    SimpleColumnInfo ci = info[i - 1];
                    map.put(ci, rs.getObject(i));
                }
                data.add(map);
            }
        }
    }

    /**
     * Returns merged results set.
     *
     * @return result set with rows from all appended result sets
     */
    public SimpleResultSet getResult() {
        SimpleResultSet rs = new SimpleResultSet();
        for (SimpleColumnInfo ci : columns) {
            rs.addColumn(ci.name, ci.type, ci.typeName, ci.precision, ci.scale);
        }
        for (Map<SimpleColumnInfo, Object> map : data) {
            Object[] row = new Object[columns.size()];
            for (Map.Entry<SimpleColumnInfo, Object> entry : map.entrySet()) {
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
