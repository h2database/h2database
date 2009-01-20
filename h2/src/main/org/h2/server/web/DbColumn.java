/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Keeps the meta data information of a column.
 * This class is used by the H2 Console.
 */
class DbColumn {

    /**
     * The column name.
     */
    String name;

    /**
     * The data type name (including precision and the NOT NULL flag if
     * applicable).
     */
    String dataType;

    DbColumn(ResultSet rs) throws SQLException {
        name = rs.getString("COLUMN_NAME");
        String type = rs.getString("TYPE_NAME");
        int size = rs.getInt(DbContents.findColumn(rs, "COLUMN_SIZE", 7));
        if (size > 0) {
            type += "(" + size;
            int prec = rs.getInt(DbContents.findColumn(rs, "DECIMAL_DIGITS", 9));
            if (prec > 0) {
                type += ", " + prec;
            }
            type += ")";
        }
        if (rs.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls) {
            type += " NOT NULL";
        }
        dataType = type;
    }
}
