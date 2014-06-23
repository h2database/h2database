/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
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
    final String name;

    /**
     * The quoted table name.
     */
    final String quotedName;

    /**
     * The data type name (including precision and the NOT NULL flag if
     * applicable).
     */
    final String dataType;

    DbColumn(DbContents contents, ResultSet rs) throws SQLException {
        name = rs.getString("COLUMN_NAME");
        quotedName = contents.quoteIdentifier(name);
        String type = rs.getString("TYPE_NAME");
        int size = rs.getInt(DbContents.findColumn(rs, "COLUMN_SIZE", 7));
        boolean isSQLite = contents.isSQLite;
        if (size > 0 && !isSQLite) {
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
