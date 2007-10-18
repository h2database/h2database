/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.SQLException;

public class IndexColumn {
    public String columnName;
    public Column column;
    public boolean descending;
    
    public String getSQL() {
        StringBuffer buff = new StringBuffer(column.getSQL());
        if (descending) {
            buff.append(" DESC");
        }
        return buff.toString();
    }
    
    public static IndexColumn[] wrap(Column[] columns) {
        IndexColumn[] list = new IndexColumn[columns.length];
        for (int i = 0; i < list.length; i++) {
            list[i] = new IndexColumn();
            list[i].column = columns[i];
        }
        return list;
    }

    public static void mapColumns(IndexColumn[] indexColumns, Table table) throws SQLException {
        for (int i = 0; i < indexColumns.length; i++) {
            IndexColumn col = indexColumns[i];
            col.column = table.getColumn(col.columnName);
        }
    }
}
