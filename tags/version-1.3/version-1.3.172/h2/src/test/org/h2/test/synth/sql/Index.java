/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.sql;

/**
 * Represents an index.
 */
public class Index {
    private final Table table;
    private final String name;
    private final Column[] columns;
    private final boolean unique;

    Index(Table table, String name, Column[] columns, boolean unique) {
        this.table = table;
        this.name = name;
        this.columns = columns;
        this.unique = unique;
    }

    String getName() {
        return name;
    }

    String getCreateSQL() {
        String sql = "CREATE ";
        if (unique) {
            sql += "UNIQUE ";
        }
        sql += "INDEX " + name + " ON " + table.getName() + "(";
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                sql += ", ";
            }
            sql += columns[i].getName();
        }
        sql += ")";
        return sql;
    }

    String getDropSQL() {
        return "DROP INDEX " + name;
    }

    Table getTable() {
        return table;
    }

}
