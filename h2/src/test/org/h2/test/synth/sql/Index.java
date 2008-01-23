/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.sql;

/**
 * Represents an index.
 */
public class Index {
    Table table;
    String name;
    Column[] columns;
    boolean unique;

    Index(Table table, String name, Column[] columns, boolean unique) {
        this.table = table;
        this.name = name;
        this.columns = columns;
        this.unique = unique;
    }

    public String getName() {
        return name;
    }

    public String getCreateSQL() {
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

    public String getDropSQL() {
        return "DROP INDEX " + name;
    }

    public Table getTable() {
        return table;
    }

}
