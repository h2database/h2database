/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Contains meta data information about a database schema.
 * This class is used by the H2 Console.
 */
public class DbSchema {
    DbContents contents;
    String name;
    String quotedName;

    DbTableOrView[] tables;
    public boolean isDefault;

    DbSchema(DbContents contents, String name, boolean isDefault) throws SQLException {
        this.contents = contents;
        this.name = name;
        this.quotedName =  contents.quoteIdentifier(name);
        this.isDefault = isDefault;
    }

    public void readTables(DatabaseMetaData meta, String[] tableTypes) throws SQLException {
        ResultSet rs = meta.getTables(null, name, null, tableTypes);
        ArrayList list = new ArrayList();
        while (rs.next()) {
            DbTableOrView table = new DbTableOrView(this, rs);
            if (contents.isOracle && table.name.indexOf('$') > 0) {
                continue;
            }
            list.add(table);
        }
        rs.close();
        tables = new DbTableOrView[list.size()];
        list.toArray(tables);
        for (int i = 0; i < tables.length; i++) {
            DbTableOrView tab = tables[i];
            tab.readColumns(meta);
        }
    }

}
