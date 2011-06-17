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
import java.util.ArrayList;
import org.h2.util.New;

/**
 * Contains meta data information about a database schema.
 * This class is used by the H2 Console.
 */
public class DbSchema {

    /**
     * Up to this many tables, the column type and indexes are listed.
     */
    static final int MAX_TABLES_LIST_INDEXES = 100;

    /**
     * Up to this many tables, the column names are listed.
     */
    static final int MAX_TABLES_LIST_COLUMNS = 500;

    /**
     * The database content container.
     */
    final DbContents contents;

    /**
     * The schema name.
     */
    final String name;

    /**
     * True if this is the default schema for this database.
     */
    final boolean isDefault;

    /**
     * True if this is a system schema (for example the INFORMATION_SCHEMA).
     */
    final boolean isSystem;

    /**
     * The quoted schema name.
     */
    final String quotedName;

    /**
     * The table list.
     */
    DbTableOrView[] tables;

    DbSchema(DbContents contents, String name, boolean isDefault) {
        this.contents = contents;
        this.name = name;
        this.quotedName =  contents.quoteIdentifier(name);
        this.isDefault = isDefault;
        if (name.toUpperCase().startsWith("INFO")) {
            isSystem = true;
        } else if (contents.isPostgreSQL && name.toUpperCase().startsWith("PG_")) {
            isSystem = true;
        } else if (contents.isDerby && name.startsWith("SYS")) {
            isSystem = true;
        } else {
            isSystem = false;
        }
    }

    /**
     * Read all tables for this schema from the database meta data.
     *
     * @param meta the database meta data
     * @param tableTypes the table types to read
     */
    void readTables(DatabaseMetaData meta, String[] tableTypes) throws SQLException {
        ResultSet rs = meta.getTables(null, name, null, tableTypes);
        ArrayList<DbTableOrView> list = New.arrayList();
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
        if (tables.length < MAX_TABLES_LIST_COLUMNS) {
            for (DbTableOrView tab : tables) {
                tab.readColumns(meta);
            }
        }
    }

}
