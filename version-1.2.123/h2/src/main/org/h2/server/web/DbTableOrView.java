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
 * Contains meta data information about a table or a view.
 * This class is used by the H2 Console.
 */
public class DbTableOrView {

    /**
     * The schema this table belongs to.
     */
    DbSchema schema;

    /**
     * The table name.
     */
    String name;

    /**
     * The quoted table name.
     */
    String quotedName;

    /**
     * True if this represents a view.
     */
    boolean isView;

    /**
     * The column list.
     */
    DbColumn[] columns;

    DbTableOrView(DbSchema schema, ResultSet rs) throws SQLException {
        this.schema = schema;
        name = rs.getString("TABLE_NAME");
        String type = rs.getString("TABLE_TYPE");
        isView = "VIEW".equals(type);
        quotedName = schema.contents.quoteIdentifier(name);
    }

    /**
     * Read the column for this table from the database meta data.
     *
     * @param meta the database meta data
     */
    void readColumns(DatabaseMetaData meta) throws SQLException {
        ResultSet rs = meta.getColumns(null, schema.name, name, null);
        ArrayList<DbColumn> list = New.arrayList();
        while (rs.next()) {
            DbColumn column = new DbColumn(rs);
            list.add(column);
        }
        rs.close();
        columns = new DbColumn[list.size()];
        list.toArray(columns);
    }

}
