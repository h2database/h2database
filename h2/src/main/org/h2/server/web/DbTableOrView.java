/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class DbTableOrView {
    DbSchema schema;
    String name;
    String quotedName;
    boolean isView;
    DbColumn[] columns;
    
    DbTableOrView(DbSchema schema, ResultSet rs) throws SQLException {
        this.schema = schema;
        name = rs.getString("TABLE_NAME");
        String type = rs.getString("TABLE_TYPE");
        isView = "VIEW".equals(type);
        quotedName = schema.contents.quoteIdentifier(name);
    }
    
    public void readColumns(DatabaseMetaData meta) throws SQLException {
        ResultSet rs = meta.getColumns(null, schema.name, name, null);
        ArrayList list = new ArrayList();        
        while(rs.next()) {
            DbColumn column = new DbColumn(rs);
            list.add(column);
        }
        rs.close();
        columns = new DbColumn[list.size()];
        list.toArray(columns);
    }

}
