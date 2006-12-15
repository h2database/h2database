/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.*;

import org.h2.tools.Server;

public class TestRemoteMetaData {

    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        Server server = org.h2.tools.Server.createTcpServer(new String[0]);
        server.start();
        
        String url = "jdbc:h2:tcp://localhost/test;TRACE_LEVEL_FILE=3";
        Connection conn = DriverManager.getConnection(url, "sa", "sa");
        DatabaseMetaData meta = conn.getMetaData();
    
        ResultSet rsTables = meta.getTables(null, null, null, null);
        while(rsTables.next()) {
            if (rsTables.getString(4).equals("TABLE")) {
                String name = rsTables.getString("TABLE_NAME");
                meta.getExportedKeys(null, null, name);
                meta.getImportedKeys(null, null, name);
            }
        }
        server.stop();
    }
}
