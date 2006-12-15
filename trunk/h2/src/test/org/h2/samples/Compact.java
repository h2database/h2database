/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.h2.tools.Backup;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.RunScript;

public class Compact {
    public static void main(String[] args) throws Exception {
        DeleteDbFiles.execute(null, "test", true);
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:test", "sa", "");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World');");
        conn.close();
        
        System.out.println("Compacting...");
        compact(null, "test", "sa", "");
        System.out.println("Done.");
        
    }
    
    public static void compact(String dir, String dbName, String user, String password) throws Exception {
        String url = "jdbc:h2:" + dbName;
        String script = "test.sql";
        Backup.execute(url, user, password, script);
        DeleteDbFiles.execute(dir, dbName, true);
        RunScript.execute(url, user, password, script, null, false);
    }
}
