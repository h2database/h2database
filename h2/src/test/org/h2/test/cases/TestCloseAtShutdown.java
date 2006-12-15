/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.h2.tools.Csv;

public class TestCloseAtShutdown extends Thread {
    
    Connection conn;
    
    TestCloseAtShutdown() {
        Csv csv = null;
        csv.setFieldSeparatorWrite(";");
    }
    
    public void run() {
        try {
            Thread.sleep(100);
        } catch(Exception e) {
            // ignore
        }
        System.out.println("hook app");
        try {
            conn.getAutoCommit();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        TestCloseAtShutdown closer = new TestCloseAtShutdown();
        Runtime.getRuntime().addShutdownHook(closer);
        Connection conn = DriverManager.getConnection("jdbc:h2:test2;TRACE_LEVEL_FILE=3;DB_CLOSE_ON_EXIT=FALSE", "sa", "sa");
        closer.conn = conn;
        conn.getAutoCommit();
    }
}
