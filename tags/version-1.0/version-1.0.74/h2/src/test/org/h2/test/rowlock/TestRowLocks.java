/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.rowlock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;

/**
 * Row level locking tests.
 */
public class TestRowLocks extends TestBase {

    private Connection c1, c2;
    private Statement s1;

    public void test() throws Exception {
        testSetMode();
        testCases();
    }

    private void testSetMode() throws Exception {
        DeleteDbFiles.execute(null, "test", true);
        Class.forName("org.h2.Driver");
        c1 = DriverManager.getConnection("jdbc:h2:test", "sa", "sa");
        Statement stat = c1.createStatement();
        stat.execute("SET LOCK_MODE 4");
        ResultSet rs = stat.executeQuery("call lock_mode()");
        rs.next();
        assertEquals("4", rs.getString(1));
        c1.close();
    }

    private void testCases() throws Exception {
        deleteDb("rowLocks");
        c1 = getConnection("rowLocks");
        s1 = c1.createStatement();
        s1.execute("SET LOCK_MODE 4");
        c2 = getConnection("rowLocks");
        // s2 = c2.createStatement();
        
        c1.setAutoCommit(false);
        c2.setAutoCommit(false);
        
        c1.close();
        c2.close();
    }

}
