/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.rowlock;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;

/**
 * Row level locking tests.
 */
public class TestRowLocks extends TestBase {

    private Connection c1, c2;
    private Statement s1, s2;

    public void test() throws Exception {
        testSetMode();
        testCases();
    }

    private void testSetMode() throws Exception {
        deleteDb("rowLocks");
        c1 = getConnection("rowLocks");
        Statement stat = c1.createStatement();
        stat.execute("SET LOCK_MODE 4");
        ResultSet rs = stat.executeQuery("call lock_mode()");
        rs.next();
        assertEquals("4", rs.getString(1));
        c1.close();
    }

    private void testCases() throws Exception {
        deleteDb("rowLocks");
        c1 = getConnection("rowLocks;MVCC=TRUE");
        s1 = c1.createStatement();
        s1.execute("SET LOCK_MODE 4");
        s1.execute("CREATE TABLE TEST AS SELECT X ID, 'Hello' NAME FROM SYSTEM_RANGE(1, 3)");
        c1.commit();
        c1.setAutoCommit(false);
        s1.execute("UPDATE TEST SET NAME='Hallo' WHERE ID=1");
        c2 = getConnection("rowLocks");
        c2.setAutoCommit(false);
        s2 = c2.createStatement();

        ResultSet rs = s1.executeQuery("SELECT NAME FROM TEST WHERE ID=1");
        rs.next();
        assertEquals("Hallo", rs.getString(1));

        rs = s2.executeQuery("SELECT NAME FROM TEST WHERE ID=1");
        rs.next();
        assertEquals("Hello", rs.getString(1));

        s2.execute("UPDATE TEST SET NAME='Hallo' WHERE ID=2");
        try {
            s2.executeUpdate("UPDATE TEST SET NAME='Hallo2' WHERE ID=1");
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }
        c1.commit();
        c2.commit();
        rs = s1.executeQuery("SELECT NAME FROM TEST WHERE ID=1");
        rs.next();
        assertEquals("Hallo", rs.getString(1));
        rs = s2.executeQuery("SELECT NAME FROM TEST WHERE ID=1");
        rs.next();
        assertEquals("Hallo", rs.getString(1));
        c1.close();
        c2.close();
    }

}
