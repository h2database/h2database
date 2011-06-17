/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.h2.api.DatabaseEventListener;
import org.h2.test.TestBase;

/**
 * Tests the DatabaseEventListener interface.
 */
public class TestDatabaseEventListener extends TestBase implements DatabaseEventListener {

    private boolean calledOpened, calledClosingDatabase, calledScan;

    public void test() throws SQLException {
        testCalled();
        testCloseLog0(false);
        testCloseLog0(true);
    }

    private void testCloseLog0(boolean shutdown) throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("databaseEventListener");
        String url = getURL("databaseEventListener", true);
        String user = getUser(), password = getPassword();
        Properties p = new Properties();
        p.setProperty("user", user);
        p.setProperty("password", password);
        Connection conn = DriverManager.getConnection(url, p);
        Statement stat = conn.createStatement();
        stat.execute("set log 0");
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test select x, space(1000) from system_range(1,1000)");
        if (shutdown) {
            stat.execute("shutdown");
        }
        conn.close();

        TestDatabaseEventListener l = new TestDatabaseEventListener();
        p.put("DATABASE_EVENT_LISTENER_OBJECT", l);
        conn = org.h2.Driver.load().connect(url, p);
        conn.close();
        if (l.calledOpened) {
            assertTrue(!l.calledScan);
        }
    }

    private void testCalled() throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", "sa");
        p.setProperty("password", "sa");
        TestDatabaseEventListener l = new TestDatabaseEventListener();
        p.put("DATABASE_EVENT_LISTENER_OBJECT", l);
        org.h2.Driver.load();
        String url = "jdbc:h2:mem:databaseEventListener";
        Connection conn = org.h2.Driver.load().connect(url, p);
        conn.close();
        assertTrue(l.calledOpened);
        assertTrue(l.calledClosingDatabase);
    }

    public void closingDatabase() {
        calledClosingDatabase = true;
    }

    public void diskSpaceIsLow(long stillAvailable) {
        // nothing to do
    }

    public void exceptionThrown(SQLException e, String sql) {
        // nothing to do
    }

    public void init(String url) {
        // nothing to do
    }

    public void opened() {
        calledOpened = true;
    }

    public void setProgress(int state, String name, int x, int max) {
        if (state == DatabaseEventListener.STATE_SCAN_FILE) {
            calledScan = true;
        }
    }

}
