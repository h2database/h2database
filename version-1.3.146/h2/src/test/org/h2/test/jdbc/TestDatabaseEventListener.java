/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
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

    private static boolean calledOpened, calledClosingDatabase, calledScan, calledCreateIndex;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        testInit();
        testIndexRebuiltOnce();
        testIndexNotRebuilt();
        testCalled();
        testCloseLog0(false);
        testCloseLog0(true);
        deleteDb("databaseEventListener");
    }

    /**
     * Initialize the database after opening.
     */
    public static class Init implements DatabaseEventListener {

        private String databaseUrl;

        public void init(String url) {
            databaseUrl = url;
        }

        public void opened() {
            try {
                Connection conn = DriverManager.getConnection(databaseUrl, "sa", "sa");
                Statement stat = conn.createStatement();
                stat.execute("create table if not exists test(id int)");
                conn.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        public void closingDatabase() {
            // nothing to do
        }

        public void diskSpaceIsLow() {
            // nothing to do
        }

        public void exceptionThrown(SQLException e, String sql) {
            // nothing to do
        }

        public void setProgress(int state, String name, int x, int max) {
            // nothing to do
        }

    }

    private void testInit() throws SQLException {
        if (config.networked || config.cipher != null || config.memory) {
            return;
        }
        deleteDb("databaseEventListener");
        String url = getURL("databaseEventListener", true);
        url += ";DATABASE_EVENT_LISTENER='"+ Init.class.getName() + "'";
        Connection conn = DriverManager.getConnection(url, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("select * from test");
        conn.close();
    }

    private void testIndexRebuiltOnce() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("databaseEventListener");
        String url = getURL("databaseEventListener", true);
        String user = getUser(), password = getPassword();
        Properties p = new Properties();
        p.setProperty("user", user);
        p.setProperty("password", password);
        Connection conn;
        Statement stat;
        conn = DriverManager.getConnection(url, p);
        stat = conn.createStatement();
        // the old.id index head is at position 0
        stat.execute("create table old(id identity) as select 1");
        // the test.id index head is at position 1
        stat.execute("create table test(id identity) as select 1");
        conn.close();
        conn = DriverManager.getConnection(url, p);
        stat = conn.createStatement();
        // free up space at position 0
        stat.execute("drop table old");
        stat.execute("insert into test values(2)");
        stat.execute("checkpoint sync");
        stat.execute("shutdown immediately");
        try {
            conn.close();
        } catch (SQLException e) {
            assertKnownException(e);
        }
        // now the index should be re-built
        conn = DriverManager.getConnection(url, p);
        conn.close();
        calledCreateIndex = false;
        p.put("DATABASE_EVENT_LISTENER", getClass().getName());
        conn = org.h2.Driver.load().connect(url, p);
        conn.close();
        assertTrue(!calledCreateIndex);
    }

    private void testIndexNotRebuilt() throws SQLException {
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
        // the old.id index head is at position 0
        stat.execute("create table old(id identity) as select 1");
        // the test.id index head is at position 1
        stat.execute("create table test(id identity) as select 1");
        conn.close();
        conn = DriverManager.getConnection(url, p);
        stat = conn.createStatement();
        // free up space at position 0
        stat.execute("drop table old");
        // truncate, relocating to position 0
        stat.execute("truncate table test");
        stat.execute("insert into test select 1");
        conn.close();
        calledCreateIndex = false;
        p.put("DATABASE_EVENT_LISTENER", getClass().getName());
        conn = org.h2.Driver.load().connect(url, p);
        conn.close();
        assertTrue(!calledCreateIndex);
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
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test select x, space(1000) from system_range(1,1000)");
        if (shutdown) {
            stat.execute("shutdown");
        }
        conn.close();

        calledOpened = false;
        calledScan = false;
        p.put("DATABASE_EVENT_LISTENER", getClass().getName());
        conn = org.h2.Driver.load().connect(url, p);
        conn.close();
        if (calledOpened) {
            assertTrue(!calledScan);
        }
    }

    private void testCalled() throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", "sa");
        p.setProperty("password", "sa");
        calledOpened = false;
        calledClosingDatabase = false;
        p.put("DATABASE_EVENT_LISTENER", getClass().getName());
        org.h2.Driver.load();
        String url = "jdbc:h2:mem:databaseEventListener";
        Connection conn = org.h2.Driver.load().connect(url, p);
        conn.close();
        assertTrue(calledOpened);
        assertTrue(calledClosingDatabase);
    }

    public void closingDatabase() {
        calledClosingDatabase = true;
    }

    public void diskSpaceIsLow() {
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
        if (state == DatabaseEventListener.STATE_CREATE_INDEX) {
            if (!name.startsWith("SYS:")) {
                calledCreateIndex = true;
            }
        }
    }

}
