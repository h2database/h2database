/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.DatabaseEventListener;
import org.h2.test.TestBase;
import org.h2.tools.Restore;
import org.h2.util.FileUtils;

/**
 * Tests opening and closing a database.
 */
public class TestOpenClose extends TestBase implements DatabaseEventListener {

    int nextId = 10;

    public void test() throws Exception {
        testCloseDelay();
        testBackup(false);
        testBackup(true);
        testCase();
        testReconnectFast();
        deleteDb("openClose");
    }
    
    private void testCloseDelay() throws Exception {
        String url = getURL("openClose;DB_CLOSE_DELAY=1", true);
        String user = getUser(), password = getPassword();
        Connection conn = DriverManager.getConnection(url, user, password);
        conn.close();
        Thread.sleep(950);
        long time = System.currentTimeMillis();
        for (int i = 0; System.currentTimeMillis() - time < 100; i++) {
            conn = DriverManager.getConnection(url, user, password);
            conn.close();
        }
        conn = DriverManager.getConnection(url, user, password);
        conn.createStatement().execute("SHUTDOWN");
        conn.close();
    }

    private void testBackup(boolean encrypt) throws SQLException {
        deleteDb(baseDir, "openClose");
        String url;
        if (encrypt) {
            url = "jdbc:h2:" + baseDir + "/openClose;CIPHER=XTEA";
        } else {
            url = "jdbc:h2:" + baseDir + "/openClose";
        }
        org.h2.Driver.load();
        Connection conn = DriverManager.getConnection(url, "sa", "abc def");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(C CLOB)");
        stat.execute("INSERT INTO TEST VALUES(SPACE(10000))");
        stat.execute("BACKUP TO '" + baseDir + "/test.zip'");
        conn.close();
        deleteDb(baseDir, "openClose");
        Restore.execute(baseDir + "/test.zip", baseDir, null, true);
        conn = DriverManager.getConnection(url, "sa", "abc def");
        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        rs.next();
        assertEquals(rs.getString(1).length(), 10000);
        assertFalse(rs.next());
        conn.close();
        FileUtils.delete(baseDir + "/test.zip");
    }

    private void testReconnectFast() throws SQLException {
        deleteDb(baseDir, "openClose");
        String url = "jdbc:h2:" + baseDir + "/openClose;DATABASE_EVENT_LISTENER='" + TestOpenClose.class.getName()
                + "'";
        Connection conn = DriverManager.getConnection(url, "sa", "sa");
        Statement stat = conn.createStatement();
        try {
            stat.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
            stat.execute("SET MAX_MEMORY_UNDO 100000");
            stat.execute("CREATE INDEX IDXNAME ON TEST(NAME)");
            stat.execute("INSERT INTO TEST SELECT X, X || ' Data' FROM SYSTEM_RANGE(1, 1000)");
        } catch (SQLException e) {
            // ok
        }
        stat.close();
        conn.close();
        conn = DriverManager.getConnection(url, "sa", "sa");
        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM DUAL");
        if (rs.next()) {
            rs.getString(1);
        }
        rs.close();
        stat.close();
        conn.close();
        conn = DriverManager.getConnection(url, "sa", "sa");
        stat = conn.createStatement();
        // stat.execute("SET DB_CLOSE_DELAY 0");
        stat.executeUpdate("SHUTDOWN");
        stat.close();
        conn.close();
    }

    private void testCase() throws Exception {
        org.h2.Driver.load();
        deleteDb(baseDir, "openClose");
        final String url = "jdbc:h2:" + baseDir + "/openClose;FILE_LOCK=NO";
        Connection conn = DriverManager.getConnection(url, "sa", "");
        conn.createStatement().execute("drop table employee if exists");
        conn.createStatement().execute("create table employee(id int primary key, name varchar, salary int)");
        conn.close();
        int len = this.getSize(200, 1000);
        Thread[] threads = new Thread[len];
        for (int i = 0; i < len; i++) {
            threads[i] = new Thread() {
                public void run() {
                    try {
                        Connection conn = DriverManager.getConnection(url, "sa", "");
                        PreparedStatement prep = conn.prepareStatement("insert into employee values(?, ?, 0)");
                        int id = getNextId();
                        prep.setInt(1, id);
                        prep.setString(2, "employee " + id);
                        prep.execute();
                        conn.close();
                    } catch (Throwable e) {
                        TestBase.logError("insert", e);
                    }
                }
            };
            threads[i].start();
        }
        // for(int i=0; i<len; i++) {
        // threads[i].start();
        // }
        for (int i = 0; i < len; i++) {
            threads[i].join();
        }
        conn = DriverManager.getConnection(url, "sa", "");
        ResultSet rs = conn.createStatement().executeQuery("select count(*) from employee");
        rs.next();
        assertEquals(rs.getInt(1), len);
        conn.close();
    }

    synchronized int getNextId() {
        return nextId++;
    }

    public void diskSpaceIsLow(long stillAvailable) throws SQLException {
        throw new SQLException("unexpected");
    }

    public void exceptionThrown(SQLException e, String sql) {
        throw new Error("unexpected: " + e + " sql: " + sql);
    }

    public void setProgress(int state, String name, int current, int max) {
        String stateName;
        switch (state) {
        case STATE_SCAN_FILE:
            stateName = "Scan " + name + " " + current + "/" + max;
            if (current > 0) {
                throw new Error("unexpected: " + stateName);
            }
            break;
        case STATE_CREATE_INDEX:
            stateName = "Create Index " + name + " " + current + "/" + max;
            if (!"SYS:SYS_ID".equals(name)) {
                throw new Error("unexpected: " + stateName);
            }
            break;
        case STATE_RECOVER:
            stateName = "Recover " + current + "/" + max;
            break;
        default:
            stateName = "?";
        }
        // System.out.println(": " + stateName);
    }

    public void closingDatabase() {
        // nothing to do
    }

    public void init(String url) {
        // nothing to do
    }

    public void opened() {
        // nothing to do
    }

}
