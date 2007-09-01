/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.jdbc.JdbcConnection;
import org.h2.store.FileLister;
import org.h2.test.TestBase;
import org.h2.util.FileUtils;
import org.h2.util.JdbcUtils;

public class TestPowerOff extends TestBase {

    private String dbName = "powerOff";
    private String dir, url;

    private int maxPowerOffCount;

    public void test() throws Exception {
        if (config.memory || config.logMode == 0) {
            return;
        }
        if (config.big) {
            dir = baseDir;
        } else {
            dir = "inmemory:";
        }
        url = dir + "/" + dbName + ";file_lock=no";
        testSummaryCrash();
        testCrash();
        testShutdown();
        testNoIndexFile();
        testMemoryTables();
        testPersistentTables();
    }

    private void testSummaryCrash() throws Exception {
        if (config.networked) {
            return;
        }
        deleteDb(dir, dbName);
        Connection conn = getConnection(url);
        Statement stat = conn.createStatement();
        for (int i = 0; i < 10; i++) {
            stat.execute("CREATE TABLE TEST" + i + "(ID INT PRIMARY KEY, NAME VARCHAR)");
            for (int j = 0; j < 10; j++) {
                stat.execute("INSERT INTO TEST" + i + " VALUES(" + j + ", 'Hello')");
            }
        }
        for (int i = 0; i < 10; i += 2) {
            stat.execute("DROP TABLE TEST" + i);
        }
        stat.execute("SET WRITE_DELAY 0");
        stat.execute("CHECKPOINT");
        for (int j = 0; j < 10; j++) {
            stat.execute("INSERT INTO TEST1 VALUES(" + (10 + j) + ", 'World')");
        }
        stat.execute("SHUTDOWN IMMEDIATELY");
        JdbcUtils.closeSilently(conn);
        conn = getConnection(url);
        stat = conn.createStatement();
        for (int i = 1; i < 10; i += 2) {
            ResultSet rs = stat.executeQuery("SELECT * FROM TEST" + i + " ORDER BY ID");
            for (int j = 0; j < 10; j++) {
                rs.next();
                check(rs.getInt(1), j);
                check(rs.getString(2), "Hello");
            }
            if (i == 1) {
                for (int j = 0; j < 10; j++) {
                    rs.next();
                    check(rs.getInt(1), j + 10);
                    check(rs.getString(2), "World");
                }
            }
            checkFalse(rs.next());
        }
        conn.close();
    }

    private void testCrash() throws Exception {
        if (config.networked) {
            return;
        }
        deleteDb(dir, dbName);
        Random random = new Random(1);
        SysProperties.runFinalize = false;
        int repeat = getSize(1, 20);
        for (int i = 0; i < repeat; i++) {
            Connection conn = getConnection(url);
            conn.close();
            conn = getConnection(url);
            Statement stat = conn.createStatement();
            stat.execute("SET WRITE_DELAY 0");
            ((JdbcConnection) conn).setPowerOffCount(random.nextInt(100));
            try {
                stat.execute("DROP TABLE IF EXISTS TEST");
                stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
                conn.setAutoCommit(false);
                int len = getSize(3, 100);
                for (int j = 0; j < len; j++) {
                    stat.execute("INSERT INTO TEST VALUES(" + j + ", 'Hello')");
                    if (random.nextInt(5) == 0) {
                        conn.commit();
                    }
                    if (random.nextInt(10) == 0) {
                        stat.execute("DROP TABLE IF EXISTS TEST");
                        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
                    }
                }
                stat.execute("DROP TABLE IF EXISTS TEST");
                conn.close();
            } catch (SQLException e) {
                if (!e.getSQLState().equals("90098")) {
                    TestBase.logError("power", e);
                }
            }
        }
    }

    private void testShutdown() throws Exception {
        deleteDb(dir, dbName);
        Connection conn = getConnection(url);
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        stat.execute("SHUTDOWN");
        conn.close();

        conn = getConnection(url);
        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        check(rs.next());
        checkFalse(rs.next());
        conn.close();
    }

    private void testNoIndexFile() throws Exception {
        if (config.networked) {
            return;
        }
        deleteDb(dir, dbName);
        Connection conn = getConnection(url);
        Statement stat = conn.createStatement();
        stat.execute("CREATE MEMORY TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        ((JdbcConnection) conn).setPowerOffCount(1);
        try {
            stat.execute("INSERT INTO TEST VALUES(2, 'Hello')");
            stat.execute("CHECKPOINT");
            error("should not work!");
        } catch (SQLException e) {
            // expected
        }
        boolean deleted = false;
        ArrayList files = FileLister.getDatabaseFiles(dir, dbName, false);
        for (int i = 0; i < files.size(); i++) {
            String fileName = (String) files.get(i);
            if (fileName.endsWith(Constants.SUFFIX_INDEX_FILE)) {
                FileUtils.delete(fileName);
                deleted = true;
            }
        }
        check(deleted);
        conn = getConnection(url);
        conn.close();
    }

    private void testMemoryTables() throws Exception {
        if (config.networked) {
            return;
        }
        deleteDb(dir, dbName);

        Connection conn = getConnection(url);
        Statement stat = conn.createStatement();
        stat.execute("CREATE MEMORY TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        stat.execute("CHECKPOINT");
        ((JdbcConnection) conn).setPowerOffCount(1);
        try {
            stat.execute("INSERT INTO TEST VALUES(2, 'Hello')");
            stat.execute("INSERT INTO TEST VALUES(3, 'Hello')");
            stat.execute("CHECKPOINT");
            error("should have failed!");
        } catch (Exception e) {
            // ok
        }

        ((JdbcConnection) conn).setPowerOffCount(0);
        conn = getConnection(url);
        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM TEST");
        rs.next();
        check(rs.getInt(1), 1);
        conn.close();
    }

    private void testPersistentTables() throws Exception {
        if (config.networked) {
            return;
        }
        deleteDb(dir, dbName);

        // ((JdbcConnection)conn).setPowerOffCount(Integer.MAX_VALUE);
        testRun(true);
        int max = maxPowerOffCount;
        trace("max=" + max);
        runTest(0, max, true);
        recoverAndCheckConsistency();
        runTest(0, max, false);
        recoverAndCheckConsistency();
    }

    void runTest(int min, int max, boolean withConsistencyCheck) throws Exception {
        for (int i = min; i < max; i++) {
            deleteDb(dir, dbName);
            Database.setInitialPowerOffCount(i);
            int expect = testRun(false);
            if (withConsistencyCheck) {
                int got = recoverAndCheckConsistency();
                trace("test " + i + " of " + max + " expect=" + expect + " got=" + got);
            } else {
                trace("test " + i + " of " + max + " expect=" + expect);
            }
        }
        Database.setInitialPowerOffCount(0);
    }

    int testRun(boolean init) throws Exception {
        if (init) {
            Database.setInitialPowerOffCount(Integer.MAX_VALUE);
        }
        int state = 0;
        try {
            Connection conn = getConnection(url);
            Statement stat = conn.createStatement();
            stat.execute("SET WRITE_DELAY 0");
            stat.execute("CREATE TABLE IF NOT EXISTS TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
            state = 1;
            conn.setAutoCommit(false);
            stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
            stat.execute("INSERT INTO TEST VALUES(2, 'World')");
            conn.commit();
            state = 2;
            stat.execute("UPDATE TEST SET NAME='Hallo' WHERE ID=1");
            stat.execute("UPDATE TEST SET NAME='Welt' WHERE ID=2");
            conn.commit();
            state = 3;
            stat.execute("DELETE FROM TEST WHERE ID=1");
            stat.execute("DELETE FROM TEST WHERE ID=2");
            conn.commit();
            state = 1;
            stat.execute("DROP TABLE TEST");
            state = 0;
            if (init) {
                maxPowerOffCount = Integer.MAX_VALUE - ((JdbcConnection) conn).getPowerOffCount();
            }
            conn.close();
        } catch (SQLException e) {
            if (e.getSQLState().equals("90098")) {
                // this is ok
            } else {
                throw e;
            }
        }
        return state;
    }

    int recoverAndCheckConsistency() throws Exception {
        int state;
        Database.setInitialPowerOffCount(0);
        Connection conn = getConnection(url);
        if (((JdbcConnection) conn).getPowerOffCount() != 0) {
            error("power off count is not 0");
        }
        Statement stat = conn.createStatement();
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs = meta.getTables(null, null, "TEST", null);
        if (!rs.next()) {
            state = 0;
        } else {
            // table does not exist
            rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
            if (!rs.next()) {
                state = 1;
            } else {
                check(rs.getInt(1), 1);
                String name1 = rs.getString(2);
                check(rs.next());
                check(rs.getInt(1), 2);
                String name2 = rs.getString(2);
                checkFalse(rs.next());
                if ("Hello".equals(name1)) {
                    check(name2, "World");
                    state = 2;
                } else {
                    check(name1, "Hallo");
                    check(name2, "Welt");
                    state = 3;
                }
            }
        }
        conn.close();
        return state;
    }

}
