/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.util.MemoryUtils;

/**
 * Tests the memory usage of the cache.
 */
public class TestMemoryUsage extends TestBase {

    private Connection conn;

    private void reconnect() throws Exception {
        if (conn != null) {
            conn.close();
        }
        // Class.forName("org.hsqldb.jdbcDriver");
        // conn = DriverManager.getConnection("jdbc:hsqldb:test", "sa", "");
        conn = getConnection("memoryUsage");
    }

    public void test() throws Exception {
        deleteDb("memoryUsage");
        testCreateIndex();
        testClob();
        deleteDb("memoryUsage");
        testReconnectOften();
        deleteDb("memoryUsage");
        reconnect();
        insertUpdateSelectDelete();
        reconnect();
        insertUpdateSelectDelete();
        conn.close();
    }

    private void testClob() throws Exception {
        if (config.memory || !config.big) {
            return;
        }
        Connection conn = getConnection("memoryUsage");
        Statement stat = conn.createStatement();
        stat.execute("SET MAX_LENGTH_INPLACE_LOB 32768");
        stat.execute("SET CACHE_SIZE 8000");
        stat.execute("CREATE TABLE TEST(ID IDENTITY, DATA CLOB)");
        System.gc();
        System.gc();
        int start = MemoryUtils.getMemoryUsed();
        for (int i = 0; i < 4; i++) {
            stat.execute("INSERT INTO TEST(DATA) SELECT SPACE(32000) FROM SYSTEM_RANGE(1, 200)");
            System.gc();
            System.gc();
            int used = MemoryUtils.getMemoryUsed();
            if ((used - start) > 16000) {
                fail("Used: " + (used - start));
            }
        }
        conn.close();
    }

    private void testCreateIndex() throws Exception {
        if (config.memory) {
            return;
        }
        Connection conn = getConnection("memoryUsage");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int, name varchar)");
        PreparedStatement prep = conn.prepareStatement("insert into test values(?, space(200) || ?)");
        int len = getSize(10000, 100000);
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            prep.setInt(2, i);
            prep.executeUpdate();
        }
        int start = MemoryUtils.getMemoryUsed();
        stat.execute("create index idx_test_id on test(id)");
        System.gc();
        System.gc();
        int used = MemoryUtils.getMemoryUsed();
        if ((used - start) > 5000) {
            fail("Used: " + (used - start));
        }
        stat.execute("drop table test");
        conn.close();
    }

    private void testReconnectOften() throws Exception {
        int len = getSize(1, 2000);
        Connection conn1 = getConnection("memoryUsage");
        printTimeMemory("start", 0);
        long time = System.currentTimeMillis();
        for (int i = 0; i < len; i++) {
            Connection conn2 = getConnection("memoryUsage");
            conn2.close();
            if (i % 10000 == 0) {
                printTimeMemory("connect", System.currentTimeMillis() - time);
            }
        }
        printTimeMemory("connect", System.currentTimeMillis() - time);
        conn1.close();
    }

    void insertUpdateSelectDelete() throws Exception {
        Statement stat = conn.createStatement();
        long time;
        int len = getSize(1, 2000);

        // insert
        time = System.currentTimeMillis();
        stat.execute("DROP TABLE IF EXISTS TEST");
        trace("drop=" + (System.currentTimeMillis() - time));
        stat.execute("CREATE CACHED TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, 'Hello World')");
        printTimeMemory("start", 0);
        time = System.currentTimeMillis();
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            prep.execute();
            if (i % 50000 == 0) {
                trace("  " + (100 * i / len) + "%");
            }
        }
        printTimeMemory("insert", System.currentTimeMillis() - time);

        // update
        time = System.currentTimeMillis();
        prep = conn.prepareStatement("UPDATE TEST SET NAME='Hallo Welt' || ID WHERE ID = ?");
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            prep.execute();
            if (i % 50000 == 0) {
                trace("  " + (100 * i / len) + "%");
            }
        }
        printTimeMemory("update", System.currentTimeMillis() - time);

        // select
        time = System.currentTimeMillis();
        prep = conn.prepareStatement("SELECT * FROM TEST WHERE ID = ?");
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            ResultSet rs = prep.executeQuery();
            rs.next();
            if (rs.next()) {
                fail("one row expected, got more");
            }
            if (i % 50000 == 0) {
                trace("  " + (100 * i / len) + "%");
            }
        }
        printTimeMemory("select", System.currentTimeMillis() - time);

        // select randomized
        Random random = new Random(1);
        time = System.currentTimeMillis();
        prep = conn.prepareStatement("SELECT * FROM TEST WHERE ID = ?");
        for (int i = 0; i < len; i++) {
            prep.setInt(1, random.nextInt(len));
            ResultSet rs = prep.executeQuery();
            rs.next();
            if (rs.next()) {
                fail("one row expected, got more");
            }
            if (i % 50000 == 0) {
                trace("  " + (100 * i / len) + "%");
            }
        }
        printTimeMemory("select randomized", System.currentTimeMillis() - time);

        // delete
        time = System.currentTimeMillis();
        prep = conn.prepareStatement("DELETE FROM TEST WHERE ID = ?");
        for (int i = 0; i < len; i++) {
            prep.setInt(1, random.nextInt(len));
            prep.executeUpdate();
            if (i % 50000 == 0) {
                trace("  " + (100 * i / len) + "%");
            }
        }
        printTimeMemory("delete", System.currentTimeMillis() - time);
    }

}
