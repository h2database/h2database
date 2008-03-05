/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (license2)
 * Initial Developer: H2 Group
 */
package org.h2.test.synth;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

import org.h2.test.TestBase;

/**
 * This test starts multiple threads and executes random operations in each
 * thread.
 */
public class TestThreads extends TestBase implements Runnable {

    public TestThreads() {
    }

    public void test() throws Exception {
        deleteDb("threads");
        Connection conn = getConnection("threads;MAX_LOG_SIZE=1");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST_A(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("CREATE TABLE TEST_B(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("CREATE TABLE TEST_C(ID INT PRIMARY KEY, NAME VARCHAR)");
        int len = 1000;
        insertRows(conn, "TEST_A", len);
        insertRows(conn, "TEST_B", len);
        insertRows(conn, "TEST_C", len);
        maxId = len;
        int threadCount = 4;
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            String table = random.nextBoolean() ? null : getRandomTable();
            int op = random.nextInt(OP_TYPES);
            op = i % 2 == 1 ? RECONNECT : CHECKPOINT;
            threads[i] = new Thread(new TestThreads(this, op, table));
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].start();
        }
        Thread.sleep(10000);
        stop = true;
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }
        conn.close();
        conn = getConnection("threads");
        checkTable(conn, "TEST_A");
        checkTable(conn, "TEST_B");
        checkTable(conn, "TEST_C");
        conn.close();
    }

    private void insertRows(Connection conn, String tableName, int len) throws Exception {
        PreparedStatement prep = conn.prepareStatement("INSERT INTO " + tableName + " VALUES(?, 'Hi')");
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            prep.execute();
        }
    }

    private void checkTable(Connection conn, String tableName) throws Exception {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT * FROM " + tableName + " ORDER BY ID");
        while (rs.next()) {
            int id = rs.getInt(1);
            String name = rs.getString(2);
            System.out.println("id=" + id + " name=" + name);
        }
    }

    private int maxId = 1;

    private volatile boolean stop;
    private TestThreads master;
    private int type;
    private String table;
    private Random random = new Random();

    private static final int INSERT = 0, UPDATE = 1, DELETE = 2;
    private static final int SELECT_ONE = 3, SELECT_ALL = 4, CHECKPOINT = 5, RECONNECT = 6;
    private static final int OP_TYPES = RECONNECT + 1;

    private int getMaxId() {
        return maxId;
    }

    private synchronized int incrementMaxId() {
        return maxId++;
    }

    TestThreads(TestThreads master, int type, String table) {
        this.master = master;
        this.type = type;
        this.table = table;
    }

    private String getRandomTable() {
        return "TEST_" + (char) ('A' + random.nextInt(3));
    }

    public void run() {
        try {
            String t = table == null ? getRandomTable() : table;
            Connection conn = master.getConnection("threads");
            Statement stat = conn.createStatement();
            ResultSet rs;
            int max = master.getMaxId();
            int rid = random.nextInt(max);
            for (int i = 0; !master.stop; i++) {
                switch (type) {
                case INSERT:
                    max = master.incrementMaxId();
                    stat.execute("INSERT INTO " + t + "(ID, NAME) VALUES(" + max + ", 'Hello')");
                    break;
                case UPDATE:
                    stat.execute("UPDATE " + t + " SET NAME='World " + rid + "' WHERE ID=" + rid);
                    break;
                case DELETE:
                    stat.execute("DELETE FROM " + t + " WHERE ID=" + rid);
                    break;
                case SELECT_ALL:
                    rs = stat.executeQuery("SELECT * FROM " + t + " ORDER BY ID");
                    while (rs.next()) {
                        // nothing
                    }
                    break;
                case SELECT_ONE:
                    rs = stat.executeQuery("SELECT * FROM " + t + " WHERE ID=" + rid);
                    while (rs.next()) {
                        // nothing
                    }
                    break;
                case CHECKPOINT:
                    stat.execute("CHECKPOINT");
                    break;
                case RECONNECT:
                    conn.close();
                    conn = master.getConnection("threads");
                    break;
                }
            }
            conn.close();
        } catch (Exception e) {
            TestBase.logError("error", e);
        }
    }

}
