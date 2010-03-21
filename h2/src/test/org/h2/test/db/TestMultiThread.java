/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import org.h2.test.TestAll;
import org.h2.test.TestBase;

/**
 * Multi-threaded tests.
 */
public class TestMultiThread extends TestBase implements Runnable {

    private boolean stop;
    private TestMultiThread parent;
    private Random random;
    private Connection threadConn;
    private Statement threadStat;

    public TestMultiThread() {
        // nothing to do
    }

    private TestMultiThread(TestAll config, TestMultiThread parent) throws SQLException {
        this.config = config;
        this.parent = parent;
        random = new Random();
        threadConn = getConnection();
        threadStat = threadConn.createStatement();
    }

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        testConcurrentAnalyze();
        testConcurrentInsertUpdateSelect();
    }

    private void testConcurrentAnalyze() throws Exception {
        if (config.mvcc) {
            return;
        }
        deleteDb("concurrentAnalyze");
        final String url = getURL("concurrentAnalyze;MULTI_THREADED=1", true);
        Connection conn = getConnection(url);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id bigint primary key) as select x from system_range(1, 1000)");
        final Exception[] ex = new Exception[1];
        Thread t = new Thread() {
            public void run() {
                try {
                    Connection conn2;
                    conn2 = getConnection(url);
                    for (int i = 0; i < 1000; i++) {
                        conn2.createStatement().execute("analyze");
                    }
                    conn2.close();
                } catch (Exception e) {
                    ex[0] = e;
                }
            }
        };
        t.start();
        Thread.yield();
        for (int i = 0; i < 1000; i++) {
            conn.createStatement().execute("analyze");
        }
        t.join();
        if (ex[0] != null) {
            throw ex[0];
        }
        stat.execute("drop table test");
        conn.close();
        deleteDb("concurrentAnalyze");
    }

    private void testConcurrentInsertUpdateSelect() throws Exception {
        threadConn = getConnection();
        threadStat = threadConn.createStatement();
        threadStat.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
        int len = getSize(10, 200);
        Thread[] threads = new Thread[len];
        for (int i = 0; i < len; i++) {
            threads[i] = new Thread(new TestMultiThread(config, this));
        }
        for (int i = 0; i < len; i++) {
            threads[i].start();
        }
        int sleep = getSize(400, 10000);
        Thread.sleep(sleep);
        this.stop = true;
        for (int i = 0; i < len; i++) {
            threads[i].join();
        }
        ResultSet rs = threadStat.executeQuery("SELECT COUNT(*) FROM TEST");
        rs.next();
        trace("max id=" + rs.getInt(1));
        threadConn.close();
    }

    private Connection getConnection() throws SQLException {
        return getConnection("jdbc:h2:mem:multiThread");
    }

    public void run() {
        try {
            while (!parent.stop) {
                threadStat.execute("SELECT COUNT(*) FROM TEST");
                threadStat.execute("INSERT INTO TEST VALUES(NULL, 'Hi')");
                PreparedStatement prep = threadConn.prepareStatement("UPDATE TEST SET NAME='Hello' WHERE ID=?");
                prep.setInt(1, random.nextInt(10000));
                prep.execute();
                prep = threadConn.prepareStatement("SELECT * FROM TEST WHERE ID=?");
                prep.setInt(1, random.nextInt(10000));
                ResultSet rs = prep.executeQuery();
                while (rs.next()) {
                    rs.getString("NAME");
                }
            }
            threadConn.close();
        } catch (Exception e) {
            logError("multi", e);
        }
    }

}
