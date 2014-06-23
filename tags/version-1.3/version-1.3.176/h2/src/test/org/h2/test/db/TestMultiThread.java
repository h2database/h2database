/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
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
import org.h2.util.SmallLRUCache;
import org.h2.util.SynchronizedVerifier;
import org.h2.util.Task;

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

    private TestMultiThread(TestAll config, TestMultiThread parent)
            throws SQLException {
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

    @Override
    public void test() throws Exception {
        testConcurrentView();
        testConcurrentAlter();
        testConcurrentAnalyze();
        testConcurrentInsertUpdateSelect();
    }

    private void testConcurrentView() throws Exception {
        if (config.mvcc) {
            return;
        }
        String db = "concurrentView";
        deleteDb(db);
        final String url = getURL(db + ";MULTI_THREADED=1", true);
        final Random r = new Random();
        Connection conn = getConnection(url);
        Statement stat = conn.createStatement();
        StringBuilder buff = new StringBuilder();
        buff.append("create table test(id int");
        final int len = 3;
        for (int i = 0; i < len; i++) {
            buff.append(", x" + i + " int");
        }
        buff.append(")");
        stat.execute(buff.toString());
        stat.execute("create view test_view as select * from test");
        stat.execute("insert into test(id) select x from system_range(1, 2)");
        Task t = new Task() {
            @Override
            public void call() throws Exception {
                Connection c2 = getConnection(url);
                while (!stop) {
                    c2.prepareStatement("select * from test_view where x" +
                            r.nextInt(len) + "=1");
                }
                c2.close();
            }
        };
        t.execute();
        SynchronizedVerifier.setDetect(SmallLRUCache.class, true);
        for (int i = 0; i < 1000; i++) {
            conn.prepareStatement("select * from test_view where x" +
                    r.nextInt(len) + "=1");
        }
        t.get();
        SynchronizedVerifier.setDetect(SmallLRUCache.class, false);
        conn.close();
    }

    private void testConcurrentAlter() throws Exception {
        deleteDb("concurrentAlter");
        final Connection conn = getConnection("concurrentAlter");
        Statement stat = conn.createStatement();
        Task t = new Task() {
            @Override
            public void call() throws Exception {
                while (!stop) {
                    conn.prepareStatement("select * from test");
                }
            }
        };
        stat.execute("create table test(id int)");
        t.execute();
        for (int i = 0; i < 200; i++) {
            stat.execute("alter table test add column x int");
            stat.execute("alter table test drop column x");
        }
        t.get();
        conn.close();
        deleteDb("concurrentAlter");
    }

    private void testConcurrentAnalyze() throws Exception {
        if (config.mvcc) {
            return;
        }
        deleteDb("concurrentAnalyze");
        final String url = getURL("concurrentAnalyze;MULTI_THREADED=1", true);
        Connection conn = getConnection(url);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id bigint primary key) " +
                "as select x from system_range(1, 1000)");
        Task t = new Task() {
            @Override
            public void call() throws SQLException {
                Connection conn2;
                conn2 = getConnection(url);
                for (int i = 0; i < 1000; i++) {
                    conn2.createStatement().execute("analyze");
                }
                conn2.close();
            }
        };
        t.execute();
        Thread.yield();
        for (int i = 0; i < 1000; i++) {
            conn.createStatement().execute("analyze");
        }
        t.get();
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

    @Override
    public void run() {
        try {
            while (!parent.stop) {
                threadStat.execute("SELECT COUNT(*) FROM TEST");
                threadStat.execute("INSERT INTO TEST VALUES(NULL, 'Hi')");
                PreparedStatement prep = threadConn.prepareStatement(
                        "UPDATE TEST SET NAME='Hello' WHERE ID=?");
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
