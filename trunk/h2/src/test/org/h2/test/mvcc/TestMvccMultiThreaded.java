/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.mvcc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import org.h2.test.TestBase;

/**
 * Multi-threaded MVCC (multi version concurrency) test cases.
 */
public class TestMvccMultiThreaded extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        testConcurrentMerge();
        testConcurrentUpdate("");
        // not supported currently
        // testConcurrentUpdate(";MULTI_THREADED=TRUE");
    }

    private void testConcurrentMerge() throws Exception {
        deleteDb("mvccMultiThreaded");
        int len = 3;
        final Connection[] connList = new Connection[len];
        for (int i = 0; i < len; i++) {
            Connection conn = getConnection("mvccMultiThreaded;MVCC=TRUE;LOCK_TIMEOUT=1000");
            connList[i] = conn;
        }
        Connection conn = connList[0];
        conn.createStatement().execute("create table test(id int primary key, name varchar)");
        final SQLException[] ex = { null };
        Thread[] threads = new Thread[len];
        final boolean[] stop = { false };
        for (int i = 0; i < len; i++) {
            final Connection c = connList[i];
            c.setAutoCommit(false);
            threads[i] = new Thread() {
                public void run() {
                    while (!stop[0]) {
                        try {
                            c.createStatement().execute("merge into test values(1, 'x')");
                            c.commit();
                        } catch (SQLException e) {
                            ex[0] = e;
                        }
                    }
                }
            };
            threads[i].start();
        }
        Thread.sleep(1000);
        stop[0] = true;
        for (int i = 0; i < len; i++) {
            threads[i].join();
        }
        if (ex[0] != null) {
            throw ex[0];
        }
        for (int i = 0; i < len; i++) {
            connList[i].close();
        }
        deleteDb("mvccMultiThreaded");
    }

    private void testConcurrentUpdate(String suffix) throws Exception {
        deleteDb("mvccMultiThreaded");
        int len = 2;
        final Connection[] connList = new Connection[len];
        for (int i = 0; i < len; i++) {
            connList[i] = getConnection("mvccMultiThreaded;MVCC=TRUE" + suffix);
        }
        Connection conn = connList[0];
        conn.createStatement().execute("create table test(id int primary key, value int)");
        conn.createStatement().execute("insert into test values(0, 0)");
        final Exception[] ex = { null };
        final int count = 1000;
        Thread[] threads = new Thread[len];

        final CountDownLatch latch = new CountDownLatch(len);

        for (int i = 0; i < len; i++) {
            final int x = i;
            threads[i] = new Thread() {
                public void run() {
                    for (int a = 0; a < count; a++) {
                        try {
                            connList[x].createStatement().execute("update test set value=value+1");
                            latch.countDown();
                            latch.await();
                        } catch (Exception e) {
                            ex[0] = e;
                            break;
                        }
                    }
                }
            };
            threads[i].start();
        }
        for (int i = 0; i < len; i++) {
            threads[i].join();
        }
        if (ex[0] != null) {
            throw ex[0];
        }
        ResultSet rs = conn.createStatement().executeQuery("select value from test");
        rs.next();
        assertEquals(count * len, rs.getInt(1));
        for (int i = 0; i < len; i++) {
            connList[i].close();
        }
        deleteDb("mvccMultiThreaded");
    }

}
