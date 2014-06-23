/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.mvcc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

import org.h2.constant.ErrorCode;
import org.h2.test.TestBase;
import org.h2.util.Task;

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

    @Override
    public void test() throws Exception {
        testMergeWithUniqueKeyViolation();
        testConcurrentMerge();
        testConcurrentUpdate("");
        // not supported currently
        // testConcurrentUpdate(";MULTI_THREADED=TRUE");
    }

    private void testMergeWithUniqueKeyViolation() throws Exception {
        deleteDb("mvccMultiThreaded");
        Connection conn = getConnection("mvccMultiThreaded");
        Statement stat = conn.createStatement();
        stat.execute("create table test(x int primary key, y int unique)");
        stat.execute("insert into test values(1, 1)");
        assertThrows(ErrorCode.DUPLICATE_KEY_1, stat).
                execute("merge into test values(2, 1)");
        stat.execute("merge into test values(1, 2)");
        conn.close();

    }

    private void testConcurrentMerge() throws Exception {
        deleteDb("mvccMultiThreaded");
        int len = 3;
        final Connection[] connList = new Connection[len];
        for (int i = 0; i < len; i++) {
            Connection conn = getConnection("mvccMultiThreaded;MVCC=TRUE;LOCK_TIMEOUT=500");
            connList[i] = conn;
        }
        Connection conn = connList[0];
        conn.createStatement().execute("create table test(id int primary key, name varchar)");
        Task[] tasks = new Task[len];
        final boolean[] stop = { false };
        for (int i = 0; i < len; i++) {
            final Connection c = connList[i];
            c.setAutoCommit(false);
            tasks[i] = new Task() {
                @Override
                public void call() throws Exception {
                    while (!stop) {
                        c.createStatement().execute("merge into test values(1, 'x')");
                        c.commit();
                        Thread.sleep(1);
                    }
                }
            };
            tasks[i].execute();
        }
        Thread.sleep(1000);
        stop[0] = true;
        for (int i = 0; i < len; i++) {
            tasks[i].get();
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
        final int count = 1000;
        Task[] tasks = new Task[len];

        final CountDownLatch latch = new CountDownLatch(len);

        for (int i = 0; i < len; i++) {
            final int x = i;
            tasks[i] = new Task() {
                @Override
                public void call() throws Exception {
                    for (int a = 0; a < count; a++) {
                        connList[x].createStatement().execute("update test set value=value+1");
                        latch.countDown();
                        latch.await();
                    }
                }
            };
            tasks[i].execute();
        }
        for (int i = 0; i < len; i++) {
            tasks[i].get();
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
