/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.mvcc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

import org.h2.test.TestAll;
import org.h2.test.TestBase;
import org.h2.util.Task;

/**
 * Multi-threaded MVCC (multi version concurrency) test cases.
 */
public class CopyOfTestMvccMultiThreaded extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.big = true;
        test.test();
    }

    public void test() throws Exception {
        TestAll.printSystemInfo();
        int test;
        if (config.big && !config.networked)
        for(int i=0; ; i++) {
            System.out.println("--------------- i: " + i);
            testConcurrentMerge();
        }
//        testConcurrentUpdate("");
        // not supported currently
        // testConcurrentUpdate(";MULTI_THREADED=TRUE");
    }

    private void testConcurrentMerge() throws Exception {
        deleteDb("mvccMultiThreadedMerge");
        int len = 3;
        final Connection[] connList = new Connection[len];
        for (int i = 0; i < len; i++) {
            Connection conn = getConnection("mvccMultiThreadedMerge;MVCC=TRUE;LOCK_TIMEOUT=1000;MAX_OPERATION_MEMORY=1000");
            connList[i] = conn;
        }
        Connection conn = connList[0];
        conn.createStatement().execute("create table test(id int primary key, name varchar)");
        Task[] tasks = new Task[len];
final StringBuffer buff = new StringBuffer();
final long startThread = System.currentTimeMillis();
        for (int i = 0; i < len; i++) {
final int x = i;
            final Connection c = connList[i];
            c.setAutoCommit(false);
            tasks[i] = new Task() {
                public void call() throws SQLException {
                    while (!stop) {
                        c.createStatement().execute("merge into test values(1, 'x')");
                        c.commit();
                        buff.append((System.currentTimeMillis() - startThread) + ": " + x + "\n");
//if (System.currentTimeMillis() - startThread > 3000) {
//    System.out.println(x);
//}

                    }
                }
            };
            tasks[i].execute();
        }
        Thread.sleep(2000);
try{
        for (int i = 0; i < len; i++) {
            tasks[i].get();
        }
}catch(Exception e) {
    System.out.println(buff);
    System.out.println(e.toString());
 //   e.printStackTrace(System.out);
}
long time = System.currentTimeMillis() - startThread;
//if (time > 10000) {
//    System.out.println(buff);
//}
        for (int i = 0; i < len; i++) {
            connList[i].close();
        }
        deleteDb("mvccMultiThreadedMerge");
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
