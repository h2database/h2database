/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbcx;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.test.TestBase;

/**
 * This class tests the JdbcConnectionPool.
 */
public class TestConnectionPool extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        deleteDb("connectionPool");
        testUncommittedTransaction();
        testPerformance();
        testKeepOpen();
        testConnect();
        testThreads();
        deleteDb("connectionPool");
    }

    private void testUncommittedTransaction() throws SQLException {
        String url = getURL("connectionPool", true), user = getUser(), password = getPassword();
        JdbcConnectionPool man = JdbcConnectionPool.create(url, user, password);

        assertEquals(5 * 60, man.getLoginTimeout());
        man.setLoginTimeout(1);
        assertEquals(1, man.getLoginTimeout());
        man.setLoginTimeout(0);
        assertEquals(5 * 60, man.getLoginTimeout());
        assertEquals(10, man.getMaxConnections());

        PrintWriter old = man.getLogWriter();
        PrintWriter pw = new PrintWriter(new StringWriter());
        man.setLogWriter(pw);
        assertTrue(pw == man.getLogWriter());
        man.setLogWriter(old);

        Connection conn1 = man.getConnection();
        assertTrue(conn1.getAutoCommit());
        conn1.setAutoCommit(false);
        conn1.close();
        assertTrue(conn1.isClosed());

        Connection conn2 = man.getConnection();
        assertTrue(conn2.getAutoCommit());
        conn2.close();

        man.dispose();
    }

    private void testPerformance() throws SQLException {
        String url = getURL("connectionPool", true), user = getUser(), password = getPassword();
        JdbcConnectionPool man = JdbcConnectionPool.create(url, user, password);
        Connection conn = man.getConnection();
        int len = 1000;
        long time = System.currentTimeMillis();
        for (int i = 0; i < len; i++) {
            man.getConnection().close();
        }
        trace((int) (System.currentTimeMillis() - time));
        man.dispose();
        time = System.currentTimeMillis();
        for (int i = 0; i < len; i++) {
            DriverManager.getConnection(url, user, password).close();
        }
        trace((int) (System.currentTimeMillis() - time));
        conn.close();
    }

    private void testKeepOpen() throws Exception {
        JdbcConnectionPool man = getConnectionPool(1);
        Connection conn = man.getConnection();
        Statement stat = conn.createStatement();
        stat.execute("create local temporary table test(id int)");
        conn.close();
        conn = man.getConnection();
        stat = conn.createStatement();
        stat.execute("select * from test");
        conn.close();
        man.dispose();
    }

    private void testThreads() throws Exception {
        final int len = getSize(4, 20);
        final JdbcConnectionPool man = getConnectionPool(len - 2);
        final boolean[] stop = { false };

        /**
         * This class gets and returns connections from the pool.
         */
        class TestRunner implements Runnable {
            public void run() {
                try {
                    while (!stop[0]) {
                        Connection conn = man.getConnection();
                        if (man.getActiveConnections() >= len + 1) {
                            throw new Exception("a: " + man.getActiveConnections()  + " is not smaller than b: " + len + 1);
                        }
                        Statement stat = conn.createStatement();
                        stat.execute("SELECT 1 FROM DUAL");
                        conn.close();
                        Thread.sleep(100);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        Thread[] threads = new Thread[len];
        for (int i = 0; i < len; i++) {
            threads[i] = new Thread(new TestRunner());
            threads[i].start();
        }
        Thread.sleep(1000);
        stop[0] = true;
        for (int i = 0; i < len; i++) {
            threads[i].join();
        }
        assertEquals(0, man.getActiveConnections());
        man.dispose();
    }

    private JdbcConnectionPool getConnectionPool(int poolSize) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(getURL("connectionPool", true));
        ds.setUser(getUser());
        ds.setPassword(getPassword());
        JdbcConnectionPool pool = JdbcConnectionPool.create(ds);
        pool.setMaxConnections(poolSize);
        return pool;
    }

    private void testConnect() throws SQLException {
        JdbcConnectionPool man = getConnectionPool(3);
        for (int i = 0; i < 100; i++) {
            Connection conn = man.getConnection();
            conn.close();
        }
        man.dispose();
    }

}
