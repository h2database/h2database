/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.mvcc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

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
        if (!config.mvcc) {
            return;
        }
        if (config.mvcc) {
            // not supported at this time
            return;
        }
        deleteDb("mvccMultiThreaded");
        int len = 2;
        final Connection[] connList = new Connection[len];
        for (int i = 0; i < len; i++) {
            connList[i] = getConnection("mvccMultiThreaded;MULTI_THREADED=TRUE");
        }
        Connection conn = connList[0];
        conn.createStatement().execute("create table test(id int primary key, value int)");
        conn.createStatement().execute("insert into test values(0, 0)");
        final SQLException[] ex = new SQLException[1];
        final int count = 1000;
        Thread[] threads = new Thread[len];
        for (int i = 0; i < len; i++) {
            final int x = i;
            threads[i] = new Thread() {
                public void run() {
                    for (int a = 0; a < count; a++) {
                        try {
                            connList[x].createStatement().execute("update test set value=value+1");
                        } catch (SQLException e) {
                            ex[0] = e;
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
