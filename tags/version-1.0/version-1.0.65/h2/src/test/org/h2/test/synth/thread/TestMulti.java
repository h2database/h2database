/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.thread;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.h2.test.TestBase;

/**
 * Starts multiple threads and performs random operations on each thread.
 */
public class TestMulti extends TestBase {

    public volatile boolean stop;

    public void test() throws Exception {
        Class.forName("org.h2.Driver");
        deleteDb(baseDir, "openClose");

        // int len = getSize(5, 100);
        int len = 10;
        TestMultiThread[] threads = new TestMultiThread[len];
        for (int i = 0; i < len; i++) {
            threads[i] = new TestMultiNews(this);
        }
        threads[0].first();
        for (int i = 0; i < len; i++) {
            threads[i].start();
        }
        Thread.sleep(10000);
        this.stop = true;
        for (int i = 0; i < len; i++) {
            threads[i].join();
        }
        threads[0].finalTest();
    }

    public Connection getConnection() throws SQLException {
        final String url = "jdbc:h2:" + baseDir + "/openClose;LOCK_MODE=3;DB_CLOSE_DELAY=-1";
        Connection conn = DriverManager.getConnection(url, "sa", "");
        return conn;
    }

}
