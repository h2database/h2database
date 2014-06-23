/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/**
 * Tests performance and helps analyze bottlenecks.
 */
public class TestBenchmark extends TestBase {

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
        test(true);
        test(false);
        test(true);
        test(false);
        test(true);
        test(false);
    }

    private void test(boolean mvStore) throws Exception {

        ;
        // TODO this test is currently disabled

        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;
        String url = "mvstore";
        if (mvStore) {
            url += ";MV_STORE=TRUE;LOG=0";
        }

        url = getURL(url, true);
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id bigint primary key, name varchar)");
        conn.setAutoCommit(false);
        PreparedStatement prep = conn
                .prepareStatement("insert into test values(?, ?)");
        String data = "Hello World";

        int rowCount = 100000;
        int readCount = 20 * rowCount;

        for (int i = 0; i < rowCount; i++) {
            prep.setInt(1, i);
            prep.setString(2, data);
            prep.execute();
            if (i % 100 == 0) {
                conn.commit();
            }
        }
        long start = System.currentTimeMillis();

        prep = conn.prepareStatement("select * from test where id = ?");
        for (int i = 0; i < readCount; i++) {
            prep.setInt(1, i % rowCount);
            prep.executeQuery();
        }

        System.out.println((System.currentTimeMillis() - start) + " "
                + (mvStore ? "mvstore" : "default"));
        conn.close();

    }

}
