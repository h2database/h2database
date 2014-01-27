/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Random;

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

        ;
        // TODO this test is currently disabled

        test(true);
        test(false);
        test(true);
        test(false);
        test(true);
        test(false);
    }

    private void test(boolean mvStore) throws Exception {
        testCreateIndex(mvStore);
    }

    private void testCreateIndex(boolean mvStore) throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;
        String url = "mvstore";
        if (mvStore) {
            url += ";MV_STORE=TRUE;MV_STORE=TRUE";
        }

        url = getURL(url, true);
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id bigint primary key, data bigint)");
        conn.setAutoCommit(false);
        PreparedStatement prep = conn
                .prepareStatement("insert into test values(?, ?)");

//        int rowCount = 10000000;
        int rowCount = 1000000;

        Random r = new Random(1);

        for (int i = 0; i < rowCount; i++) {
            prep.setInt(1, i);
            prep.setInt(2, i);
            // prep.setInt(2, r.nextInt());
            prep.execute();
            if (i % 10000 == 0) {
                conn.commit();
            }
        }

        long start = System.currentTimeMillis();
        // Profiler prof = new Profiler().startCollecting();
        stat.execute("create index on test(data)");
        // System.out.println(prof.getTop(5));

        System.out.println((System.currentTimeMillis() - start) + " "
                + (mvStore ? "mvstore" : "default"));
        conn.close();
    }

    private void testBinary(boolean mvStore) throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;
        String url = "mvstore";
        if (mvStore) {
            url += ";MV_STORE=TRUE;MV_STORE=TRUE";
        }

        url = getURL(url, true);
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id bigint primary key, data blob)");
        conn.setAutoCommit(false);
        PreparedStatement prep = conn
                .prepareStatement("insert into test values(?, ?)");
        byte[] data = new byte[1024 * 1024];

        int rowCount = 100;
        int readCount = 20 * rowCount;

        long start = System.currentTimeMillis();

        for (int i = 0; i < rowCount; i++) {
            prep.setInt(1, i);
            randomize(data, i);
            prep.setBytes(2, data);
            prep.execute();
            if (i % 100 == 0) {
                conn.commit();
            }
        }

        prep = conn.prepareStatement("select * from test where id = ?");
        for (int i = 0; i < readCount; i++) {
            prep.setInt(1, i % rowCount);
            prep.executeQuery();
        }

        System.out.println((System.currentTimeMillis() - start) + " "
                + (mvStore ? "mvstore" : "default"));
        conn.close();
    }

    private void randomize(byte[] data, int i) {
        Random r = new Random(i);
        r.nextBytes(data);
    }

    private void testInsertSelect(boolean mvStore) throws Exception {

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
