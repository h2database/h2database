/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;
import org.h2.mvstore.db.MVTableEngine;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/**
 * Tests the MVStore in a database.
 */
public class TestMVTableEngine extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        testCase();
        testSimple();
    }


    private void testCase() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        ResultSet rs;

        Random random = new Random(1);
        int len = getSize(50, 500);
        for (int i = 0; i < len; i++) {
            stat.execute("drop table if exists test");
            stat.execute("create table test(x int)");
            if (random.nextBoolean()) {
                int count = random.nextBoolean() ? 1 : 1 + random.nextInt(len);
                if (count > 0) {
                    stat.execute("insert into test select null from system_range(1, " + count + ")");
                }
            }
            int maxExpected = -1;
            int minExpected = -1;
            if (random.nextInt(10) != 1) {
                minExpected = 1;
                maxExpected = 1 + random.nextInt(len);
                stat.execute("insert into test select x from system_range(1, " + maxExpected + ")");
            }
            String sql = "create index idx on test(x";
            if (random.nextBoolean()) {
                sql += " desc";
            }
            if (random.nextBoolean()) {
                if (random.nextBoolean()) {
                    sql += " nulls first";
                } else {
                    sql += " nulls last";
                }
            }
            sql += ")";
            stat.execute(sql);

            rs = stat.executeQuery("select min(x), max(x) from test");
            rs.next();
            int min = rs.getInt(1);
            if (rs.wasNull()) {
                min = -1;
            }
            int max = rs.getInt(2);
            if (rs.wasNull()) {
                max = -1;
            }
            assertEquals(minExpected, min);
            assertEquals(maxExpected, max);
        }
        conn.close();

    }

    private void testSimple() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn = getConnection("mvstore");
        Statement stat = conn.createStatement();
        // create table test(id int, name varchar) engine "org.h2.mvstore.db.MVStoreTableEngine"
        stat.execute("create table test(id int primary key, name varchar) engine \"" +
                MVTableEngine.class.getName() + "\"");
        stat.execute("insert into test values(1, 'Hello'), (2, 'World')");
        ResultSet rs = stat.executeQuery("select * from test");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        conn.close();

        conn = getConnection("mvstore");
        stat = conn.createStatement();
        rs = stat.executeQuery("select * from test order by id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("World", rs.getString(2));
        assertFalse(rs.next());

        stat.execute("delete from test where id = 2");
        rs = stat.executeQuery("select * from test order by id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());

        stat.execute("create index idx_name on test(name)");
        rs = stat.executeQuery("select * from test where name = 'Hello'");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());

        conn.close();
    }


}
