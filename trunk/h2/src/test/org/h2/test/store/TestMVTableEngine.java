/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
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
        // testCase();
        testSimple();
    }


    private void testCase() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();

        conn = getConnection("cases");
        stat = conn.createStatement();
        stat.execute("set max_operation_memory 1");
        stat.execute("create table test(id int)");
        stat.execute("insert into test values(1), (2)");
        stat.execute("create index idx on test(id)");
        conn.setAutoCommit(false);
        stat.execute("update test set id = id where id=2");
        stat.execute("update test set id = id");
        conn.rollback();

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
