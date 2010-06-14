/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.constant.SysProperties;
import org.h2.test.TestBase;
import org.h2.util.ScriptReader;

/**
 * Tests nested joins and right outer joins.
 */
public class TestNestedJoins extends TestBase {
    
    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        System.setProperty("h2.nestedJoins", "true");
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        if (!SysProperties.NESTED_JOINS) {
            return;
        }
        
        deleteDb("nestedJoins");
        
        Connection conn = getConnection("nestedJoins");
        Statement stat = conn.createStatement();
        ResultSet rs;
        String sql;
        
        // not yet supported:
        /*
        stat.execute("drop table a, b, c");
        stat.execute("create table a(x int)");
        stat.execute("create table b(x int)");
        stat.execute("create table c(x int, y int)");
        stat.execute("insert into a values(1), (2)");
        stat.execute("insert into b values(3)");
        stat.execute("insert into c values(1, 3)");
        stat.execute("insert into c values(4, 5)");
        rs = stat.executeQuery("explain select * from a left outer join (b left outer join c on b.x = c.y) on a.x = c.x");
        assertTrue(rs.next());
        sql = cleanRemarks(conn, rs.getString(1));
        assertEquals("", sql);

        rs = stat.executeQuery("select * from a left outer join (b left outer join c on b.x = c.y) on a.x = c.x");
        // expected result: 1   3       1       3;  2       null    null    null
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("3", rs.getString(2));
        assertEquals("1", rs.getString(3));
        assertEquals("3", rs.getString(4));
        assertTrue(rs.next());
        assertEquals("2", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertEquals(null, rs.getString(4));
        assertFalse(rs.next());
        */

        stat.execute("create table a(x int primary key)");
        stat.execute("insert into a values(0), (1)");
        stat.execute("create table b(x int primary key)");
        stat.execute("insert into b values(0)");
        stat.execute("create table c(x int primary key)");

        rs = stat.executeQuery("select a.*, b.*, c.* from a left outer join (b inner join c on b.x = c.x) on a.x = b.x");
        // expected result: 0, null, null; 1, null, null
        assertTrue(rs.next());
        assertEquals("0", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertFalse(rs.next());
        
        rs = stat.executeQuery("select * from a left outer join b on a.x = b.x inner join c on b.x = c.x");
        // expected result: -
        assertFalse(rs.next());

        rs = stat.executeQuery("select * from a left outer join b on a.x = b.x left outer join c on b.x = c.x");
        // expected result: 0   0       null; 1       null    null
        assertTrue(rs.next());
        assertEquals("0", rs.getString(1));
        assertEquals("0", rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertFalse(rs.next());
        
        rs = stat.executeQuery("select * from a left outer join (b inner join c on b.x = c.x) on a.x = b.x");
        // expected result: 0   null    null; 1       null    null
        assertTrue(rs.next());
        assertEquals("0", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertFalse(rs.next());
        
        rs = stat.executeQuery("explain select * from a left outer join (b inner join c on c.x = 1) on a.x = b.x");
        assertTrue(rs.next());
        sql = cleanRemarks(conn, rs.getString(1));
        assertEquals("SELECT A.X, B.X, C.X FROM PUBLIC.A LEFT OUTER JOIN (PUBLIC.B INNER JOIN PUBLIC.C ON (C.X = 1) AND (A.X = B.X)) ON A.X = B.X", sql);

        stat.execute("create table test(id int primary key)");
        stat.execute("insert into test values(0), (1), (2)");
        rs = stat.executeQuery("select * from test a left outer join (test b inner join test c on b.id = c.id - 2) on a.id = b.id + 1");
        // drop table test;
        // create table test(id int primary key);
        // insert into test values(0), (1), (2);
        // select * from test a left outer join (test b inner join test c on b.id = c.id - 2) on a.id = b.id + 1;
        // expected result: 0   null    null; 1       0       2; 2       null    null
        assertTrue(rs.next());
        assertEquals("0", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));
        assertEquals("0", rs.getString(2));
        assertEquals("2", rs.getString(3));
        assertTrue(rs.next());
        assertEquals("2", rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertEquals(null, rs.getString(3));
        assertFalse(rs.next());
        
        // while (rs.next()) {
        //     for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
        //         System.out.print(rs.getString(i + 1) + " ");
        //     }
        //     System.out.println();
        // }
        
        conn.close();
        deleteDb("nestedJoins");
    }

    private String cleanRemarks(Connection conn, String sql) throws SQLException {
        ScriptReader r = new ScriptReader(new StringReader(sql));
        r.setSkipRemarks(true);
        sql = r.readStatement();
        sql = sql.replaceAll("\\n", " ");
        // sql = sql.replaceAll("\\/\\*.*\\*\\/", "");
        while (sql.indexOf("  ") >= 0) {
            sql = sql.replaceAll("  ", " ");
        }
        return sql;
    }
}
