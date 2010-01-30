/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.test.TestBase;

/**
 * Test for views.
 */
public class TestView extends TestBase {

    private static int x;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        testCache();
        testCacheFunction(true);
        testCacheFunction(false);
        testInSelect();
        testUnionReconnect();
        testManyViews();
        deleteDb("view");
    }

    private void testCacheFunction(boolean deterministic) throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS GET_X " +
                (deterministic ? "DETERMINISTIC" : "") +
                " FOR \"" + getClass().getName() + ".getX\"");
        stat.execute("CREATE VIEW V AS SELECT * FROM (SELECT GET_X())");
        ResultSet rs;
        x = 8;
        rs = stat.executeQuery("SELECT * FROM V");
        rs.next();
        assertEquals(8, rs.getInt(1));
        x = 5;
        rs = stat.executeQuery("SELECT * FROM V");
        rs.next();
        assertEquals(deterministic ? 8 : 5, rs.getInt(1));
        conn.close();
    }

    /**
     * This method is called via reflection from the database.
     *
     * @return the static value x
     */
    public static int getX() {
        return x;
    }

    private void testCache() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("SET @X 8");
        stat.execute("CREATE VIEW V AS SELECT * FROM (SELECT @X)");
        ResultSet rs;
        rs = stat.executeQuery("SELECT * FROM V");
        rs.next();
        assertEquals(8, rs.getInt(1));
        stat.execute("SET @X 5");
        rs = stat.executeQuery("SELECT * FROM V");
        rs.next();
        assertEquals(5, rs.getInt(1));
        conn.close();
    }

    private void testInSelect() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key) as select 1");
        PreparedStatement prep = conn.prepareStatement(
                "select * from test t where t.id in (select t2.id from test t2 where t2.id in (?, ?))");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        prep.execute();
        conn.close();
    }

    private void testUnionReconnect() throws SQLException {
        if (config.memory) {
            return;
        }
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("create table t1(k smallint, ts timestamp(6))");
        stat.execute("create table t2(k smallint, ts timestamp(6))");
        stat.execute("create table t3(k smallint, ts timestamp(6))");
        stat.execute("create view v_max_ts as select " +
                "max(ts) from (select max(ts) as ts from t1 " +
                "union select max(ts) as ts from t2 " +
                "union select max(ts) as ts from t3)");
        stat.execute("create view v_test as select max(ts) as ts from t1 " +
                "union select max(ts) as ts from t2 " +
                "union select max(ts) as ts from t3");
        conn.close();
        conn = getConnection("view");
        stat = conn.createStatement();
        stat.execute("select * from v_max_ts");
        conn.close();
        deleteDb("view");
    }

    private void testManyViews() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement s = conn.createStatement();
        s.execute("create table t0(id int primary key)");
        s.execute("insert into t0 values(1), (2), (3)");
        for (int i = 0; i < 30; i++) {
            s.execute("create view t" + (i + 1) + " as select * from t" + i);
            s.execute("select * from t" + (i + 1));
            ResultSet rs = s.executeQuery("select count(*) from t" + (i + 1) + " where id=2");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
        conn.close();
        conn = getConnection("view");
        conn.close();
        deleteDb("view");
    }
}
