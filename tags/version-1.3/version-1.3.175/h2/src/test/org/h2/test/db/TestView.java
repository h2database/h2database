/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
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
import org.h2.constant.ErrorCode;
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

    @Override
    public void test() throws SQLException {
        testEmptyColumn();
        testChangeSchemaSearchPath();
        testParameterizedView();
        testCache();
        testCacheFunction(true);
        testCacheFunction(false);
        testInSelect();
        testUnionReconnect();
        testManyViews();
        testReferenceView();
        testViewAlterAndCommandCache();
        deleteDb("view");
    }

    private void testEmptyColumn() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("create table test(a int, b int)");
        stat.execute("create view test_view as select a, b from test");
        stat.execute("select * from test_view where a between 1 and 2 and b = 2");
        conn.close();
    }

    private void testChangeSchemaSearchPath() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view;FUNCTIONS_IN_SCHEMA=TRUE");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS X AS $$ int x() { return 1; } $$;");
        stat.execute("CREATE SCHEMA S");
        stat.execute("CREATE VIEW S.TEST AS SELECT X() FROM DUAL");
        stat.execute("SET SCHEMA=S");
        stat.execute("SET SCHEMA_SEARCH_PATH=S");
        stat.execute("SELECT * FROM TEST");
        conn.close();
    }

    private void testParameterizedView() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE Test(id INT AUTO_INCREMENT NOT NULL, f1 VARCHAR NOT NULL, f2 VARCHAR NOT NULL)");
        stat.execute("INSERT INTO Test(f1, f2) VALUES ('value1','value2')");
        stat.execute("INSERT INTO Test(f1, f2) VALUES ('value1','value3')");
        PreparedStatement ps = conn.prepareStatement("CREATE VIEW Test_View AS SELECT f2 FROM Test WHERE f1=?");
        ps.setString(1, "value1");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, ps).
                executeUpdate();
        // ResultSet rs;
        // rs = stat.executeQuery("SELECT * FROM Test_View");
        // assertTrue(rs.next());
        // assertFalse(rs.next());
        // rs = stat.executeQuery("select VIEW_DEFINITION " +
        // "from information_schema.views " +
        // "where TABLE_NAME='TEST_VIEW'");
        // rs.next();
        // assertEquals("...", rs.getString(1));
        conn.close();
    }

    private void testCacheFunction(boolean deterministic) throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        x = 8;
        stat.execute("CREATE ALIAS GET_X " +
                (deterministic ? "DETERMINISTIC" : "") +
                " FOR \"" + getClass().getName() + ".getX\"");
        stat.execute("CREATE VIEW V AS SELECT * FROM (SELECT GET_X())");
        ResultSet rs;
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

    private void testReferenceView() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement s = conn.createStatement();
        s.execute("create table t0(id int primary key)");
        s.execute("create view t1 as select * from t0");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, s).execute(
                "create table t2(id int primary key, col1 int not null, foreign key (col1) references t1(id))");
        conn.close();
        deleteDb("view");
    }

    /**
     * Make sure that when we change a view, that change in reflected in other
     * sessions command cache.
     */
    private void testViewAlterAndCommandCache() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("create table t0(id int primary key)");
        stat.execute("create table t1(id int primary key)");
        stat.execute("insert into t0 values(0)");
        stat.execute("insert into t1 values(1)");
        stat.execute("create view v1 as select * from t0");
        ResultSet rs = stat.executeQuery("select * from v1");
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        stat.execute("create or replace view v1 as select * from t1");
        rs = stat.executeQuery("select * from v1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        conn.close();
        deleteDb("view");
    }

}
