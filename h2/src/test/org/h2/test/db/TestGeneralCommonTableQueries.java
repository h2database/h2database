/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.h2.test.TestBase;

/**
 * Test non-recursive queries using WITH, but more than one common table defined.
 */
public class TestGeneralCommonTableQueries extends TestBase {

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
        testSimple();
        testImpliedColumnNames();
        testChainedQuery();
        testParameterizedQuery();
        testNumberedParameterizedQuery();
    }

    private void testSimple() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        Statement stat;
        PreparedStatement prep;
        ResultSet rs;

        stat = conn.createStatement();
        final String simple_two_column_query = "with " +
            "t1(n) as (select 1 as first) " +
            ",t2(n) as (select 2 as first) " +
            "select * from t1 union all select * from t2";
        rs = stat.executeQuery(simple_two_column_query);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        prep = conn.prepareStatement(simple_two_column_query);
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        prep = conn.prepareStatement("with " +
            "t1(n) as (select 2 as first) " +
            ",t2(n) as (select 3 as first) " +
            "select * from t1 union all select * from t2 where n<>?");
        prep.setInt(1, 0); // omit no lines since zero is not in list
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());

        prep = conn.prepareStatement("with " +
            "t1(n) as (select 2 as first) " +
            ",t2(n) as (select 3 as first) " +
            ",t3(n) as (select 4 as first) " +
            "select * from t1 union all select * from t2 union all select * from t3 where n<>?");
        prep.setInt(1, 4); // omit 4 line (last)
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());

        conn.close();
        deleteDb("commonTableExpressionQueries");
    }

    private void testImpliedColumnNames() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        PreparedStatement prep;
        ResultSet rs;

        prep = conn.prepareStatement("with " +
            "t1 as (select 2 as first_col) " +
            ",t2 as (select first_col+1 from t1) " +
            ",t3 as (select 4 as first_col) " +
            "select * from t1 union all select * from t2 union all select * from t3 where first_col<>?");
        prep.setInt(1, 4); // omit 4 line (last)
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("FIRST_COL"));
        assertFalse(rs.next());
        assertEquals(rs.getMetaData().getColumnCount(),1);
        assertEquals("FIRST_COL",rs.getMetaData().getColumnLabel(1));

        conn.close();
        deleteDb("commonTableExpressionQueries");
    }

    private void testChainedQuery() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        PreparedStatement prep;
        ResultSet rs;

        prep = conn.prepareStatement(
                "    WITH t1 AS (" +
                "        SELECT 1 AS FIRST_COLUMN" +
                ")," +
                "     t2 AS (" +
                "        SELECT FIRST_COLUMN+1 AS FIRST_COLUMN FROM t1 " +
                ") " +
                "SELECT sum(FIRST_COLUMN) FROM t2");

        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        conn.close();
        deleteDb("commonTableExpressionQueries");
    }

    private void testParameterizedQuery() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        PreparedStatement prep;
        ResultSet rs;

        prep = conn.prepareStatement("WITH t1 AS (" +
                "     SELECT X, 'T1' FROM SYSTEM_RANGE(?,?)" +
                ")," +
                "t2 AS (" +
                "     SELECT X, 'T2' FROM SYSTEM_RANGE(?,?)" +
                ") " +
                "SELECT * FROM t1 UNION ALL SELECT * FROM t2 " +
                "UNION ALL SELECT X, 'Q' FROM SYSTEM_RANGE(?,?)");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        prep.setInt(3, 3);
        prep.setInt(4, 4);
        prep.setInt(5, 5);
        prep.setInt(6, 6);
        rs = prep.executeQuery();

        for(int n: new int[]{1,2,3,4,5,6} ){
            assertTrue(rs.next());
            assertEquals(n, rs.getInt(1));
        }
        assertFalse(rs.next());

        // call it twice
        rs = prep.executeQuery();

        for(int n: new int[]{1,2,3,4,5,6} ){
            assertTrue(rs.next());
            assertEquals(n, rs.getInt(1));
        }
        assertFalse(rs.next());

        conn.close();
        deleteDb("commonTableExpressionQueries");
    }

    private void testNumberedParameterizedQuery() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        PreparedStatement prep;
        ResultSet rs;

        prep = conn.prepareStatement("WITH t1 AS ("
            +"     SELECT R.X, 'T1' FROM SYSTEM_RANGE(?1,?2) R"
            +"),"
            +"t2 AS ("
            +"     SELECT R.X, 'T2' FROM SYSTEM_RANGE(?3,?4) R"
            +") "
            +"SELECT * FROM t1 UNION ALL SELECT * FROM t2 UNION ALL SELECT X, 'Q' FROM SYSTEM_RANGE(?5,?6)");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        prep.setInt(3, 3);
        prep.setInt(4, 4);
        prep.setInt(5, 5);
        prep.setInt(6, 6);
        rs = prep.executeQuery();

        for (int n : new int[] { 1, 2, 3, 4, 5, 6 }) {
            assertTrue(rs.next());
            assertEquals(n, rs.getInt(1));
        }
        assertEquals("X",rs.getMetaData().getColumnLabel(1));
        assertEquals("'T1'",rs.getMetaData().getColumnLabel(2));

        assertFalse(rs.next());

        conn.close();
        deleteDb("commonTableExpressionQueries");
    }
}
