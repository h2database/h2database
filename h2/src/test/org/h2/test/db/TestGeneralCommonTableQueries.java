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

    private static final String SIMPLE_TWO_COMMON_QUERY = "with " +
            "t1(n) as (select 1 as first) " +
            ",t2(n) as (select 2 as first) " +
            "select * from t1 union all select * from t2";
    
    private static final String PARAMETERIZED_TWO_COMMON_QUERY = "with " +
            "t1(n) as (select 2 as first) " +
            ",t2(n) as (select 3 as first) " +
            "select * from t1 union all select * from t2 where n<>?";
    
    private static final String PARAMETERIZED_THREE_COMMON_QUERY = "with " +
            "t1(n) as (select 2 as first) " +
            ",t2(n) as (select 3 as first) " +
            ",t3(n) as (select 4 as first) " +
            "select * from t1 union all select * from t2 union all select * from t3 where n<>?";

    private static final String PARAMETERIZED_THREE_COMMON_QUERY_IMPLIED_COLUMN_NAMES = "with " +
            "t1 as (select 2 as first_col) " +
            ",t2 as (select first_col+1 from t1) " +
            ",t3 as (select 4 as first_col) " +
            "select * from t1 union all select * from t2 union all select * from t3 where first_col<>?";
	/**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
        System.out.println("Testing done");
    }

    @Override
    public void test() throws Exception {
        testSimple();
        testImpliedColumnNames();
    }

    private void testSimple() throws Exception {
        deleteDb("commonTableExpressionQueries");
        Connection conn = getConnection("commonTableExpressionQueries");
        Statement stat;
        PreparedStatement prep;
        ResultSet rs;

        stat = conn.createStatement();
        rs = stat.executeQuery(SIMPLE_TWO_COMMON_QUERY);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
        
        prep = conn.prepareStatement(SIMPLE_TWO_COMMON_QUERY);
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
        
        prep = conn.prepareStatement(PARAMETERIZED_TWO_COMMON_QUERY);
        prep.setInt(1, 0); // omit no lines since zero is not in list
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());
        
        prep = conn.prepareStatement(PARAMETERIZED_THREE_COMMON_QUERY);
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
      
        prep = conn.prepareStatement(PARAMETERIZED_THREE_COMMON_QUERY_IMPLIED_COLUMN_NAMES);
        prep.setInt(1, 4); // omit 4 line (last)
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("FIRST_COL"));
        assertFalse(rs.next());
        assertEquals("rsMeta0: columns=1",rs.getMetaData().toString());
        assertEquals("FIRST_COL",rs.getMetaData().getColumnLabel(1));
        
        conn.close();
        deleteDb("commonTableExpressionQueries");
    }
}
