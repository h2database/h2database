/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Test recursive queries using WITH.
 */
public class TestRecursiveQueries extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        testWrongLinkLargeResult();
        testSimpleUnionAll();
        testSimpleUnion();
        testParameters();
    }

    private void testWrongLinkLargeResult() throws Exception {
        deleteDb("recursiveQueries");
        Connection conn = getConnection("recursiveQueries");
        Statement stat;
        stat = conn.createStatement();
        stat.execute("create table test(parent varchar(255), child varchar(255))");
        stat.execute("insert into test values('/', 'a'), ('a', 'b1'), " +
                "('a', 'b2'), ('a', 'c'), ('c', 'd1'), ('c', 'd2')");

        ResultSet rs = stat.executeQuery(
                "with recursive rec_test(depth, parent, child) as (" +
                "select 0, parent, child from test where parent = '/' " +
                "union all " +
                "select depth+1, r.parent, r.child from test i join rec_test r " +
                "on (i.parent = r.child) where depth<9 " +
                ") select count(*) from rec_test");
        rs.next();
        assertEquals(29524, rs.getInt(1));
        stat.execute("with recursive rec_test(depth, parent, child) as ( "+
                "select 0, parent, child from test where parent = '/' "+
                "union all "+
                "select depth+1, i.parent, i.child from test i join rec_test r "+
                "on (r.child = i.parent) where depth<10 "+
                ") select * from rec_test");
        conn.close();
        deleteDb("recursiveQueries");
    }

    private void testSimpleUnionAll() throws Exception {
        deleteDb("recursiveQueries");
        Connection conn = getConnection("recursiveQueries");
        Statement stat;
        PreparedStatement prep, prep2;
        ResultSet rs;

        stat = conn.createStatement();
        rs = stat.executeQuery("with recursive t(n) as " +
                "(select 1 union all select n+1 from t where n<3) " +
                "select * from t");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());

        prep = conn.prepareStatement("with recursive t(n) as " +
                "(select 1 union all select n+1 from t where n<3) " +
                "select * from t where n>?");
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());

        prep.setInt(1, 1);
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());

        prep = conn.prepareStatement("with recursive t(n) as " +
                "(select @start union all select n+@inc from t where n<@end_index) " +
                "select * from t");
        prep2 = conn.prepareStatement("select @start:=?, @inc:=?, @end_index:=?");
        prep2.setInt(1, 10);
        prep2.setInt(2, 2);
        prep2.setInt(3, 14);
        assertTrue(prep2.executeQuery().next());
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(12, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(14, rs.getInt(1));
        assertFalse(rs.next());

        prep2.setInt(1, 100);
        prep2.setInt(2, 3);
        prep2.setInt(3, 103);
        assertTrue(prep2.executeQuery().next());
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(100, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(103, rs.getInt(1));
        assertFalse(rs.next());

        prep = conn.prepareStatement("with recursive t(n) as " +
                "(select ? union all select n+? from t where n<?) " +
                "select * from t");
        prep.setInt(1, 10);
        prep.setInt(2, 2);
        prep.setInt(3, 14);
        rs = prep.executeQuery();
        assertResultSetOrdered(rs, new String[][]{{"10"}, {"12"}, {"14"}});

        prep.setInt(1, 100);
        prep.setInt(2, 3);
        prep.setInt(3, 103);
        rs = prep.executeQuery();
        assertResultSetOrdered(rs, new String[][]{{"100"}, {"103"}});

        rs = stat.executeQuery("with recursive t(i, s, d) as "
                + "(select 1, '.', localtimestamp union all"
                + " select i+1, s||'.', d from t where i<3)"
                + " select * from t");
        assertResultSetMeta(rs, 3, new String[]{ "I", "S", "D" },
                new int[]{ Types.INTEGER, Types.VARCHAR, Types.TIMESTAMP },
                null, null);

        rs = stat.executeQuery("select x from system_range(1,5) "
                + "where x not in (with recursive w(x) as (select 1 union all select x+1 from w where x<3) "
                + "select x from w)");
        assertResultSetOrdered(rs, new String[][]{{"4"}, {"5"}});

        conn.close();
        deleteDb("recursiveQueries");
    }

    private void testSimpleUnion() throws Exception {
        deleteDb("recursiveQueries");
        Connection conn = getConnection("recursiveQueries");
        Statement stat;
        ResultSet rs;

        stat = conn.createStatement();
        rs = stat.executeQuery("with recursive t(n) as " +
                "(select 1 union select n+1 from t where n<3) " +
                "select * from t");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());

        conn.close();
        deleteDb("recursiveQueries");
    }

    private void testParameters() throws Exception {
        deleteDb("recursiveQueries");
        Connection conn = getConnection("recursiveQueries");
        PreparedStatement prep = conn.prepareStatement("WITH RECURSIVE T1(F1, F2) AS (\n" //
                + "    SELECT CAST(? AS INT), CAST(? AS VARCHAR(15))\n" //
                + "    UNION ALL\n" //
                + "    SELECT (T1.F1 + CAST(? AS INT)), CAST((CAST(? AS VARCHAR) || T1.F2) AS VARCHAR(15))\n" //
                + "    FROM T1 WHERE T1.F1 < 10\n" //
                + "  ),\n" //
                + "T2(G1, G2) AS (\n" //
                + "    SELECT CAST(? AS INT), CAST(? AS VARCHAR(15))\n" //
                + "    UNION ALL\n" //
                + "    SELECT (T2.G1 + 1), CAST(('b' || T2.G2) AS VARCHAR(15))\n" //
                + "    FROM T2 WHERE T2.G1 < 10\n" //
                + "  )\n" //
                + "SELECT T1.F1, T1.F2, T2.G1, T2.G2 FROM T1 JOIN T2 ON T1.F1 = T2.G1");
        prep.setInt(1, 1);
        prep.setString(2, "a");
        prep.setInt(3, 1);
        prep.setString(4, "a");
        prep.setInt(5, 1);
        prep.setString(6, "b");
        ResultSet rs = prep.executeQuery();
        StringBuilder a = new StringBuilder(10), b = new StringBuilder(10);
        for (int i = 1; i <= 10; i++) {
            a.append('a');
            b.append('b');
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
            assertEquals(a.toString(), rs.getString(2));
            assertEquals(i, rs.getInt(3));
            assertEquals(b.toString(), rs.getString(4));
        }
        assertFalse(rs.next());
        conn.close();
        deleteDb("recursiveQueries");
    }

}
