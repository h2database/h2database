/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Test for GitHub Issue #701: An available index is never used for ordering
 * when the requested order is DESC.
 *
 * Issue: CREATE TABLE my_table(K1 INT);
 *        CREATE INDEX my_index ON my_table(K1);
 *        EXPLAIN PLAN FOR SELECT * FROM my_table ORDER BY K1 DESC;
 *
 * Expected: The ASC index should be used in reverse for DESC ordering
 * Before fix: Table scan (index not used)
 * After fix: Index used with "/* index sorted
 */
public class TestIssue701 extends TestDb {

    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        testDescOrderingWithAscIndex();
        testMultiColumnDescOrdering();
        testMixedAscDescIndexExists();
    }

    /**
     * Test the exact scenario from Issue #701
     */
    private void testDescOrderingWithAscIndex() throws SQLException {
        deleteDb("issue701");
        Connection conn = getConnection("issue701");
        Statement stat = conn.createStatement();

        // Exact setup from issue #701
        stat.execute("CREATE TABLE my_table(K1 INT)");
        stat.execute("CREATE INDEX my_index ON my_table(K1)");
        stat.execute("INSERT INTO my_table VALUES (3), (1), (2)");

        // Test EXPLAIN shows index is used
        ResultSet rs = stat.executeQuery("EXPLAIN PLAN FOR SELECT * FROM my_table ORDER BY K1 DESC");
        rs.next();
        String plan = rs.getString(1);

        System.out.println("Issue #701 Test:");
        System.out.println("Plan: " + plan);

        // Verify index is used (should contain "index sorted")
        assertTrue("Index should be used for DESC ordering", plan.contains("/* index sorted */"));

        // Verify correct results
        rs = stat.executeQuery("SELECT * FROM my_table ORDER BY K1 DESC");
        rs.next();
        assertEquals(3, rs.getInt(1));
        rs.next();
        assertEquals(2, rs.getInt(1));
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        stat.execute("DROP TABLE my_table");
        conn.close();
        deleteDb("issue701");

        System.out.println("✅ Issue #701 test PASSED - ASC index used for DESC ordering\n");
    }

    /**
     * Test multi-column DESC ordering with ASC indexes
     */
    private void testMultiColumnDescOrdering() throws SQLException {
        deleteDb("issue701");
        Connection conn = getConnection("issue701");
        Statement stat = conn.createStatement();

        stat.execute("CREATE TABLE test(a INT, b INT)");
        stat.execute("CREATE INDEX idx_ab ON test(a, b)");
        stat.execute("INSERT INTO test VALUES (1, 1), (1, 2), (2, 1), (2, 2)");

        // Test DESC on both columns
        ResultSet rs = stat.executeQuery("EXPLAIN SELECT * FROM test ORDER BY a DESC, b DESC");
        rs.next();
        String plan = rs.getString(1);

        System.out.println("Multi-column DESC test:");
        System.out.println("Plan: " + plan);

        assertTrue("Index should be used for multi-column DESC", plan.contains("/* index sorted */"));

        // Verify correct results
        rs = stat.executeQuery("SELECT * FROM test ORDER BY a DESC, b DESC");
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt(2));

        stat.execute("DROP TABLE test");
        conn.close();
        deleteDb("issue701");

        System.out.println("✅ Multi-column DESC test PASSED\n");
    }

    /**
     * Test that when both ASC and DESC indexes exist, the appropriate one is chosen
     */
    private void testMixedAscDescIndexExists() throws SQLException {
        deleteDb("issue701");
        Connection conn = getConnection("issue701");
        Statement stat = conn.createStatement();

        stat.execute("CREATE TABLE test(id INT)");
        stat.execute("CREATE INDEX idx_asc ON test(id ASC)");
        stat.execute("CREATE INDEX idx_desc ON test(id DESC)");
        stat.execute("INSERT INTO test VALUES (3), (1), (2)");

        // Test ASC ordering - should use ASC index
        ResultSet rs = stat.executeQuery("EXPLAIN SELECT * FROM test ORDER BY id ASC");
        rs.next();
        String ascPlan = rs.getString(1);
        assertTrue("Should use index for ASC", ascPlan.contains("/* index sorted */"));

        // Test DESC ordering - can use either index (ASC in reverse or DESC forward)
        rs = stat.executeQuery("EXPLAIN SELECT * FROM test ORDER BY id DESC");
        rs.next();
        String descPlan = rs.getString(1);
        assertTrue("Should use index for DESC", descPlan.contains("/* index sorted */"));

        System.out.println("Mixed ASC/DESC indexes test:");
        System.out.println("ASC plan: " + ascPlan);
        System.out.println("DESC plan: " + descPlan);

        // Verify both produce correct results
        rs = stat.executeQuery("SELECT * FROM test ORDER BY id ASC");
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.next();
        assertEquals(2, rs.getInt(1));
        rs.next();
        assertEquals(3, rs.getInt(1));

        rs = stat.executeQuery("SELECT * FROM test ORDER BY id DESC");
        rs.next();
        assertEquals(3, rs.getInt(1));
        rs.next();
        assertEquals(2, rs.getInt(1));
        rs.next();
        assertEquals(1, rs.getInt(1));

        stat.execute("DROP TABLE test");
        conn.close();
        deleteDb("issue701");

        System.out.println("✅ Mixed indexes test PASSED\n");
    }
}

