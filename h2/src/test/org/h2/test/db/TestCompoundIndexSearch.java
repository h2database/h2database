/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import org.h2.test.TestBase;
import org.h2.test.TestDb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Test various queries against compound indexes.
 */
public class TestCompoundIndexSearch extends TestDb {

    private static final String DB_NAME = "compoundIndexSearch";

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
        Connection conn = prepare();

        simpleInAgainstSimpleIndexCheck(conn);
        simpleInAgainstFirstCompoundIndex(conn);
        simpleInAgainstSecondCompoundIndex(conn);
        compoundInAgainstCompoundIndex(conn);
        compoundEqAgainstCompoundIndex(conn);
        multipleEqAgainstCompoundIndex(conn);

        conn.close();
        deleteDb(DB_NAME);
    }

    private Connection prepare() throws Exception {
        deleteDb(DB_NAME);
        Connection conn = getConnection(DB_NAME);
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE test (a INT, b INT, c INT, d INT);");
        stat.execute("CREATE INDEX idx_a ON test(a);");
        stat.execute("CREATE INDEX idx_b_c ON test(b, c);");
        stat.execute("INSERT INTO test (a, b, c, d) VALUES " +
                "(1, 1, 1, 1), " +
                "(1, 1, 2, 2), " +
                "(1, 3, 3, 3), " +
                "(2, 2, 1, 4), " +
                "(2, 3, 2, 1), " +
                "(2, 3, 3, 2), " +
                "(3, 2, 1, 3), " +
                "(3, 2, 2, 4), " +
                "(3, 3, 3, 1), " +
                "(4, 1, 1, 2);"
        );
        stat.close();
        return conn;
    }

    /**
     * Executes a query with a simple IN condition against an indexed column.
     */
    private void simpleInAgainstSimpleIndexCheck(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("EXPLAIN ANALYZE SELECT b, c FROM test WHERE (a) IN (1, 4)");
        rs.next();
        assertEquals(rs.getString(1).replaceAll("[\\r\\n\\s]+", " "),
                "SELECT \"B\", \"C\" FROM \"PUBLIC\".\"TEST\" /* PUBLIC.IDX_A: A IN(1, 4) */ " +
                        "/* scanCount: 5 */ WHERE \"A\" IN(1, 4)");
        stat.close();
    }

    /**
     * Executes a query with a simple IN condition against a compound index. The lookup column is the first component
     * of the index, so the lookup works as it was a simple index.
     */
    private void simpleInAgainstFirstCompoundIndex(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("EXPLAIN ANALYZE SELECT b, c FROM test WHERE b IN (1, 2)");
        rs.next();
        assertContains(rs.getString(1).replaceAll("[\\r\\n\\s]+", " "),
                "SELECT \"B\", \"C\" FROM \"PUBLIC\".\"TEST\" /* PUBLIC.IDX_B_C: B IN(1, 2) */ " +
                        "/* scanCount: 7 */ WHERE \"B\" IN(1, 2)");
        stat.close();
    }

    /**
     * Executes a query with a simple IN condition against a compound index. The lookup column is the second component
     * of the index, so a full table scan happens.
     */
    private void simpleInAgainstSecondCompoundIndex(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("EXPLAIN ANALYZE SELECT b, c FROM test WHERE c IN (1, 2)");
        rs.next();
        assertContains(rs.getString(1).replaceAll("[\\r\\n\\s]+", " "),
                "SELECT \"B\", \"C\" FROM \"PUBLIC\".\"TEST\" /* PUBLIC.IDX_B_C */ " +
                        "/* scanCount: 11 */ WHERE \"C\" IN(1, 2)");
        stat.close();
    }

    /**
     * Executes a query with a compound IN condition against a compound index.
     */
    private void compoundInAgainstCompoundIndex(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("EXPLAIN ANALYZE SELECT b, c FROM test WHERE (b, c) IN ((1, 1), (2, 2))");
        rs.next();
        assertContains(rs.getString(1).replaceAll("[\\r\\n\\s]+", " "),
                "SELECT \"B\", \"C\" FROM \"PUBLIC\".\"TEST\" " +
                        "/* PUBLIC.IDX_B_C: IN(ROW (1, 1), ROW (2, 2)) AND B IN(1, 2) */ " +
                        "/* scanCount: 4 */ WHERE ROW (\"B\", \"C\") IN(ROW (1, 1), ROW (2, 2))");
        stat.close();
    }

    /**
     * Executes a query with a compound EQ condition against a compound index.
     */
    private void compoundEqAgainstCompoundIndex(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("EXPLAIN ANALYZE SELECT b, c FROM test WHERE (b, c) = (1, 1)");
        rs.next();
        assertContains(rs.getString(1).replaceAll("[\\r\\n\\s]+", " "),
                "SELECT \"B\", \"C\" FROM \"PUBLIC\".\"TEST\" /* PUBLIC.IDX_B_C: B = 1 AND C = 1 */ " +
                        "/* scanCount: 3 */ WHERE ROW (\"B\", \"C\") = ROW (1, 1)");
        stat.close();
    }

    /**
     * Executes a query with multiple EQ conditions against a compound index.
     */
    private void multipleEqAgainstCompoundIndex(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("EXPLAIN ANALYZE SELECT b, c FROM test WHERE b=1 AND c=1");
        rs.next();
        assertContains(rs.getString(1).replaceAll("[\\r\\n\\s]+", " "),
                "SELECT \"B\", \"C\" FROM \"PUBLIC\".\"TEST\" /* PUBLIC.IDX_B_C: B = 1 AND C = 1 */ " +
                        "/* scanCount: 3 */ WHERE (\"B\" = 1) AND (\"C\" = 1)");
        stat.close();
    }

}
