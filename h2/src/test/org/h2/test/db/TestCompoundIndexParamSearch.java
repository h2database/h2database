/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import org.h2.test.TestBase;
import org.h2.test.TestDb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Test various queries with hardcoded, and
 * {@link PreparedStatement#setObject(int, Object) prepared statement}
 * parameters. The test cases are the same as in {@link TestCompoundIndexSearch}
 * but we are checking whether the hard coded, and the passed parameters works
 * as the same.
 */
public class TestCompoundIndexParamSearch extends TestDb {

    private static final String DB_NAME = "paramSearch";

    private static final Pattern SCAN_COUNT_PATTERN = Pattern.compile("\\/\\* scanCount: (\\d+) \\*\\/");

    /**
     * Run just this test.
     *
     * @param a
     *            ignored
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
        compoundInNoIndexAndNull(conn);
        compoundInAgainstCompoundIndex(conn);
        compoundInAgainstCompoundIndexUnordered(conn);
        compoundInAgainstSimpleIndex(conn);
        compoundEqAgainstCompoundIndex(conn);
        multipleEqAgainstCompoundIndex(conn);

        conn.close();
        deleteDb(DB_NAME);
    }

    private Connection prepare() throws Exception {
        deleteDb(DB_NAME);
        Connection conn = getConnection(DB_NAME);
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE test (a INT, b INT, c CHAR, d INT);");
        stat.execute("CREATE INDEX idx_a ON test(a);");
        stat.execute("CREATE INDEX idx_b_c ON test(b, c);");
        stat.execute("INSERT INTO test (a, b, c, d) VALUES " + "(1, 1, '1', 1), " + "(1, 1, '2', 2), " //
                + "(1, 3, '3', 3), " + "(2, 2, '1', 4), " + "(2, 3, '2', 1), " + "(2, 3, '3', 2), " //
                + "(3, 2, '1', 3), " + "(3, 2, '2', 4), " + "(3, 3, '3', 1), " + "(4, 1, '1', 2);");
        stat.close();
        return conn;
    }

    private static String findScanCount(String input) {
        Matcher matcher = SCAN_COUNT_PATTERN.matcher(input.replaceAll("[\\r\\n\\s]+", " "));
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    /**
     * Executes a query with a simple IN condition against an indexed column.
     */
    private void simpleInAgainstSimpleIndexCheck(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("EXPLAIN ANALYZE SELECT b, c FROM test WHERE (a) IN (1, 4)");
        rs.next();
        String expected = findScanCount(rs.getString(1));
        stat.close();

        PreparedStatement pStat = conn.prepareStatement("EXPLAIN ANALYZE SELECT b, c FROM test WHERE (a) IN (1, ?)");
        pStat.setInt(1, 4);
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();

        pStat = conn.prepareStatement("EXPLAIN ANALYZE SELECT b, c FROM test WHERE (a) IN (?, ?)");
        pStat.setInt(1, 1);
        pStat.setInt(2, 4);
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();
    }

    /**
     * Executes a query with a simple IN condition against a compound index. The
     * lookup column is the first component of the index, so the lookup works as
     * it was a simple index.
     */
    private void simpleInAgainstFirstCompoundIndex(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("EXPLAIN ANALYZE SELECT b, c FROM test WHERE b IN (1, 2)");
        rs.next();
        String expected = findScanCount(rs.getString(1));
        stat.close();

        PreparedStatement pStat = conn.prepareStatement("EXPLAIN ANALYZE SELECT b, c FROM test WHERE b IN (1, ?)");
        pStat.setInt(1, 2);
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();

        pStat = conn.prepareStatement("EXPLAIN ANALYZE SELECT b, c FROM test WHERE b IN (?, ?)");
        pStat.setInt(1, 1);
        pStat.setInt(2, 2);
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();
    }

    /**
     * Executes a query with a simple IN condition against a compound index. The
     * lookup column is the second component of the index, so a full table scan
     * happens.
     */
    private void simpleInAgainstSecondCompoundIndex(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("EXPLAIN ANALYZE SELECT b, c FROM test WHERE c IN ('1', '2')");
        rs.next();
        String expected = findScanCount(rs.getString(1));
        stat.close();

        PreparedStatement pStat = conn.prepareStatement("EXPLAIN ANALYZE SELECT b, c FROM test WHERE c IN ('1', ?)");
        pStat.setString(1, "2");
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();

        pStat = conn.prepareStatement("EXPLAIN ANALYZE SELECT b, c FROM test WHERE c IN (?, ?)");
        pStat.setString(1, "1");
        pStat.setString(2, "2");
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();
    }

    /**
     * Executes a query with a compound IN condition against a compound index.
     */
    private void compoundInAgainstCompoundIndex(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("EXPLAIN ANALYZE SELECT b, c FROM test WHERE (b, c) IN ((2, '1'), (3, '2'))");
        rs.next();
        String expected = findScanCount(rs.getString(1));
        stat.close();

        PreparedStatement pStat = conn
                .prepareStatement("EXPLAIN ANALYZE SELECT b, c FROM test WHERE (b, c) IN ((2, ?), (3, ?))");
        pStat.setString(1, "1");
        pStat.setString(2, "2");
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();

        pStat = conn.prepareStatement("EXPLAIN ANALYZE SELECT b, c FROM test WHERE (b, c) IN ((?, ?), (?, ?))");
        pStat.setInt(1, 2);
        pStat.setString(2, "1");
        pStat.setInt(3, 3);
        pStat.setString(4, "2");
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();
    }

    /**
     * Executes a query with a compound IN condition against a compound index,
     * but the condition columns are in different order than in the index.<br />
     * condition (c, b) vs index (b, c)
     */
    private void compoundInAgainstCompoundIndexUnordered(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("EXPLAIN ANALYZE SELECT b, c FROM test WHERE (c, b) IN (('1', 2), ('2', 3))");
        rs.next();
        String expected = findScanCount(rs.getString(1));
        stat.close();

        PreparedStatement pStat = conn
                .prepareStatement("EXPLAIN ANALYZE SELECT b, c FROM test WHERE (c, b) IN (('1', ?), ('2', ?))");
        pStat.setInt(1, 2);
        pStat.setInt(2, 3);
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();

        pStat = conn.prepareStatement("EXPLAIN ANALYZE SELECT b, c FROM test WHERE (c, b) IN ((?, ?), (?, ?))");
        pStat.setString(1, "1");
        pStat.setInt(2, 2);
        pStat.setString(3, "2");
        pStat.setInt(4, 3);
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();
    }

    /**
     * Executes a query with a compound IN condition. Creates a table on the fly
     * without any indexes. The table and the query both contain NULL values.
     */
    private void compoundInNoIndexAndNull(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST_NULL(A INT, B INT) AS (VALUES (1, 1), (1, 2), (2, 1), (2, NULL));");
        ResultSet rs = stat.executeQuery(
                "EXPLAIN ANALYZE SELECT * FROM TEST_NULL " + "WHERE (A, B) IN ((1, 1), (2, 1), (2, 2), (2, NULL))");
        rs.next();
        String expected = findScanCount(rs.getString(1));
        stat.close();

        PreparedStatement pStat = conn.prepareStatement(
                "EXPLAIN ANALYZE SELECT * FROM TEST_NULL " + "WHERE (A, B) IN ((1, ?), (2, ?), (2, ?), (2, ?))");
        pStat.setInt(1, 1);
        pStat.setInt(2, 1);
        pStat.setInt(3, 2);
        pStat.setObject(4, null);
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();

        pStat = conn.prepareStatement(
                "EXPLAIN ANALYZE SELECT * FROM TEST_NULL " + "WHERE (A, B) IN ((?, ?), (?, ?), (?, ?), (?, ?))");
        pStat.setInt(1, 1);
        pStat.setInt(2, 1);
        pStat.setInt(3, 1);
        pStat.setInt(4, 2);
        pStat.setInt(5, 2);
        pStat.setInt(6, 1);
        pStat.setInt(7, 2);
        pStat.setObject(8, null);
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();
    }

    /**
     * Executes a query with a compound IN condition against a simple index.
     */
    private void compoundInAgainstSimpleIndex(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("EXPLAIN ANALYZE SELECT a, d FROM test WHERE (a, d) IN ((1, 3), (2, 4))");
        rs.next();
        String expected = findScanCount(rs.getString(1));
        stat.close();

        PreparedStatement pStat = conn
                .prepareStatement("EXPLAIN ANALYZE SELECT a, d FROM test WHERE (a, d) IN ((1, ?), (2, ?))");
        pStat.setInt(1, 3);
        pStat.setInt(2, 2);
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();

        pStat = conn.prepareStatement("EXPLAIN ANALYZE SELECT a, d FROM test WHERE (a, d) IN ((?, ?), (?, ?))");
        pStat.setInt(1, 1);
        pStat.setInt(2, 3);
        pStat.setInt(3, 2);
        pStat.setInt(4, 4);
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();
    }

    /**
     * Executes a query with a compound EQ condition against a compound index.
     */
    private void compoundEqAgainstCompoundIndex(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("EXPLAIN ANALYZE SELECT b, c FROM test WHERE (b, c) = (1, '1')");
        rs.next();
        String expected = findScanCount(rs.getString(1));
        stat.close();

        PreparedStatement pStat = conn.prepareStatement("EXPLAIN ANALYZE SELECT b, c FROM test WHERE (b, c) = (1, ?)");
        pStat.setString(1, "1");
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();

        pStat = conn.prepareStatement("EXPLAIN ANALYZE SELECT b, c FROM test WHERE (b, c) = (?, ?)");
        pStat.setInt(1, 1);
        pStat.setString(2, "1");
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();
    }

    /**
     * Executes a query with multiple EQ conditions against a compound index.
     */
    private void multipleEqAgainstCompoundIndex(Connection conn) throws Exception {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("EXPLAIN ANALYZE SELECT b, c FROM test WHERE b=1 AND c='1'");
        rs.next();
        String expected = findScanCount(rs.getString(1));
        stat.close();

        PreparedStatement pStat = conn.prepareStatement("EXPLAIN ANALYZE SELECT b, c FROM test WHERE b=1 AND c=?");
        pStat.setString(1, "1");
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();

        pStat = conn.prepareStatement("EXPLAIN ANALYZE SELECT b, c FROM test WHERE b=? AND c=?");
        pStat.setInt(1, 1);
        pStat.setString(2, "1");
        rs = pStat.executeQuery();
        rs.next();
        assertEquals(findScanCount(rs.getString(1)), expected);
        pStat.close();
    }

}
