/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.jdbc.JdbcConnection;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

public class TestAlterTableNotFound extends TestDb {

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
        testWithoutAnyCandidate();
        testWithoutAnyCandidateWhenDatabaseToLower();
        testWithoutAnyCandidateWhenDatabaseToUpper();
        testWithoutAnyCandidateWhenCaseInsensitiveIdentifiers();
        testWithOneCandidate();
        testWithOneCandidateWhenDatabaseToLower();
        testWithOneCandidateWhenDatabaseToUpper();
        testWithOneCandidateWhenCaseInsensitiveIdentifiers();
        testWithTwoCandidates();
    }

    private void testWithoutAnyCandidate() throws SQLException {
        deleteDb(getTestName());
        final Connection conn = getJdbcConnection("DATABASE_TO_LOWER=false;DATABASE_TO_UPPER=false");
        final Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE T2 ( ID INT IDENTITY )");
        try {
            stat.execute("ALTER TABLE t1 DROP COLUMN ID");
            fail("Table `t1` was accessible but should not have been.");
        } catch (SQLException e) {
            final String message = e.getMessage();
            assertContains(message, "Table \"t1\" not found;");
        }

        conn.close();
        deleteDb(getTestName());
    }

    private void testWithoutAnyCandidateWhenDatabaseToLower() throws SQLException {
        deleteDb(getTestName());
        final Connection conn = getJdbcConnection("DATABASE_TO_LOWER=true;DATABASE_TO_UPPER=false");
        final Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE T2 ( ID INT IDENTITY )");
        try {
            stat.execute("ALTER TABLE T1 DROP COLUMN ID");
            fail("Table `t1` was accessible but should not have been.");
        } catch (SQLException e) {
            final String message = e.getMessage();
            assertContains(message, "Table \"t1\" not found;");
        }

        conn.close();
        deleteDb(getTestName());
    }

    private void testWithoutAnyCandidateWhenDatabaseToUpper() throws SQLException {
        deleteDb(getTestName());
        final Connection conn = getJdbcConnection("DATABASE_TO_LOWER=false;DATABASE_TO_UPPER=true");
        final Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE T2 ( ID INT IDENTITY )");
        try {
            stat.execute("ALTER TABLE t1 DROP COLUMN ID");
            fail("Table `T1` was accessible but should not have been.");
        } catch (SQLException e) {
            final String message = e.getMessage();
            assertContains(message, "Table \"T1\" not found;");
        }

        conn.close();
        deleteDb(getTestName());
    }

    private void testWithoutAnyCandidateWhenCaseInsensitiveIdentifiers() throws SQLException {
        deleteDb(getTestName());
        final Connection conn = getJdbcConnection(
                "DATABASE_TO_LOWER=false;DATABASE_TO_UPPER=false;CASE_INSENSITIVE_IDENTIFIERS=true");
        final Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE T2 ( ID INT IDENTITY )");
        try {
            stat.execute("ALTER TABLE t1 DROP COLUMN ID");
            fail("Table `t1` was accessible but should not have been.");
        } catch (SQLException e) {
            final String message = e.getMessage();
            assertContains(message, "Table \"t1\" not found;");
        }

        conn.close();
        deleteDb(getTestName());
    }

    private void testWithOneCandidate() throws SQLException {
        deleteDb(getTestName());
        final Connection conn = getJdbcConnection("DATABASE_TO_LOWER=false;DATABASE_TO_UPPER=false");
        final Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE T1 ( ID INT IDENTITY )");
        try {
            stat.execute("ALTER TABLE t1 DROP COLUMN ID");
            fail("Table `t1` was accessible but should not have been.");
        } catch (SQLException e) {
            final String message = e.getMessage();
            assertContains(message, "Table \"t1\" not found (candidates are: \"`T1`\")");
        }

        conn.close();
        deleteDb(getTestName());
    }

    private void testWithOneCandidateWhenDatabaseToLower() throws SQLException {
        deleteDb(getTestName());
        final Connection conn = getJdbcConnection("DATABASE_TO_LOWER=true;DATABASE_TO_UPPER=false");
        final Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE t1 ( ID INT IDENTITY, PAYLOAD INT )");
        stat.execute("ALTER TABLE T1 DROP COLUMN PAYLOAD");
        conn.close();
        deleteDb(getTestName());
    }

    private void testWithOneCandidateWhenDatabaseToUpper() throws SQLException {
        deleteDb(getTestName());
        final Connection conn = getJdbcConnection("DATABASE_TO_LOWER=false;DATABASE_TO_UPPER=true");
        final Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE T1 ( ID INT IDENTITY, PAYLOAD INT )");
        stat.execute("ALTER TABLE t1 DROP COLUMN PAYLOAD");
        conn.close();
        deleteDb(getTestName());
    }

    private void testWithOneCandidateWhenCaseInsensitiveIdentifiers() throws SQLException {
        deleteDb(getTestName());
        final Connection conn = getJdbcConnection(
                "DATABASE_TO_LOWER=false;DATABASE_TO_UPPER=false;CASE_INSENSITIVE_IDENTIFIERS=true");
        final Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE T1 ( ID INT IDENTITY, PAYLOAD INT )");
        stat.execute("ALTER TABLE t1 DROP COLUMN PAYLOAD");
        conn.close();
        deleteDb(getTestName());
    }

    private void testWithTwoCandidates() throws SQLException {
        deleteDb(getTestName());
        final Connection conn = getJdbcConnection("DATABASE_TO_LOWER=false;DATABASE_TO_UPPER=false");
        final Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE Toast ( ID INT IDENTITY )");
        stat.execute("CREATE TABLE TOAST ( ID INT IDENTITY )");
        try {
            stat.execute("ALTER TABLE toast DROP COLUMN ID");
            fail("Table `toast` was accessible but should not have been.");
        } catch (SQLException e) {
            final String message = e.getMessage();
            assertContains(message, "Table \"toast\" not found (candidates are: \"`TOAST`, `Toast`\")");
        }

        conn.close();
        deleteDb(getTestName());
    }

    private JdbcConnection getJdbcConnection(final String settings) throws SQLException {
        return (JdbcConnection) getConnection(getTestName() + ";" + settings);
    }
}
