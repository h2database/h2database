/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import org.h2.command.dml.SetTypes;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test subquery performance with lazy query execution mode {@link SetTypes#LAZY_QUERY_EXECUTION}.
 */
public class TestSubqueryPerformanceOnLazyExecutionMode extends TestDb {
    /** Rows count. */
    private static final int ROWS = 5000;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        deleteDb("lazySubq");
        try (Connection conn = getConnection("lazySubq")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE one (x INTEGER, y INTEGER )");

                try (PreparedStatement prep = conn.prepareStatement("insert into one values (?,?)")) {
                    for (int row = 0; row < ROWS; row++) {
                        prep.setInt(1, row / 100);
                        prep.setInt(2, row);
                        prep.execute();
                    }
                }

                testSubqueryInCondition(stmt);
                testSubqueryInJoin(stmt);
            }
        }
        finally {
            deleteDb("lazySubq");
        }
    }

    public void testSubqueryInCondition(Statement stmt) throws Exception {
        String sql = "SELECT COUNT (*) FROM one WHERE x IN (SELECT y FROM one WHERE y < 50)";

        long tNotLazy = executeAndCheckResult(stmt, sql, false);
        long tLazy = executeAndCheckResult(stmt, sql, true);

        assertTrue("Lazy execution too slow. lazy time: "
                        + tLazy + ", not lazy time: " + tNotLazy,
                tNotLazy * 5 > tLazy);
    }

    public void testSubqueryInJoin(Statement stmt) throws Exception {
        String sql =
                "SELECT COUNT (one.x) FROM one " +
                "JOIN (SELECT y AS val FROM one WHERE y < 50) AS subq ON subq.val=one.x";

        long tNotLazy = executeAndCheckResult(stmt, sql, false);
        long tLazy = executeAndCheckResult(stmt, sql, true);

        assertTrue("Lazy execution too slow. lazy time: "
                        + tLazy + ", not lazy time: " + tNotLazy,
                tNotLazy * 5 > tLazy);
    }

    /**
     * @return Time of the query execution.
     */
    private long executeAndCheckResult(Statement stmt, String sql, boolean lazy) throws SQLException {
        if (lazy) {
            stmt.execute("SET LAZY_QUERY_EXECUTION 1");
        }
        else {
            stmt.execute("SET LAZY_QUERY_EXECUTION 0");
        }

        long t0 = System.currentTimeMillis();
        try (ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            assertEquals(ROWS, rs.getInt(1));
        }

        return System.currentTimeMillis() - t0;
    }
}
