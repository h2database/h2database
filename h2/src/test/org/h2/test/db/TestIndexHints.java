/*
 * Copyright 2004-2017 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import org.h2.test.TestBase;

import java.sql.*;

/**
 * Tests the index hints feature of this database.
 */
public class TestIndexHints extends TestBase {

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
        deleteDb("indexhints");
        createDb();
        testWithSingleIndexName();
        testWithEmptyIndexHintsList();
        testWithInvalidIndexName();
        testWithMultipleIndexNames();
        testWithTableAlias();
        testWithTableAliasCalledUse();
        deleteDb("indexhints");
    }

    private void createDb() throws SQLException {
        Connection conn = getConnection("indexhints");
        Statement stat = conn.createStatement();
        stat.execute("create table test (x int, y int)");
        stat.execute("create index idx1 on test (x)");
        stat.execute("create index idx2 on test (x, y)");
    }

    private void testWithSingleIndexName() throws SQLException {
        Connection conn = getConnection("indexhints");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("explain analyze select * " +
                "from test use index(idx2) where x=1 and y=1");
        rs.next();
        String result = rs.getString(1);
        assertTrue(result.contains("/* PUBLIC.IDX2:"));
        conn.close();
    }

    private void testWithTableAlias() throws SQLException {
        Connection conn = getConnection("indexhints");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("explain analyze select * " +
                "from test t use index(idx2) where x=1 and y=1");
        rs.next();
        String result = rs.getString(1);
        assertTrue(result.contains("/* PUBLIC.IDX2:"));
        conn.close();
    }

    private void testWithTableAliasCalledUse() throws SQLException {
        // make sure that while adding new syntax for table hints, code
        // that uses "USE" as a table alias still works
        Connection conn = getConnection("indexhints");
        Statement stat = conn.createStatement();
        stat.executeQuery("explain analyze select * " +
                "from test use where use.x=1 and use.y=1");
        conn.close();
    }

    private void testWithMultipleIndexNames() throws SQLException {
        Connection conn = getConnection("indexhints");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("explain analyze select * " +
                "from test use index(idx1, idx2) where x=1 and y=1");
        rs.next();
        String result = rs.getString(1);
        assertTrue(result.contains("/* PUBLIC.IDX2:"));
        conn.close();
    }

    private void testWithEmptyIndexHintsList() throws SQLException {
        Connection conn = getConnection("indexhints");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("explain analyze select * " +
                "from test use index () where x=1 and y=1");
        rs.next();
        String result = rs.getString(1);
        assertTrue(result.contains("/* PUBLIC.TEST.tableScan"));
        conn.close();
    }

    private void testWithInvalidIndexName() throws SQLException {
        Connection conn = getConnection("indexhints");
        Statement stat = conn.createStatement();
        try {
            stat.executeQuery("explain analyze select * " +
                    "from test use index(idx_doesnt_exist) where x=1 and y=1");
            fail("Expected exception: "
                    + "Index \"IDX_DOESNT_EXIST\" not found");
        } catch (SQLException e) {
            assert(e.getErrorCode() == 1);
        } finally {
            conn.close();

        }
    }

}
