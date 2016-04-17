/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import org.h2.test.TestBase;

import java.sql.*;

/**
 * Test for the read-only database feature.
 */
public class TestSynonymForTable extends TestBase {

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
        testSelectFromSynonym();
        testInsertIntoSynonym();
        testDeleteFromSynonym();
        testTruncateSynonym();
        // TODO: check create existing tablename
        // TODO: check create for non existing table
        // TODO: Test Meta Data
        // TODO: CREATE OR REPLACE TableSynonym.
        // TODO: Check schema name in synonym and table
        testReopenDatabase();
        // TODO: test drop synonym.
    }

    /**
     * Make sure, that the schema changes are persisted when reopening the database
     */
    private void testReopenDatabase() throws SQLException {
        Connection conn = getConnection("synonym");
        createTableWithSynonym(conn);
        insertIntoBackingTable(conn, 9);
        conn.close();
        Connection conn2 = getConnection("synonym");
        assertSynonymContains(conn2, 9);
        conn2.close();
    }

    private void testTruncateSynonym() throws SQLException {
        Connection conn = getConnection("synonym");
        createTableWithSynonym(conn);

        insertIntoBackingTable(conn, 7);
        assertBackingTableContains(conn, 7);

        conn.createStatement().execute("TRUNCATE TABLE testsynonym");

        assertBackingTableIsEmpty(conn);
        conn.close();
    }

    private void testDeleteFromSynonym() throws SQLException {
        Connection conn = getConnection("synonym");
        createTableWithSynonym(conn);

        insertIntoBackingTable(conn, 7);
        assertBackingTableContains(conn, 7);
        deleteFromSynonym(conn, 7);

        assertBackingTableIsEmpty(conn);
        conn.close();
    }

    private void deleteFromSynonym(Connection conn, int id) throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "DELETE FROM testsynonym WHERE id = ?");
        prep.setInt(1, id);
        prep.execute();
    }

    private void assertBackingTableIsEmpty(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT id FROM backingtable");
        assertFalse(rs.next());
    }

    private void testInsertIntoSynonym() throws SQLException {
        Connection conn = getConnection("synonym");
        createTableWithSynonym(conn);

        insertIntoSynonym(conn, 5);
        assertBackingTableContains(conn, 5);
        conn.close();
    }

    private void assertBackingTableContains(Connection conn, int testValue) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT id FROM backingtable");
        assertTrue(rs.next());
        assertEquals(testValue, rs.getInt(1));
        assertFalse(rs.next());
    }

    private void testSelectFromSynonym() throws SQLException {
        deleteDb("synonym");
        Connection conn = getConnection("synonym");
        createTableWithSynonym(conn);
        insertIntoBackingTable(conn, 1);
        assertSynonymContains(conn, 1);
        conn.close();
    }

    private void assertSynonymContains(Connection conn, int id) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT id FROM testsynonym");
        assertTrue(rs.next());
        assertEquals(id, rs.getInt(1));
        assertFalse(rs.next());
    }

    private void insertIntoSynonym(Connection conn, int id) throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "insert into testsynonym values(?)");
        prep.setInt(1, id);
        prep.execute();
    }

    private void insertIntoBackingTable(Connection conn, int id) throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "insert into backingtable values(?)");
        prep.setInt(1, id);
        prep.execute();
    }

    private void createTableWithSynonym(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE IF NOT EXISTS backingtable(id INT PRIMARY KEY)");
        stat.execute("CREATE SYNONYM IF NOT EXISTS testsynonym FOR backingtable");
        stat.execute("TRUNCATE TABLE backingtable");
    }

}
