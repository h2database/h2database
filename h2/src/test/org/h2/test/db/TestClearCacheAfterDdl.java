/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import static org.h2.api.ErrorCode.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Tests for clearing query ache after DDL execution: After executing DDL that
 * invalidates previously cached queries, re-executing the same SQL should fail.
 * When the referenced object no longer exists or was renamed. This asserts that
 * cached prepared statements were cleared.
 *
 * @author Seungyong Hong
 */
public class TestClearCacheAfterDdl extends TestDb {

    public static String staticIdentityFunction(String str) {
        return str;
    }

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
    public void test() throws SQLException {
        testTableDrop();
        testTableRename();
        testColumnDrop();
        testColumnRename();
        testViewDrop();
        testSynonymDrop();
        testSequenceDrop();
        testSchemaRenameQualified();
        testSchemaDropCascade();
        testAliasDrop();
    }

    private void expectErrorAfterDdl(Connection connection, String sqlToCache, String ddl, int errorCode)
            throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(sqlToCache);
        statement.execute(ddl);
        assertThrows(errorCode, statement, sqlToCache);
    }

    private void testTableDrop() throws SQLException {
        Connection connection = getConnection("clearCacheAfterDdl_tableDrop");
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE T_DROP(ID INT PRIMARY KEY)");
        expectErrorAfterDdl(connection, "SELECT * FROM T_DROP", "DROP TABLE T_DROP",
                TABLE_OR_VIEW_NOT_FOUND_DATABASE_EMPTY_1);
        connection.close();
    }

    private void testTableRename() throws SQLException {
        Connection connection = getConnection("clearCacheAfterDdl_tableRename");
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE T_RENAME(ID INT PRIMARY KEY)");
        expectErrorAfterDdl(connection, "SELECT * FROM T_RENAME", "ALTER TABLE T_RENAME RENAME TO T_RENAMED",
                TABLE_OR_VIEW_NOT_FOUND_1);
        connection.close();
    }

    private void testColumnDrop() throws SQLException {
        Connection connection = getConnection("clearCacheAfterDdl_colDrop");
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE T_COL_DROP(A INT, B INT, PRIMARY KEY(A))");
        expectErrorAfterDdl(connection, "SELECT B FROM T_COL_DROP", "ALTER TABLE T_COL_DROP DROP COLUMN B",
                COLUMN_NOT_FOUND_1);
        connection.close();
    }

    private void testColumnRename() throws SQLException {
        Connection connection = getConnection("clearCacheAfterDdl_colRename");
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE T_COL_RENAME(A INT, B INT, PRIMARY KEY(A))");
        expectErrorAfterDdl(connection, "SELECT B FROM T_COL_RENAME",
                "ALTER TABLE T_COL_RENAME ALTER COLUMN B RENAME TO C", COLUMN_NOT_FOUND_1);
        connection.close();
    }

    private void testViewDrop() throws SQLException {
        Connection connection = getConnection("clearCacheAfterDdl_viewDrop");
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE V_BACK(ID INT PRIMARY KEY)");
        statement.execute("CREATE VIEW V1 AS SELECT * FROM V_BACK");
        expectErrorAfterDdl(connection, "SELECT * FROM V1", "DROP VIEW V1", TABLE_OR_VIEW_NOT_FOUND_1);
        connection.close();
    }

    private void testSynonymDrop() throws SQLException {
        Connection connection = getConnection("clearCacheAfterDdl_synonym");
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE BACKINGTABLE(ID INT PRIMARY KEY)");
        statement.execute("CREATE OR REPLACE SYNONYM TESTSYNONYM FOR BACKINGTABLE");
        expectErrorAfterDdl(connection, "SELECT * FROM TESTSYNONYM", "DROP SYNONYM TESTSYNONYM",
                TABLE_OR_VIEW_NOT_FOUND_1);
        connection.close();
    }

    private void testSequenceDrop() throws SQLException {
        Connection connection = getConnection("clearCacheAfterDdl_sequence");
        Statement statement = connection.createStatement();
        statement.execute("CREATE SEQUENCE SEQ1");
        expectErrorAfterDdl(connection, "SELECT NEXT VALUE FOR SEQ1", "DROP SEQUENCE SEQ1", SEQUENCE_NOT_FOUND_1);
        connection.close();
    }

    private void testSchemaRenameQualified() throws SQLException {
        Connection connection = getConnection("clearCacheAfterDdl_schemaRename");
        Statement statement = connection.createStatement();
        statement.execute("CREATE SCHEMA S1");
        statement.execute("CREATE TABLE S1.T(ID INT PRIMARY KEY)");
        expectErrorAfterDdl(connection, "SELECT * FROM S1.T", "ALTER SCHEMA S1 RENAME TO S2", SCHEMA_NOT_FOUND_1);
        connection.close();
    }

    private void testSchemaDropCascade() throws SQLException {
        Connection connection = getConnection("clearCacheAfterDdl_schemaDrop");
        Statement statement = connection.createStatement();
        statement.execute("CREATE SCHEMA SD");
        statement.execute("CREATE TABLE SD.T(ID INT PRIMARY KEY)");
        expectErrorAfterDdl(connection, "SELECT * FROM SD.T", "DROP SCHEMA SD CASCADE", SCHEMA_NOT_FOUND_1);
        connection.close();
    }

    private void testAliasDrop() throws SQLException {
        Connection connection = getConnection("clearCacheAfterDdl_alias");
        Statement statement = connection.createStatement();
        statement.execute("CREATE ALIAS F1 FOR \"org.h2.test.db.TestClearCacheAfterDdl.staticIdentityFunction\"");
        expectErrorAfterDdl(connection, "SELECT F1('something')", "DROP ALIAS F1", FUNCTION_NOT_FOUND_1);
        connection.close();
    }
}
