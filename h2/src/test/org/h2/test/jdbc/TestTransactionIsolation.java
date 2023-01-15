/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Transaction isolation level tests.
 */
public class TestTransactionIsolation extends TestDb {

    private Connection conn1, conn2;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws SQLException {
        testTableLevelLocking();
    }

    private void testTableLevelLocking() throws SQLException {
        deleteDb("transactionIsolation");

        conn1 = getConnection("transactionIsolation");
        conn1.setAutoCommit(false);

        conn2 = getConnection("transactionIsolation");
        conn2.setAutoCommit(false);

        assertEquals(Connection.TRANSACTION_READ_COMMITTED, conn1.getMetaData().getDefaultTransactionIsolation());
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, conn1.getTransactionIsolation());

        try (Connection conn = getConnection("transactionIsolation");
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE TEST(ID INT)");
        }
        testIt(Connection.TRANSACTION_READ_UNCOMMITTED);
        testIt(Connection.TRANSACTION_READ_COMMITTED);
        testIt(Connection.TRANSACTION_REPEATABLE_READ);
        testIt(Connection.TRANSACTION_SERIALIZABLE);

        try (Connection conn = getConnection("transactionIsolation");
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE TEST");
            stmt.execute("CREATE TABLE TEST(ID INT UNIQUE)");
        }
        testIt(Connection.TRANSACTION_READ_UNCOMMITTED);
        testIt(Connection.TRANSACTION_READ_COMMITTED);
        testIt(Connection.TRANSACTION_REPEATABLE_READ);
        testIt(Connection.TRANSACTION_SERIALIZABLE);

        conn2.close();
        conn1.close();
        deleteDb("transactionIsolation");
    }

    private void testIt(int isolationLevel2) throws SQLException {
        try (Connection conn = getConnection("transactionIsolation");
                Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM TEST");
            stmt.execute("INSERT INTO TEST VALUES(1)");
        }

        conn2.setTransactionIsolation(isolationLevel2);
        assertEquals(isolationLevel2, conn2.getTransactionIsolation());

        testRowLocks(Connection.TRANSACTION_READ_UNCOMMITTED);
        testRowLocks(Connection.TRANSACTION_READ_COMMITTED);
        testRowLocks(Connection.TRANSACTION_REPEATABLE_READ);
        testRowLocks(Connection.TRANSACTION_SERIALIZABLE);

        testDirtyRead(Connection.TRANSACTION_READ_UNCOMMITTED, 1, true, true);
        testDirtyRead(Connection.TRANSACTION_READ_COMMITTED, 2, false, true);
        testDirtyRead(Connection.TRANSACTION_REPEATABLE_READ, 3, false, false);
        testDirtyRead(Connection.TRANSACTION_SERIALIZABLE, 4, false, false);
    }

    private void testDirtyRead(int isolationLevel, int value, boolean dirtyVisible, boolean committedVisible)
            throws SQLException {
        conn1.setTransactionIsolation(isolationLevel);
        assertSingleValue(conn1.createStatement(), "SELECT * FROM TEST", value);
        int newValue = value + 1;
        conn2.createStatement().executeUpdate("UPDATE TEST SET ID=" + newValue);
        assertSingleValue(conn1.createStatement(), "SELECT * FROM TEST", dirtyVisible ? newValue  : value);
        conn2.commit();
        assertSingleValue(conn1.createStatement(), "SELECT * FROM TEST", committedVisible ? newValue : value);
    }

    private void testRowLocks(int isolationLevel) throws SQLException {
        conn1.setTransactionIsolation(isolationLevel);
        assertSingleValue(conn1.createStatement(), "SELECT * FROM TEST", 1);
        assertSingleValue(conn2.createStatement(), "SELECT * FROM TEST FOR UPDATE", 1);
        assertThrows(ErrorCode.LOCK_TIMEOUT_1, conn1.createStatement()).executeUpdate("DELETE FROM TEST");
        conn2.commit();
    }
}
