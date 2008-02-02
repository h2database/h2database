/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.h2.test.TestBase;

/**
 * Transaction isolation level tests.
 */
public class TestTransactionIsolation extends TestBase {

    Connection conn1, conn2;

    public void test() throws Exception {
        if (config.mvcc) {
            // no tests yet
        } else {
            testTableLevelLocking();
        }
    }

    void testTableLevelLocking() throws Exception {
        deleteDb("transactionIsolation");
        conn1 = getConnection("transactionIsolation");
        check(conn1.getTransactionIsolation(), Connection.TRANSACTION_READ_COMMITTED);
        conn1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        check(conn1.getTransactionIsolation(), Connection.TRANSACTION_SERIALIZABLE);
        conn1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        check(conn1.getTransactionIsolation(), Connection.TRANSACTION_READ_UNCOMMITTED);
        checkSingleValue(conn1.createStatement(), "CALL LOCK_MODE()", 0);
        conn1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        checkSingleValue(conn1.createStatement(), "CALL LOCK_MODE()", 3);
        check(conn1.getTransactionIsolation(), Connection.TRANSACTION_READ_COMMITTED);
        conn1.createStatement().execute("SET LOCK_MODE 1");
        check(conn1.getTransactionIsolation(), Connection.TRANSACTION_SERIALIZABLE);
        conn1.createStatement().execute("CREATE TABLE TEST(ID INT)");
        conn1.createStatement().execute("INSERT INTO TEST VALUES(1)");
        conn1.setAutoCommit(false);

        conn2 = getConnection("transactionIsolation");
        conn2.setAutoCommit(false);

        conn1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        // serializable: just reading
        checkSingleValue(conn1.createStatement(), "SELECT * FROM TEST", 1);
        checkSingleValue(conn2.createStatement(), "SELECT * FROM TEST", 1);
        conn1.commit();
        conn2.commit();

        // serializable: write lock
        conn1.createStatement().executeUpdate("UPDATE TEST SET ID=2");
        try {
            checkSingleValue(conn2.createStatement(), "SELECT * FROM TEST", 1);
            error("Expected lock timeout");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        conn1.commit();
        conn2.commit();

        conn1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        // read-committed: #1 read, #2 update, #1 read again
        checkSingleValue(conn1.createStatement(), "SELECT * FROM TEST", 2);
        conn2.createStatement().executeUpdate("UPDATE TEST SET ID=3");
        conn2.commit();
        checkSingleValue(conn1.createStatement(), "SELECT * FROM TEST", 3);
        conn1.commit();

        // read-committed: #1 read, #2 read, #2 update, #1 delete
        checkSingleValue(conn1.createStatement(), "SELECT * FROM TEST", 3);
        checkSingleValue(conn2.createStatement(), "SELECT * FROM TEST", 3);
        conn2.createStatement().executeUpdate("UPDATE TEST SET ID=4");
        try {
            conn1.createStatement().executeUpdate("DELETE FROM TEST");
            error("Expected lock timeout");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        conn2.commit();
        conn1.commit();
        checkSingleValue(conn1.createStatement(), "SELECT * FROM TEST", 4);
        checkSingleValue(conn2.createStatement(), "SELECT * FROM TEST", 4);

        conn1.close();
        conn2.close();
    }

}
