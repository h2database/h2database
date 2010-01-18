/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.mvcc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;

/**
 * Additional MVCC (multi version concurrency) test cases.
 */
public class TestMvcc2 extends TestBase {

    private static final String DROP_TABLE = "DROP TABLE IF EXISTS EMPLOYEE";
    private static final String CREATE_TABLE = "CREATE TABLE EMPLOYEE (id BIGINT, version BIGINT, NAME VARCHAR(255))";
    private static final String INSERT = "INSERT INTO EMPLOYEE (id, version, NAME) VALUES (1, 1, 'Jones')";
    private static final String UPDATE = "UPDATE EMPLOYEE SET NAME = 'Miller' WHERE version = 1";

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        if (!config.mvcc) {
            return;
        }
        deleteDb("mvcc2");
        testInsertUpdateRollback();
        testInsertRollback();
        deleteDb("mvcc2");
    }

    private Connection getConnection() throws SQLException {
        return getConnection("mvcc2");
    }

    private void testInsertUpdateRollback() throws SQLException {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.execute(DROP_TABLE);
        stmt.execute(CREATE_TABLE);
        conn.commit();
        stmt.execute(INSERT);
        stmt.execute(UPDATE);
        conn.rollback();
        conn.close();
    }

    private void testInsertRollback() throws SQLException {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.execute(DROP_TABLE);
        stmt.execute(CREATE_TABLE);
        conn.commit();
        stmt.execute(INSERT);
        conn.rollback();
        conn.close();
    }

}
