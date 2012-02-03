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
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
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
        System.setProperty("h2.selectForUpdateMvcc", "true");
        TestBase test = TestBase.createCaller().init();
        test.config.mvcc = true;
        test.test();
    }

    public void test() throws SQLException {
        if (!config.mvcc) {
            return;
        }
        deleteDb("mvcc2");
        testSelectForUpdate();
        testInsertUpdateRollback();
        testInsertRollback();
        deleteDb("mvcc2");
    }

    private Connection getConnection() throws SQLException {
        return getConnection("mvcc2");
    }

    private void testSelectForUpdate() throws SQLException {
        if (!SysProperties.SELECT_FOR_UPDATE_MVCC) {
            return;
        }
        Connection conn = getConnection();
        Connection conn2 = getConnection();
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        conn.setAutoCommit(false);
        stat.execute("insert into test select x, 'Hello' from system_range(1, 10)");
        stat.execute("select * from test where id = 3 for update");
        conn.commit();
        try {
            stat.execute("select sum(id) from test for update");
            fail();
        } catch (SQLException e) {
            assertEquals(ErrorCode.FEATURE_NOT_SUPPORTED_1, e.getErrorCode());
        }
        try {
            stat.execute("select distinct id from test for update");
            fail();
        } catch (SQLException e) {
            assertEquals(ErrorCode.FEATURE_NOT_SUPPORTED_1, e.getErrorCode());
        }
        try {
            stat.execute("select id from test group by id for update");
            fail();
        } catch (SQLException e) {
            assertEquals(ErrorCode.FEATURE_NOT_SUPPORTED_1, e.getErrorCode());
        }
        try {
            stat.execute("select t1.id from test t1, test t2 for update");
            fail();
        } catch (SQLException e) {
            assertEquals(ErrorCode.FEATURE_NOT_SUPPORTED_1, e.getErrorCode());
        }
        stat.execute("select * from test where id = 3 for update");
        conn2.setAutoCommit(false);
        conn2.createStatement().execute("select * from test where id = 4 for update");
        try {
            conn2.createStatement().execute("select * from test where id = 3 for update");
            fail();
        } catch (SQLException e) {
            assertEquals(ErrorCode.CONCURRENT_UPDATE_1, e.getErrorCode());
        }
        conn.close();
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
