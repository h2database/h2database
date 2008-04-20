/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (license2)
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;

import org.h2.constant.SysProperties;
import org.h2.jdbc.JdbcStatement;
import org.h2.test.TestBase;

/**
 * Tests for the Statement implementation.
 */
public class TestStatement extends TestBase {

    Connection conn;

    public void test() throws Exception {
        deleteDb("statement");
        conn = getConnection("statement");
        if (config.jdk14) {
            testSavepoint();
        }
        testConnectionRollback();
        testStatement();
        if (config.jdk14) {
            testIdentity();
        }
        conn.close();
    }

    private void testConnectionRollback() throws Exception {
        Statement stat = conn.createStatement();
        conn.setAutoCommit(false);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        conn.rollback();
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        checkFalse(rs.next());
        stat.execute("DROP TABLE TEST");
        conn.setAutoCommit(true);
    }

    void testSavepoint() throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        conn.setAutoCommit(false);
        stat.execute("INSERT INTO TEST VALUES(0, 'Hi')");
        Savepoint savepoint1 = conn.setSavepoint();
        int id1 = savepoint1.getSavepointId();
        try {
            savepoint1.getSavepointName();
            error();
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        stat.execute("DELETE FROM TEST");
        conn.rollback(savepoint1);
        stat.execute("UPDATE TEST SET NAME='Hello'");
        Savepoint savepoint2a = conn.setSavepoint();
        Savepoint savepoint2 = conn.setSavepoint();
        conn.releaseSavepoint(savepoint2a);
        try {
            savepoint2a.getSavepointId();
            error();
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        int id2 = savepoint2.getSavepointId();
        check(id1 != id2);
        stat.execute("UPDATE TEST SET NAME='Hallo' WHERE NAME='Hello'");
        Savepoint savepointTest = conn.setSavepoint("Joe's");
        stat.execute("DELETE FROM TEST");
        check(savepointTest.getSavepointName(), "Joe's");
        try {
            savepointTest.getSavepointId();
            error();
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        conn.rollback(savepointTest);
        conn.commit();
        ResultSet rs = stat.executeQuery("SELECT NAME FROM TEST");
        rs.next();
        String name = rs.getString(1);
        check(name, "Hallo");
        checkFalse(rs.next());
        try {
            conn.rollback(savepoint2);
            error();
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        stat.execute("DROP TABLE TEST");
        conn.setAutoCommit(true);
    }

    void testStatement() throws Exception {

        Statement stat = conn.createStatement();

        //## Java 1.4 begin ##
        check(ResultSet.HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        check(ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());
        //## Java 1.4 end ##

        // ignored
        stat.setCursorName("x");
        // fixed return value
        check(stat.getFetchDirection(), ResultSet.FETCH_FORWARD);
        // ignored
        stat.setFetchDirection(ResultSet.FETCH_REVERSE);
        // ignored
        stat.setMaxFieldSize(100);

        check(SysProperties.SERVER_RESULT_SET_FETCH_SIZE, stat.getFetchSize());
        stat.setFetchSize(10);
        check(10, stat.getFetchSize());
        stat.setFetchSize(0);
        check(SysProperties.SERVER_RESULT_SET_FETCH_SIZE, stat.getFetchSize());
        check(ResultSet.TYPE_FORWARD_ONLY, stat.getResultSetType());
        Statement stat2 = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        check(ResultSet.TYPE_SCROLL_SENSITIVE, stat2.getResultSetType());
        check(ResultSet.HOLD_CURSORS_OVER_COMMIT, stat2.getResultSetHoldability());
        check(ResultSet.CONCUR_UPDATABLE, stat2.getResultSetConcurrency());
        check(0, stat.getMaxFieldSize());
        check(!((JdbcStatement) stat2).isClosed());
        stat2.close();
        check(((JdbcStatement) stat2).isClosed());


        ResultSet rs;
        int count;
        boolean result;

        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("SELECT * FROM TEST");
        stat.execute("DROP TABLE TEST");

        conn.getTypeMap();

        // this method should not throw an exception - if not supported, this
        // calls are ignored

        if (config.jdk14) {
            check(stat.getResultSetHoldability(), ResultSet.HOLD_CURSORS_OVER_COMMIT);
        }
        check(stat.getResultSetConcurrency(), ResultSet.CONCUR_UPDATABLE);

        stat.cancel();
        stat.setQueryTimeout(10);
        check(stat.getQueryTimeout() == 10);
        stat.setQueryTimeout(0);
        check(stat.getQueryTimeout() == 0);
        // this is supposed to throw an exception
        try {
            stat.setQueryTimeout(-1);
            error("setQueryTimeout(-1) didn't throw an exception");
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        check(stat.getQueryTimeout() == 0);
        trace("executeUpdate");
        count = stat.executeUpdate("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
        check(count, 0);
        count = stat.executeUpdate("INSERT INTO TEST VALUES(1,'Hello')");
        check(count, 1);
        count = stat.executeUpdate("INSERT INTO TEST(VALUE,ID) VALUES('JDBC',2)");
        check(count, 1);
        count = stat.executeUpdate("UPDATE TEST SET VALUE='LDBC' WHERE ID=2 OR ID=1");
        check(count, 2);
        count = stat.executeUpdate("UPDATE TEST SET VALUE='\\LDBC\\' WHERE VALUE LIKE 'LDBC' ");
        check(count, 2);
        count = stat.executeUpdate("UPDATE TEST SET VALUE='LDBC' WHERE VALUE LIKE '\\\\LDBC\\\\'");
        trace("count:" + count);
        check(count, 2);
        count = stat.executeUpdate("DELETE FROM TEST WHERE ID=-1");
        check(count, 0);
        count = stat.executeUpdate("DELETE FROM TEST WHERE ID=2");
        check(count, 1);
        try {
            stat.executeUpdate("SELECT * FROM TEST");
            error("executeUpdate allowed SELECT");
        } catch (SQLException e) {
            checkNotGeneralException(e);
            trace("no error - SELECT not allowed with executeUpdate");
        }
        count = stat.executeUpdate("DROP TABLE TEST");
        check(count == 0);

        trace("execute");
        result = stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
        check(!result);
        result = stat.execute("INSERT INTO TEST VALUES(1,'Hello')");
        check(!result);
        result = stat.execute("INSERT INTO TEST(VALUE,ID) VALUES('JDBC',2)");
        check(!result);
        result = stat.execute("UPDATE TEST SET VALUE='LDBC' WHERE ID=2");
        check(!result);
        result = stat.execute("DELETE FROM TEST WHERE ID=3");
        check(!result);
        result = stat.execute("SELECT * FROM TEST");
        check(result);
        result = stat.execute("DROP TABLE TEST");
        check(!result);

        trace("executeQuery");
        try {
            stat.executeQuery("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
            error("executeQuery allowed CREATE TABLE");
        } catch (SQLException e) {
            checkNotGeneralException(e);
            trace("no error - CREATE not allowed with executeQuery");
        }
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
        try {
            stat.executeQuery("INSERT INTO TEST VALUES(1,'Hello')");
            error("executeQuery allowed INSERT");
        } catch (SQLException e) {
            checkNotGeneralException(e);
            trace("no error - INSERT not allowed with executeQuery");
        }
        try {
            stat.executeQuery("UPDATE TEST SET VALUE='LDBC' WHERE ID=2");
            error("executeQuery allowed UPDATE");
        } catch (SQLException e) {
            checkNotGeneralException(e);
            trace("no error - UPDATE not allowed with executeQuery");
        }
        try {
            stat.executeQuery("DELETE FROM TEST WHERE ID=3");
            error("executeQuery allowed DELETE");
        } catch (SQLException e) {
            checkNotGeneralException(e);
            trace("no error - DELETE not allowed with executeQuery");
        }
        stat.executeQuery("SELECT * FROM TEST");
        try {
            stat.executeQuery("DROP TABLE TEST");
            error("executeQuery allowed DROP");
        } catch (SQLException e) {
            checkNotGeneralException(e);
            trace("no error - DROP not allowed with executeQuery");
        }
        // getMoreResults
        rs = stat.executeQuery("SELECT * FROM TEST");
        checkFalse(stat.getMoreResults());
        try {
            // supposed to be closed now
            rs.next();
            error("getMoreResults didn't close this result set");
        } catch (SQLException e) {
            checkNotGeneralException(e);
            trace("no error - getMoreResults is supposed to close the result set");
        }
        check(stat.getUpdateCount() == -1);
        count = stat.executeUpdate("DELETE FROM TEST");
        checkFalse(stat.getMoreResults());
        check(stat.getUpdateCount() == -1);

        stat.execute("DROP TABLE TEST");
        stat.executeUpdate("DROP TABLE IF EXISTS TEST");

        check(stat.getWarnings() == null);
        stat.clearWarnings();
        check(stat.getWarnings() == null);
        check(conn == stat.getConnection());

        stat.close();
    }

    private void testIdentity() throws Exception {
        Statement stat = conn.createStatement();
        stat.execute("CREATE SEQUENCE SEQ");
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)");
        ResultSet rs = stat.getGeneratedKeys();
        rs.next();
        check(rs.getInt(1), 1);
        checkFalse(rs.next());
        stat.execute("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", Statement.RETURN_GENERATED_KEYS);
        rs = stat.getGeneratedKeys();
        rs.next();
        check(rs.getInt(1), 2);
        checkFalse(rs.next());
        stat.execute("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", new int[] { 1 });
        rs = stat.getGeneratedKeys();
        rs.next();
        check(rs.getInt(1), 3);
        checkFalse(rs.next());
        stat.execute("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", new String[] { "ID" });
        rs = stat.getGeneratedKeys();
        rs.next();
        check(rs.getInt(1), 4);
        checkFalse(rs.next());
        stat.executeUpdate("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", Statement.RETURN_GENERATED_KEYS);
        rs = stat.getGeneratedKeys();
        rs.next();
        check(rs.getInt(1), 5);
        checkFalse(rs.next());
        stat.executeUpdate("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", new int[] { 1 });
        rs = stat.getGeneratedKeys();
        rs.next();
        check(rs.getInt(1), 6);
        checkFalse(rs.next());
        stat.executeUpdate("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", new String[] { "ID" });
        rs = stat.getGeneratedKeys();
        rs.next();
        check(rs.getInt(1), 7);
        checkFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

}
