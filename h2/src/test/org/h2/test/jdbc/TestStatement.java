/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.File;
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

    private Connection conn;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        deleteDb("statement");
        conn = getConnection("statement");
        testTraceError();
        if (config.jdk14) {
            testSavepoint();
        }
        testConnectionRollback();
        testStatement();
        if (config.jdk14) {
            testIdentityMerge();
            testIdentity();
        }
        conn.close();
        deleteDb("statement");
    }

    private void testTraceError() throws SQLException {
        if (config.memory || config.networked || config.traceLevelFile != 0) {
            return;
        }
        Statement stat = conn.createStatement();
        File trace = new File(baseDir + "/statement.trace.db");
        stat.execute("DROP TABLE TEST IF EXISTS");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY)");
        stat.execute("INSERT INTO TEST VALUES(1)");
        long start = trace.length();
        try {
            stat.execute("ERROR");
        } catch (SQLException e) {
            // ignore
        }
        long error = trace.length();
        assertSmaller(start, error);
        start = error;
        try {
            stat.execute("INSERT INTO TEST VALUES(1)");
        } catch (SQLException e) {
            // ignore
        }
        error = trace.length();
        assertEquals(start, error);
        stat.execute("DROP TABLE TEST IF EXISTS");
    }

    private void testConnectionRollback() throws SQLException {
        Statement stat = conn.createStatement();
        conn.setAutoCommit(false);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        conn.rollback();
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
        conn.setAutoCommit(true);
    }

    private void testSavepoint() throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        conn.setAutoCommit(false);
        stat.execute("INSERT INTO TEST VALUES(0, 'Hi')");
        Savepoint savepoint1 = conn.setSavepoint();
        int id1 = savepoint1.getSavepointId();
        try {
            savepoint1.getSavepointName();
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }
        stat.execute("DELETE FROM TEST");
        conn.rollback(savepoint1);
        stat.execute("UPDATE TEST SET NAME='Hello'");
        Savepoint savepoint2a = conn.setSavepoint();
        Savepoint savepoint2 = conn.setSavepoint();
        conn.releaseSavepoint(savepoint2a);
        try {
            savepoint2a.getSavepointId();
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }
        int id2 = savepoint2.getSavepointId();
        assertTrue(id1 != id2);
        stat.execute("UPDATE TEST SET NAME='Hallo' WHERE NAME='Hello'");
        Savepoint savepointTest = conn.setSavepoint("Joe's");
        stat.execute("DELETE FROM TEST");
        assertEquals(savepointTest.getSavepointName(), "Joe's");
        try {
            savepointTest.getSavepointId();
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }
        conn.rollback(savepointTest);
        conn.commit();
        ResultSet rs = stat.executeQuery("SELECT NAME FROM TEST");
        rs.next();
        String name = rs.getString(1);
        assertEquals(name, "Hallo");
        assertFalse(rs.next());
        try {
            conn.rollback(savepoint2);
            fail();
        } catch (SQLException e) {
            assertKnownException(e);
        }
        stat.execute("DROP TABLE TEST");
        conn.setAutoCommit(true);
    }

    private void testStatement() throws SQLException {

        Statement stat = conn.createStatement();

        //## Java 1.4 begin ##
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());
        //## Java 1.4 end ##

        // ignored
        stat.setCursorName("x");
        // fixed return value
        assertEquals(stat.getFetchDirection(), ResultSet.FETCH_FORWARD);
        // ignored
        stat.setFetchDirection(ResultSet.FETCH_REVERSE);
        // ignored
        stat.setMaxFieldSize(100);

        assertEquals(SysProperties.SERVER_RESULT_SET_FETCH_SIZE, stat.getFetchSize());
        stat.setFetchSize(10);
        assertEquals(10, stat.getFetchSize());
        stat.setFetchSize(0);
        assertEquals(SysProperties.SERVER_RESULT_SET_FETCH_SIZE, stat.getFetchSize());
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, stat.getResultSetType());
        Statement stat2 = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, stat2.getResultSetType());
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stat2.getResultSetHoldability());
        assertEquals(ResultSet.CONCUR_READ_ONLY, stat2.getResultSetConcurrency());
        assertEquals(0, stat.getMaxFieldSize());
        assertTrue(!((JdbcStatement) stat2).isClosed());
        stat2.close();
        assertTrue(((JdbcStatement) stat2).isClosed());


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
            assertEquals(stat.getResultSetHoldability(), ResultSet.HOLD_CURSORS_OVER_COMMIT);
        }
        assertEquals(stat.getResultSetConcurrency(), ResultSet.CONCUR_READ_ONLY);

        stat.cancel();
        stat.setQueryTimeout(10);
        assertTrue(stat.getQueryTimeout() == 10);
        stat.setQueryTimeout(0);
        assertTrue(stat.getQueryTimeout() == 0);
        // this is supposed to throw an exception
        try {
            stat.setQueryTimeout(-1);
            fail("setQueryTimeout(-1) didn't throw an exception");
        } catch (SQLException e) {
            assertKnownException(e);
        }
        assertTrue(stat.getQueryTimeout() == 0);
        trace("executeUpdate");
        count = stat.executeUpdate("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
        assertEquals(count, 0);
        count = stat.executeUpdate("INSERT INTO TEST VALUES(1,'Hello')");
        assertEquals(count, 1);
        count = stat.executeUpdate("INSERT INTO TEST(VALUE,ID) VALUES('JDBC',2)");
        assertEquals(count, 1);
        count = stat.executeUpdate("UPDATE TEST SET VALUE='LDBC' WHERE ID=2 OR ID=1");
        assertEquals(count, 2);
        count = stat.executeUpdate("UPDATE TEST SET VALUE='\\LDBC\\' WHERE VALUE LIKE 'LDBC' ");
        assertEquals(count, 2);
        count = stat.executeUpdate("UPDATE TEST SET VALUE='LDBC' WHERE VALUE LIKE '\\\\LDBC\\\\'");
        trace("count:" + count);
        assertEquals(count, 2);
        count = stat.executeUpdate("DELETE FROM TEST WHERE ID=-1");
        assertEquals(count, 0);
        count = stat.executeUpdate("DELETE FROM TEST WHERE ID=2");
        assertEquals(count, 1);
        try {
            stat.executeUpdate("SELECT * FROM TEST");
            fail("executeUpdate allowed SELECT");
        } catch (SQLException e) {
            assertKnownException(e);
            trace("no error - SELECT not allowed with executeUpdate");
        }
        count = stat.executeUpdate("DROP TABLE TEST");
        assertTrue(count == 0);

        trace("execute");
        result = stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
        assertTrue(!result);
        result = stat.execute("INSERT INTO TEST VALUES(1,'Hello')");
        assertTrue(!result);
        result = stat.execute("INSERT INTO TEST(VALUE,ID) VALUES('JDBC',2)");
        assertTrue(!result);
        result = stat.execute("UPDATE TEST SET VALUE='LDBC' WHERE ID=2");
        assertTrue(!result);
        result = stat.execute("DELETE FROM TEST WHERE ID=3");
        assertTrue(!result);
        result = stat.execute("SELECT * FROM TEST");
        assertTrue(result);
        result = stat.execute("DROP TABLE TEST");
        assertTrue(!result);

        trace("executeQuery");
        try {
            stat.executeQuery("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
            fail("executeQuery allowed CREATE TABLE");
        } catch (SQLException e) {
            assertKnownException(e);
            trace("no error - CREATE not allowed with executeQuery");
        }
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
        try {
            stat.executeQuery("INSERT INTO TEST VALUES(1,'Hello')");
            fail("executeQuery allowed INSERT");
        } catch (SQLException e) {
            assertKnownException(e);
            trace("no error - INSERT not allowed with executeQuery");
        }
        try {
            stat.executeQuery("UPDATE TEST SET VALUE='LDBC' WHERE ID=2");
            fail("executeQuery allowed UPDATE");
        } catch (SQLException e) {
            assertKnownException(e);
            trace("no error - UPDATE not allowed with executeQuery");
        }
        try {
            stat.executeQuery("DELETE FROM TEST WHERE ID=3");
            fail("executeQuery allowed DELETE");
        } catch (SQLException e) {
            assertKnownException(e);
            trace("no error - DELETE not allowed with executeQuery");
        }
        stat.executeQuery("SELECT * FROM TEST");
        try {
            stat.executeQuery("DROP TABLE TEST");
            fail("executeQuery allowed DROP");
        } catch (SQLException e) {
            assertKnownException(e);
            trace("no error - DROP not allowed with executeQuery");
        }
        // getMoreResults
        rs = stat.executeQuery("SELECT * FROM TEST");
        assertFalse(stat.getMoreResults());
        try {
            // supposed to be closed now
            rs.next();
            fail("getMoreResults didn't close this result set");
        } catch (SQLException e) {
            assertKnownException(e);
            trace("no error - getMoreResults is supposed to close the result set");
        }
        assertTrue(stat.getUpdateCount() == -1);
        count = stat.executeUpdate("DELETE FROM TEST");
        assertFalse(stat.getMoreResults());
        assertTrue(stat.getUpdateCount() == -1);

        stat.execute("DROP TABLE TEST");
        stat.executeUpdate("DROP TABLE IF EXISTS TEST");

        assertTrue(stat.getWarnings() == null);
        stat.clearWarnings();
        assertTrue(stat.getWarnings() == null);
        assertTrue(conn == stat.getConnection());

        stat.close();
    }

    private void testIdentityMerge() throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test1");
        stat.execute("create table test1(id identity, x int)");
        stat.execute("drop table if exists test2");
        stat.execute("create table test2(id identity, x int)");
        stat.execute("merge into test1(x) key(x) values(5)");
        ResultSet keys;
        keys = stat.getGeneratedKeys();
        keys.next();
        assertEquals(1, keys.getInt(1));
        stat.execute("insert into test2(x) values(10), (11), (12)");
        stat.execute("merge into test1(x) key(x) values(5)");
        keys = stat.getGeneratedKeys();
        keys.next();
        assertEquals(0, keys.getInt(1));
        stat.execute("merge into test1(x) key(x) values(6)");
        keys = stat.getGeneratedKeys();
        keys.next();
        assertEquals(2, keys.getInt(1));
        stat.execute("drop table test1, test2");
    }

    private void testIdentity() throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE SEQUENCE SEQ");
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)");
        ResultSet rs = stat.getGeneratedKeys();
        rs.next();
        assertEquals(rs.getInt(1), 1);
        assertFalse(rs.next());
        stat.execute("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", Statement.RETURN_GENERATED_KEYS);
        rs = stat.getGeneratedKeys();
        rs.next();
        assertEquals(rs.getInt(1), 2);
        assertFalse(rs.next());
        stat.execute("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", new int[] { 1 });
        rs = stat.getGeneratedKeys();
        rs.next();
        assertEquals(rs.getInt(1), 3);
        assertFalse(rs.next());
        stat.execute("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", new String[] { "ID" });
        rs = stat.getGeneratedKeys();
        rs.next();
        assertEquals(rs.getInt(1), 4);
        assertFalse(rs.next());
        stat.executeUpdate("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", Statement.RETURN_GENERATED_KEYS);
        rs = stat.getGeneratedKeys();
        rs.next();
        assertEquals(rs.getInt(1), 5);
        assertFalse(rs.next());
        stat.executeUpdate("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", new int[] { 1 });
        rs = stat.getGeneratedKeys();
        rs.next();
        assertEquals(rs.getInt(1), 6);
        assertFalse(rs.next());
        stat.executeUpdate("INSERT INTO TEST VALUES(NEXT VALUE FOR SEQ)", new String[] { "ID" });
        rs = stat.getGeneratedKeys();
        rs.next();
        assertEquals(rs.getInt(1), 7);
        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

}
