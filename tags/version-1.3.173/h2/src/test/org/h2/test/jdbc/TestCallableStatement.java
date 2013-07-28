/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

import org.h2.constant.ErrorCode;
import org.h2.test.TestBase;
import org.h2.tools.SimpleResultSet;

/**
 * Tests for the CallableStatement class.
 */
public class TestCallableStatement extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        deleteDb("callableStatement");
        Connection conn = getConnection("callableStatement");
        testCallWithResultSet(conn);
        testCallWithResult(conn);
        testPrepare(conn);
        conn.close();
        deleteDb("callableStatement");
    }

    private void testCallWithResultSet(Connection conn) throws SQLException {
        CallableStatement call;
        ResultSet rs;
        call = conn.prepareCall("select 10");

        call.execute();
        rs = call.getResultSet();
        rs.next();
        assertEquals(10, rs.getInt(1));

        call.executeUpdate();
        rs = call.getResultSet();
        rs.next();
        assertEquals(10, rs.getInt(1));

    }

    private void testCallWithResult(Connection conn) throws SQLException {
        CallableStatement call;
        for (String s : new String[]{"{?= call abs(?)}", " { ? = call abs(?)}", " {? = call abs(?)}"}) {
            call = conn.prepareCall(s);
            call.setInt(2, -3);
            call.registerOutParameter(1, Types.INTEGER);
            call.execute();
            assertEquals(3, call.getInt(1));
            call.executeUpdate();
            assertEquals(3, call.getInt(1));
        }
    }

    private void testPrepare(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        CallableStatement call;
        ResultSet rs;
        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR)");
        call = conn.prepareCall("INSERT INTO TEST VALUES(?, ?)");
        call.setInt(1, 1);
        call.setString(2, "Hello");
        call.execute();
        call = conn.prepareCall("SELECT * FROM TEST",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        rs = call.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        call = conn.prepareCall("SELECT * FROM TEST",
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
        rs = call.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        stat.execute("CREATE ALIAS testCall FOR \"" + getClass().getName() + ".testCall\"");
        call = conn.prepareCall("{CALL testCall(?,?,?)}");
        call.setInt("A", 100);
        call.setString(2, "abc");
        long t = System.currentTimeMillis();
        call.setTimestamp("C", new Timestamp(t));
        call.registerOutParameter(1, Types.INTEGER);
        call.registerOutParameter("B", Types.VARCHAR);
        call.executeUpdate();
        try {
            call.getTimestamp("C");
            fail("not registered out parameter accessible");
        } catch (SQLException e) {
            // expected exception
        }
        call.registerOutParameter(3, Types.TIMESTAMP);
        call.executeUpdate();
        assertEquals(t + 1, call.getTimestamp(3).getTime());
        assertEquals(200, call.getInt("A"));
        assertEquals("ABC", call.getString("B"));
        try {
            call.getString(100);
            fail("incorrect parameter index value");
        } catch (SQLException e) {
            // expected exception
        }
        try {
            call.getString(0);
            fail("incorrect parameter index value");
        } catch (SQLException e) {
            // expected exception
        }
        try {
            call.getBoolean("X");
            fail("incorrect parameter name value");
        } catch (SQLException e) {
            // expected exception
        }
        // test for exceptions after closing
        call.close();
        assertThrows(ErrorCode.OBJECT_CLOSED, call).
                executeUpdate();
        assertThrows(ErrorCode.OBJECT_CLOSED, call).
                registerOutParameter(1, Types.INTEGER);
        assertThrows(ErrorCode.OBJECT_CLOSED, call).
                getString("X");
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param conn the connection
     * @param a the value a
     * @param b the value b
     * @param c the value c
     * @return a result set
     */
    public static ResultSet testCall(Connection conn,  int a, String b, Timestamp c) throws SQLException {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("A", Types.INTEGER, 0, 0);
        rs.addColumn("B", Types.VARCHAR, 0, 0);
        rs.addColumn("C", Types.TIMESTAMP, 0, 0);
        if ("jdbc:columnlist:connection".equals(conn.getMetaData().getURL())) {
            return rs;
        }
        rs.addRow(a * 2, b.toUpperCase(), new Timestamp(c.getTime() + 1));
        return rs;
    }

}
