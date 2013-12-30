/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;

import org.h2.constant.ErrorCode;
import org.h2.test.TestBase;
import org.h2.tools.SimpleResultSet;
import org.h2.util.Utils;

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
        testOutParameter(conn);
        testUnsupportedOperations(conn);
        testGetters(conn);
        testCallWithResultSet(conn);
        testCallWithResult(conn);
        testPrepare(conn);
        testClassLoader(conn);
        conn.close();
        deleteDb("callableStatement");
    }
    
    private void testOutParameter(Connection conn) throws SQLException {
        conn.createStatement().execute(
                "create table test(id identity) as select null");
        for (int i = 1; i < 20; i++) {
            CallableStatement cs = conn.prepareCall("{ ? = call IDENTITY()}");
            cs.registerOutParameter(1, Types.BIGINT);
            cs.execute();
            long id = cs.getLong(1);
            assertEquals(1, id);
            cs.close();
        }
        conn.createStatement().execute(
                "drop table test");
    }
    
    private void testUnsupportedOperations(Connection conn) throws SQLException {
        CallableStatement call;
        call = conn.prepareCall("select 10 as a");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).getURL(1);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).getObject(1, Collections.<String, Class<?>>emptyMap());
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).getRef(1);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).getRowId(1);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).getSQLXML(1);
        
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).getURL("a");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).getObject("a", Collections.<String, Class<?>>emptyMap());
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).getRef("a");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).getRowId("a");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).getSQLXML("a");
        
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).setURL(1, (URL) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).setRef(1, (Ref) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).setRowId(1, (RowId) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).setSQLXML(1, (SQLXML) null);
        
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).setURL("a", (URL) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).setRowId("a", (RowId) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, call).setSQLXML("a", (SQLXML) null);

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
    
    private void testGetters(Connection conn) throws SQLException {
        CallableStatement call;
        call = conn.prepareCall("{?=call ?}");
        call.setLong(2, 1);
        call.registerOutParameter(1, Types.BIGINT);
        call.execute();
        assertEquals(1, call.getLong(1));

        call.setFloat(2, 1.1f);
        call.registerOutParameter(1, Types.REAL);
        call.execute();
        assertEquals(1.1f, call.getFloat(1));

        call.setDouble(2, Math.PI);
        call.registerOutParameter(1, Types.DOUBLE);
        call.execute();
        assertEquals(Math.PI, call.getDouble(1));
        
        call.setBytes(2, new byte[11]);
        call.registerOutParameter(1, Types.BINARY);
        call.execute();
        assertEquals(11, call.getBytes(1).length);

        call.setDate(2, java.sql.Date.valueOf("2000-01-01"));
        call.registerOutParameter(1, Types.DATE);
        call.execute();
        assertEquals("2000-01-01", call.getDate(1).toString());
        
        call.setTime(2, java.sql.Time.valueOf("01:02:03"));
        call.registerOutParameter(1, Types.TIME);
        call.execute();
        assertEquals("01:02:03", call.getTime(1).toString());

        call.setTimestamp(2, java.sql.Timestamp.valueOf("2001-02-03 04:05:06.789"));
        call.registerOutParameter(1, Types.TIMESTAMP);
        call.execute();
        assertEquals("2001-02-03 04:05:06.789", call.getTimestamp(1).toString());

        call.setBoolean(2, true);
        call.registerOutParameter(1, Types.BIT);
        call.execute();
        assertEquals(true, call.getBoolean(1));

        call.setShort(2, (short) 123);
        call.registerOutParameter(1, Types.SMALLINT);
        call.execute();
        assertEquals(123, call.getShort(1));
        
        call.setBigDecimal(2, BigDecimal.TEN);
        call.registerOutParameter(1, Types.DECIMAL);
        call.execute();
        assertEquals("10", call.getBigDecimal(1).toString());
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

    private void testClassLoader(Connection conn) throws SQLException {
        Utils.ClassFactory myFactory = new TestClassFactory();
        Utils.addClassFactory(myFactory);
        try {
            Statement stat = conn.createStatement();
            stat.execute("CREATE ALIAS T_CLASSLOADER FOR \"TestClassFactory.testClassF\"");
            ResultSet rs = stat.executeQuery("SELECT T_CLASSLOADER(true)");
            assertTrue(rs.next());
            assertEquals(false, rs.getBoolean(1));
        } finally {
            Utils.removeClassFactory(myFactory);
        }
    }

    /**
     * Class factory unit test
     * @param b boolean value
     * @return !b
     */
    public static Boolean testClassF(Boolean b) {
        return !b;
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

    /**
     * A class factory used for testing.
     */
    static class TestClassFactory implements Utils.ClassFactory {

        @Override
        public boolean match(String name) {
            return name.equals("TestClassFactory");
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            return TestCallableStatement.class;
        }
    }

}
