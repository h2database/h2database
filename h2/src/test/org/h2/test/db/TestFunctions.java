/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.h2.test.TestBase;
import org.h2.tools.SimpleResultSet;

public class TestFunctions extends TestBase {

    Statement stat;

    public void test() throws Exception {
        deleteDb("functions");
        Connection conn = getConnection("functions");
        stat = conn.createStatement();
        test("abs(null)", null);
        test("abs(1)", "1");
        test("abs(1)", "1");

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("CREATE ALIAS ADD_ROW FOR \"" + getClass().getName() + ".addRow\"");
        ResultSet rs;
        rs = stat.executeQuery("CALL ADD_ROW(1, 'Hello')");
        rs.next();
        check(rs.getInt(1), 1);
        rs = stat.executeQuery("SELECT * FROM TEST");
        rs.next();
        check(rs.getInt(1), 1);
        check(rs.getString(2), "Hello");
        checkFalse(rs.next());

        rs = stat.executeQuery("CALL ADD_ROW(2, 'World')");

        stat.execute("CREATE ALIAS SELECT_F FOR \"" + getClass().getName() + ".select\"");
        rs = stat.executeQuery("CALL SELECT_F('SELECT * FROM TEST ORDER BY ID')");
        check(rs.getMetaData().getColumnCount(), 2);
        rs.next();
        check(rs.getInt(1), 1);
        check(rs.getString(2), "Hello");
        rs.next();
        check(rs.getInt(1), 2);
        check(rs.getString(2), "World");
        checkFalse(rs.next());
        
        rs = stat.executeQuery("SELECT NAME FROM SELECT_F('SELECT * FROM TEST ORDER BY NAME') ORDER BY NAME DESC");
        check(rs.getMetaData().getColumnCount(), 1);
        rs.next();
        check(rs.getString(1), "World");
        rs.next();
        check(rs.getString(1), "Hello");
        checkFalse(rs.next());

        rs = stat.executeQuery("SELECT SELECT_F('SELECT * FROM TEST WHERE ID=' || ID) FROM TEST ORDER BY ID");
        check(rs.getMetaData().getColumnCount(), 1);
        rs.next();
        check("((1, Hello))", rs.getString(1));
        rs.next();
        check("((2, World))", rs.getString(1));
        checkFalse(rs.next());

        rs = stat.executeQuery("SELECT SELECT_F('SELECT * FROM TEST ORDER BY ID') FROM DUAL");
        check(rs.getMetaData().getColumnCount(), 1);
        rs.next();
        check("((1, Hello), (2, World))", rs.getString(1));
        checkFalse(rs.next());

        try {
            rs = stat.executeQuery("CALL SELECT_F('ERROR')");
            error("expected error");
        } catch (SQLException e) {
            check("42001", e.getSQLState());
        }

        stat.execute("CREATE ALIAS SIMPLE FOR \"" + getClass().getName() + ".simpleResultSet\"");
        rs = stat.executeQuery("CALL SIMPLE(2, 1,1,1,1,1,1,1)");
        check(rs.getMetaData().getColumnCount(), 2);
        rs.next();
        check(rs.getInt(1), 0);
        check(rs.getString(2), "Hello");
        rs.next();
        check(rs.getInt(1), 1);
        check(rs.getString(2), "World");
        checkFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM SIMPLE(1, 1,1,1,1,1,1,1)");
        check(rs.getMetaData().getColumnCount(), 2);
        rs.next();
        check(rs.getInt(1), 0);
        check(rs.getString(2), "Hello");
        checkFalse(rs.next());

        stat.execute("CREATE ALIAS ARRAY FOR \"" + getClass().getName() + ".getArray\"");
        rs = stat.executeQuery("CALL ARRAY()");
        check(rs.getMetaData().getColumnCount(), 2);
        rs.next();
        check(rs.getInt(1), 0);
        check(rs.getString(2), "Hello");
        checkFalse(rs.next());

        stat.execute("CREATE ALIAS ROOT FOR \"" + getClass().getName() + ".root\"");
        rs = stat.executeQuery("CALL ROOT(9)");
        rs.next();
        check(rs.getInt(1), 3);
        checkFalse(rs.next());

        stat.execute("CREATE ALIAS MAX_ID FOR \"" + getClass().getName() + ".selectMaxId\"");
        rs = stat.executeQuery("CALL MAX_ID()");
        rs.next();
        check(rs.getInt(1), 2);
        checkFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM MAX_ID()");
        rs.next();
        check(rs.getInt(1), 2);
        checkFalse(rs.next());

        rs = stat.executeQuery("CALL CASE WHEN -9 < 0 THEN 0 ELSE ROOT(-9) END");
        rs.next();
        check(rs.getInt(1), 0);
        checkFalse(rs.next());

        stat.execute("CREATE ALIAS blob2stream FOR \"" + getClass().getName() + ".blob2stream\"");
        stat.execute("CREATE ALIAS stream2stream FOR \"" + getClass().getName() + ".stream2stream\"");
        stat.execute("CREATE TABLE TEST_BLOB(ID INT PRIMARY KEY, VALUE BLOB)");
        stat.execute("INSERT INTO TEST_BLOB VALUES(0, null)");
        stat.execute("INSERT INTO TEST_BLOB VALUES(1, 'edd1f011edd1f011edd1f011')");
        rs = stat.executeQuery("SELECT blob2stream(VALUE) FROM TEST_BLOB");
        while(rs.next()) {
        }
        rs.close();
        rs = stat.executeQuery("SELECT stream2stream(VALUE) FROM TEST_BLOB");
        while(rs.next()) {
            // ignore
        }
        
        stat.execute("CREATE ALIAS NULL_RESULT FOR \"" + getClass().getName() + ".nullResultSet\"");
        rs = stat.executeQuery("CALL NULL_RESULT()");
        check(rs.getMetaData().getColumnCount(), 1);
        rs.next();
        check(rs.getString(1), null);
        checkFalse(rs.next());

        conn.close();
    }

    void test(String sql, String value) throws Exception {
        ResultSet rs = stat.executeQuery("CALL " + sql);
        rs.next();
        String s = rs.getString(1);
        check(value, s);
    }

    public static BufferedInputStream blob2stream(Blob value) throws SQLException {
        if(value == null) {
            return null;
        }
        BufferedInputStream bufferedInStream = new BufferedInputStream(value.getBinaryStream());
        return bufferedInStream;
    }

    public static BufferedInputStream stream2stream(InputStream value) throws SQLException {
        if(value == null) {
            return null;
        }
        BufferedInputStream bufferedInStream = new BufferedInputStream(value);
        return bufferedInStream;
    }

    public static int addRow(Connection conn, int id, String name) throws SQLException {
        conn.createStatement().execute("INSERT INTO TEST VALUES(" + id + ", '" + name + "')");
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM TEST");
        rs.next();
        int result = rs.getInt(1);
        rs.close();
        return result;
    }

    public static ResultSet select(Connection conn, String sql) throws SQLException {
    	Statement stat = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    	return stat.executeQuery(sql);
    }

    public static ResultSet selectMaxId(Connection conn) throws SQLException {
        return conn.createStatement().executeQuery("SELECT MAX(ID) FROM TEST");
    }

    public static Object[] getArray() {
        return new Object[] { new Integer(0), "Hello" };
    }
    
    public static ResultSet nullResultSet(Connection conn) throws SQLException {
        PreparedStatement statement = conn.prepareStatement("select null from system_range(1,1)");
        return statement.executeQuery();
    }

    public static ResultSet simpleResultSet(Integer count, int ip, boolean bp, float fp, double dp, long lp, byte byParam, short sp) throws SQLException {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("ID", Types.INTEGER, 10, 0);
        rs.addColumn("NAME", Types.VARCHAR, 255, 0);
        if (count == null) {
            if (ip != 0 || bp || fp != 0.0 || dp != 0.0 || sp != 0 || lp != 0 || byParam != 0) {
                throw new Error("params not 0/false");
            }
        }
        if (count != null) {
            if (ip != 1 || !bp || fp != 1.0 || dp != 1.0 || sp != 1 || lp != 1 || byParam != 1) {
                throw new Error("params not 1/true");
            }
            if (count.intValue() >= 1) {
                rs.addRow(new Object[] { new Integer(0), "Hello" });
            }
            if (count.intValue() >= 2) {
                rs.addRow(new Object[] { new Integer(1), "World" });
            }
        }
        return rs;
    }

    public static int root(int value) {
        if (value < 0) {
            TestBase.logError("function called but should not", null);
        }
        return (int) Math.sqrt(value);
    }

}
