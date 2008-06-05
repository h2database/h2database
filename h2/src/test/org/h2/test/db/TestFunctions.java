/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Properties;

import org.h2.api.AggregateFunction;
import org.h2.test.TestBase;
import org.h2.tools.SimpleResultSet;
import org.h2.util.IOUtils;

/**
 * Tests for user defined functions and aggregates.
 */
public class TestFunctions extends TestBase {

    private Statement stat;

    public void test() throws Exception {
        testAggregate();
        testFunctions();
        testFileRead();
    }

    public void testFileRead() throws Exception {
        Connection conn = getConnection("functions");
        stat = conn.createStatement();
        File f = new File(baseDir + "/test.txt");
        Properties prop = System.getProperties();
        FileOutputStream out = new FileOutputStream(f);
        prop.store(out, "");
        out.close();
        ResultSet rs = stat.executeQuery("SELECT LENGTH(FILE_READ('" + baseDir + "/test.txt')) LEN");
        rs.next();
        assertEquals(f.length(), rs.getInt(1));
        rs = stat.executeQuery("SELECT FILE_READ('" + baseDir + "/test.txt') PROP");
        rs.next();
        Properties p2 = new Properties();
        p2.load(rs.getBinaryStream(1));
        assertEquals(prop.size(), p2.size());
        rs = stat.executeQuery("SELECT FILE_READ('" + baseDir + "/test.txt', NULL) PROP");
        rs.next();
        String ps = rs.getString(1);
        FileReader r = new FileReader(f);
        String ps2 = IOUtils.readStringAndClose(r, -1);
        assertEquals(ps, ps2);
        f.delete();
        conn.close();
    }

    /**
     * This median implementation keeps all objects in memory.
     */
    public static class MedianString implements AggregateFunction {

        private ArrayList list = new ArrayList();

        public void add(Object value) {
            list.add(value.toString());
        }

        public Object getResult() {
            return list.get(list.size() / 2);
        }

        public int getType(int[] inputType) {
            return Types.VARCHAR;
        }

        public void init(Connection conn) {
            // nothing to do
        }

    }

    private void testAggregate() throws Exception {
        deleteDb("functions");
        Connection conn = getConnection("functions");
        stat = conn.createStatement();
        stat.execute("CREATE AGGREGATE MEDIAN FOR \"" + MedianString.class.getName() + "\"");
        stat.execute("CREATE AGGREGATE IF NOT EXISTS MEDIAN FOR \"" + MedianString.class.getName() + "\"");
        ResultSet rs = stat.executeQuery("SELECT MEDIAN(X) FROM SYSTEM_RANGE(1, 9)");
        rs.next();
        assertEquals("5", rs.getString(1));
        conn.close();

        if (config.memory) {
            return;
        }

        conn = getConnection("functions");
        stat = conn.createStatement();
        rs = stat.executeQuery("SELECT MEDIAN(X) FROM SYSTEM_RANGE(1, 9)");
        DatabaseMetaData meta = conn.getMetaData();
        rs = meta.getProcedures(null, null, "MEDIAN");
        assertTrue(rs.next());
        assertFalse(rs.next());
        rs = stat.executeQuery("SCRIPT");
        boolean found = false;
        while (rs.next()) {
            String sql = rs.getString(1);
            if (sql.indexOf("MEDIAN") >= 0) {
                found = true;
            }
        }
        assertTrue(found);
        stat.execute("DROP AGGREGATE MEDIAN");
        stat.execute("DROP AGGREGATE IF EXISTS MEDIAN");
        conn.close();
    }

    private void testFunctions() throws Exception {
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
        assertEquals(rs.getInt(1), 1);
        rs = stat.executeQuery("SELECT * FROM TEST");
        rs.next();
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getString(2), "Hello");
        assertFalse(rs.next());

        rs = stat.executeQuery("CALL ADD_ROW(2, 'World')");

        stat.execute("CREATE ALIAS SELECT_F FOR \"" + getClass().getName() + ".select\"");
        rs = stat.executeQuery("CALL SELECT_F('SELECT * FROM TEST ORDER BY ID')");
        assertEquals(rs.getMetaData().getColumnCount(), 2);
        rs.next();
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getString(2), "Hello");
        rs.next();
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getString(2), "World");
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT NAME FROM SELECT_F('SELECT * FROM TEST ORDER BY NAME') ORDER BY NAME DESC");
        assertEquals(rs.getMetaData().getColumnCount(), 1);
        rs.next();
        assertEquals(rs.getString(1), "World");
        rs.next();
        assertEquals(rs.getString(1), "Hello");
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT SELECT_F('SELECT * FROM TEST WHERE ID=' || ID) FROM TEST ORDER BY ID");
        assertEquals(rs.getMetaData().getColumnCount(), 1);
        rs.next();
        assertEquals("((1, Hello))", rs.getString(1));
        rs.next();
        assertEquals("((2, World))", rs.getString(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT SELECT_F('SELECT * FROM TEST ORDER BY ID') FROM DUAL");
        assertEquals(rs.getMetaData().getColumnCount(), 1);
        rs.next();
        assertEquals("((1, Hello), (2, World))", rs.getString(1));
        assertFalse(rs.next());

        try {
            rs = stat.executeQuery("CALL SELECT_F('ERROR')");
            fail();
        } catch (SQLException e) {
            assertEquals("42001", e.getSQLState());
        }

        stat.execute("CREATE ALIAS SIMPLE FOR \"" + getClass().getName() + ".simpleResultSet\"");
        rs = stat.executeQuery("CALL SIMPLE(2, 1,1,1,1,1,1,1)");
        assertEquals(rs.getMetaData().getColumnCount(), 2);
        rs.next();
        assertEquals(rs.getInt(1), 0);
        assertEquals(rs.getString(2), "Hello");
        rs.next();
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getString(2), "World");
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM SIMPLE(1, 1,1,1,1,1,1,1)");
        assertEquals(rs.getMetaData().getColumnCount(), 2);
        rs.next();
        assertEquals(rs.getInt(1), 0);
        assertEquals(rs.getString(2), "Hello");
        assertFalse(rs.next());

        stat.execute("CREATE ALIAS ARRAY FOR \"" + getClass().getName() + ".getArray\"");
        rs = stat.executeQuery("CALL ARRAY()");
        assertEquals(rs.getMetaData().getColumnCount(), 2);
        rs.next();
        assertEquals(rs.getInt(1), 0);
        assertEquals(rs.getString(2), "Hello");
        assertFalse(rs.next());

        stat.execute("CREATE ALIAS ROOT FOR \"" + getClass().getName() + ".root\"");
        rs = stat.executeQuery("CALL ROOT(9)");
        rs.next();
        assertEquals(rs.getInt(1), 3);
        assertFalse(rs.next());

        stat.execute("CREATE ALIAS MAX_ID FOR \"" + getClass().getName() + ".selectMaxId\"");
        rs = stat.executeQuery("CALL MAX_ID()");
        rs.next();
        assertEquals(rs.getInt(1), 2);
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM MAX_ID()");
        rs.next();
        assertEquals(rs.getInt(1), 2);
        assertFalse(rs.next());

        rs = stat.executeQuery("CALL CASE WHEN -9 < 0 THEN 0 ELSE ROOT(-9) END");
        rs.next();
        assertEquals(rs.getInt(1), 0);
        assertFalse(rs.next());

        stat.execute("CREATE ALIAS blob2stream FOR \"" + getClass().getName() + ".blob2stream\"");
        stat.execute("CREATE ALIAS stream2stream FOR \"" + getClass().getName() + ".stream2stream\"");
        stat.execute("CREATE TABLE TEST_BLOB(ID INT PRIMARY KEY, VALUE BLOB)");
        stat.execute("INSERT INTO TEST_BLOB VALUES(0, null)");
        stat.execute("INSERT INTO TEST_BLOB VALUES(1, 'edd1f011edd1f011edd1f011')");
        rs = stat.executeQuery("SELECT blob2stream(VALUE) FROM TEST_BLOB");
        while (rs.next()) {
            // ignore
        }
        rs.close();
        rs = stat.executeQuery("SELECT stream2stream(VALUE) FROM TEST_BLOB");
        while (rs.next()) {
            // ignore
        }

        stat.execute("CREATE ALIAS NULL_RESULT FOR \"" + getClass().getName() + ".nullResultSet\"");
        rs = stat.executeQuery("CALL NULL_RESULT()");
        assertEquals(rs.getMetaData().getColumnCount(), 1);
        rs.next();
        assertEquals(rs.getString(1), null);
        assertFalse(rs.next());

        conn.close();
    }

    void test(String sql, String value) throws Exception {
        ResultSet rs = stat.executeQuery("CALL " + sql);
        rs.next();
        String s = rs.getString(1);
        assertEquals(value, s);
    }

    public static BufferedInputStream blob2stream(Blob value) throws SQLException {
        if (value == null) {
            return null;
        }
        BufferedInputStream bufferedInStream = new BufferedInputStream(value.getBinaryStream());
        return bufferedInStream;
    }

    public static BufferedInputStream stream2stream(InputStream value) {
        if (value == null) {
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

    /**
     * Test method to create a simple result set.
     * 
     * @param count the number of rows
     * @param ip an int
     * @param bp a boolean
     * @param fp a float
     * @param dp a double
     * @param lp a long
     * @param byParam a byte
     * @param sp a short
     * @return a result set
     */
    public static ResultSet simpleResultSet(Integer count, int ip, boolean bp, float fp, double dp, long lp,
            byte byParam, short sp) throws SQLException {
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
