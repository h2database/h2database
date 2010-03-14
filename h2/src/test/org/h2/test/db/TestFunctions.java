/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigDecimal;
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
import java.util.UUID;

import org.h2.api.AggregateFunction;
import org.h2.test.TestBase;
import org.h2.tools.SimpleResultSet;
import org.h2.util.IOUtils;
import org.h2.util.New;

/**
 * Tests for user defined functions and aggregates.
 */
public class TestFunctions extends TestBase implements AggregateFunction {

    static int count;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        deleteDb("functions");
        testGreatest();
        testSource();
        testDynamicArgumentAndReturn();
        testUUID();
        testDeterministic();
        testTransactionId();
        testPrecision();
        testVarArgs();
        testAggregate();
        testFunctions();
        testFileRead();
        deleteDb("functions");
    }

    private void testGreatest() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();

        String createSQL = "CREATE TABLE testGreatest (id BIGINT);";
        stat.execute(createSQL);
        stat.execute("insert into testGreatest values (1)");

        String query = "SELECT GREATEST(id, " + ((long) Integer.MAX_VALUE) + ") FROM testGreatest";
        ResultSet rs = stat.executeQuery(query);
        rs.next();
        Object o = rs.getObject(1);
        assertEquals(Long.class.getName(), o.getClass().getName());

        String query2 = "SELECT GREATEST(id, " + ((long) Integer.MAX_VALUE + 1) + ") FROM testGreatest";
        ResultSet rs2 = stat.executeQuery(query2);
        rs2.next();
        Object o2 = rs2.getObject(1);
        assertEquals(Long.class.getName(), o2.getClass().getName());

        conn.close();
    }

    private void testSource() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        ResultSet rs;
        stat.execute("create force alias sayHi as 'String test(String name) {\n" +
                "return \"Hello \" + name;\n}'");
        rs = stat.executeQuery("call sayHi('Joe')");
        rs.next();
        assertEquals("Hello Joe", rs.getString(1));
        if (!config.memory) {
            conn.close();
            conn = getConnection("functions");
            stat = conn.createStatement();
            rs = stat.executeQuery("call sayHi('Joe')");
            rs.next();
            assertEquals("Hello Joe", rs.getString(1));
        }
        stat.execute("drop alias sayHi");
        conn.close();
    }

    private void testDynamicArgumentAndReturn() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        ResultSet rs;
        stat.execute("create alias dynamic deterministic for \"" + getClass().getName() + ".dynamic\"");
        setCount(0);
        rs = stat.executeQuery("call dynamic(('a', 1))[0]");
        rs.next();
        String a = rs.getString(1);
        assertEquals("a1", a);
        stat.execute("drop alias dynamic");
        conn.close();
    }

    private void testUUID() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("create alias xorUUID for \""+getClass().getName()+".xorUUID\"");
        setCount(0);
        rs = stat.executeQuery("call xorUUID(random_uuid(), random_uuid())");
        rs.next();
        Object o = rs.getObject(1);
        assertEquals(UUID.class.toString(), o.getClass().toString());
        stat.execute("drop alias xorUUID");

        conn.close();
    }

    private void testDeterministic() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("create alias getCount for \""+getClass().getName()+".getCount\"");
        setCount(0);
        rs = stat.executeQuery("select getCount() from system_range(1, 2)");
        rs.next();
        assertEquals(0, rs.getInt(1));
        rs.next();
        assertEquals(1, rs.getInt(1));
        stat.execute("drop alias getCount");

        stat.execute("create alias getCount deterministic for \""+getClass().getName()+".getCount\"");
        setCount(0);
        rs = stat.executeQuery("select getCount() from system_range(1, 2)");
        rs.next();
        assertEquals(0, rs.getInt(1));
        rs.next();
        assertEquals(0, rs.getInt(1));
        stat.execute("drop alias getCount");

        stat.execute("create alias reverse deterministic for \""+getClass().getName()+".reverse\"");
        rs = stat.executeQuery("select reverse(x) from system_range(700, 700)");
        rs.next();
        assertEquals("007", rs.getString(1));
        stat.execute("drop alias reverse");

        conn.close();
    }

    private void testTransactionId() throws SQLException {
        if (config.memory) {
            return;
        }
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        ResultSet rs;
        rs = stat.executeQuery("call transaction_id()");
        rs.next();
        assertTrue(rs.getString(1) == null && rs.wasNull());
        stat.execute("insert into test values(1)");
        rs = stat.executeQuery("call transaction_id()");
        rs.next();
        assertTrue(rs.getString(1) == null && rs.wasNull());
        conn.setAutoCommit(false);
        stat.execute("delete from test");
        rs = stat.executeQuery("call transaction_id()");
        rs.next();
        assertTrue(rs.getString(1) != null);
        stat.execute("drop table test");
        conn.close();
    }

    private void testPrecision() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        stat.execute("create alias no_op for \""+getClass().getName()+".noOp\"");
        PreparedStatement prep = conn.prepareStatement("select * from dual where no_op(1.6)=?");
        prep.setBigDecimal(1, new BigDecimal("1.6"));
        ResultSet rs = prep.executeQuery();
        assertTrue(rs.next());

        stat.execute("create aggregate agg_sum for \""+getClass().getName()+"\"");
        rs = stat.executeQuery("select agg_sum(1), sum(1.6) from dual");
        rs.next();
        assertEquals(1, rs.getMetaData().getScale(2));
        assertEquals(32767, rs.getMetaData().getScale(1));
        conn.close();
    }

    private void testVarArgs() throws SQLException {
//## Java 1.5 begin ##
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        stat.execute("CREATE ALIAS mean FOR \"" +
                getClass().getName() + ".mean\"");
        ResultSet rs = stat.executeQuery(
                "select mean(), mean(10), mean(10, 20), mean(10, 20, 30)");
        rs.next();
        assertEquals(1.0, rs.getDouble(1));
        assertEquals(10.0, rs.getDouble(2));
        assertEquals(15.0, rs.getDouble(3));
        assertEquals(20.0, rs.getDouble(4));

        stat.execute("CREATE ALIAS mean2 FOR \"" +
                getClass().getName() + ".mean2\"");
        rs = stat.executeQuery(
                "select mean2(), mean2(10), mean2(10, 20)");
        rs.next();
        assertEquals(Double.NaN, rs.getDouble(1));
        assertEquals(10.0, rs.getDouble(2));
        assertEquals(15.0, rs.getDouble(3));

        stat.execute("CREATE ALIAS printMean FOR \"" +
                getClass().getName() + ".printMean\"");
        rs = stat.executeQuery(
                "select printMean('A'), printMean('A', 10), " +
                "printMean('BB', 10, 20), printMean ('CCC', 10, 20, 30)");
        rs.next();
        assertEquals("A: 0", rs.getString(1));
        assertEquals("A: 10", rs.getString(2));
        assertEquals("BB: 15", rs.getString(3));
        assertEquals("CCC: 20", rs.getString(4));
        conn.close();
//## Java 1.5 end ##
    }

    private void testFileRead() throws Exception {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        String fileName = baseDir + "/test.txt";
        Properties prop = System.getProperties();
        OutputStream out = IOUtils.openFileOutputStream(fileName, false);
        prop.store(out, "");
        out.close();
        ResultSet rs = stat.executeQuery("SELECT LENGTH(FILE_READ('" + fileName + "')) LEN");
        rs.next();
        assertEquals(IOUtils.length(fileName), rs.getInt(1));
        rs = stat.executeQuery("SELECT FILE_READ('" + fileName + "') PROP");
        rs.next();
        Properties p2 = new Properties();
        p2.load(rs.getBinaryStream(1));
        assertEquals(prop.size(), p2.size());
        rs = stat.executeQuery("SELECT FILE_READ('" + fileName + "', NULL) PROP");
        rs.next();
        String ps = rs.getString(1);
        InputStreamReader r = new InputStreamReader(IOUtils.openFileInputStream(fileName));
        String ps2 = IOUtils.readStringAndClose(r, -1);
        assertEquals(ps, ps2);
        IOUtils.delete(fileName);
        conn.close();
    }

    /**
     * This median implementation keeps all objects in memory.
     */
    public static class MedianString implements AggregateFunction {

        private ArrayList<String> list = New.arrayList();

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

    private void testAggregate() throws SQLException {
        deleteDb("functions");
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
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
        stat.executeQuery("SELECT MEDIAN(X) FROM SYSTEM_RANGE(1, 9)");
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

    private void testFunctions() throws SQLException {
        deleteDb("functions");
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        assertCallResult(null, stat, "abs(null)");
        assertCallResult("1", stat, "abs(1)");
        assertCallResult("1", stat, "abs(1)");

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("CREATE ALIAS ADD_ROW FOR \"" + getClass().getName() + ".addRow\"");
        ResultSet rs;
        rs = stat.executeQuery("CALL ADD_ROW(1, 'Hello')");
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs = stat.executeQuery("SELECT * FROM TEST");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());

        rs = stat.executeQuery("CALL ADD_ROW(2, 'World')");

        stat.execute("CREATE ALIAS SELECT_F FOR \"" + getClass().getName() + ".select\"");
        rs = stat.executeQuery("CALL SELECT_F('SELECT * FROM TEST ORDER BY ID')");
        assertEquals(2, rs.getMetaData().getColumnCount());
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals("World", rs.getString(2));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT NAME FROM SELECT_F('SELECT * FROM TEST ORDER BY NAME') ORDER BY NAME DESC");
        assertEquals(1, rs.getMetaData().getColumnCount());
        rs.next();
        assertEquals("World", rs.getString(1));
        rs.next();
        assertEquals("Hello", rs.getString(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT SELECT_F('SELECT * FROM TEST WHERE ID=' || ID) FROM TEST ORDER BY ID");
        assertEquals(1, rs.getMetaData().getColumnCount());
        rs.next();
        assertEquals("((1, Hello))", rs.getString(1));
        rs.next();
        assertEquals("((2, World))", rs.getString(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT SELECT_F('SELECT * FROM TEST ORDER BY ID') FROM DUAL");
        assertEquals(1, rs.getMetaData().getColumnCount());
        rs.next();
        assertEquals("((1, Hello), (2, World))", rs.getString(1));
        assertFalse(rs.next());

        try {
            stat.executeQuery("CALL SELECT_F('ERROR')");
            fail();
        } catch (SQLException e) {
            assertEquals("42001", e.getSQLState());
        }

        stat.execute("CREATE ALIAS SIMPLE FOR \"" + getClass().getName() + ".simpleResultSet\"");
        rs = stat.executeQuery("CALL SIMPLE(2, 1,1,1,1,1,1,1)");
        assertEquals(2, rs.getMetaData().getColumnCount());
        rs.next();
        assertEquals(0, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("World", rs.getString(2));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM SIMPLE(1, 1,1,1,1,1,1,1)");
        assertEquals(2, rs.getMetaData().getColumnCount());
        rs.next();
        assertEquals(0, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());

        stat.execute("CREATE ALIAS ARRAY FOR \"" + getClass().getName() + ".getArray\"");
        rs = stat.executeQuery("CALL ARRAY()");
        assertEquals(2, rs.getMetaData().getColumnCount());
        rs.next();
        assertEquals(0, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());

        stat.execute("CREATE ALIAS ROOT FOR \"" + getClass().getName() + ".root\"");
        rs = stat.executeQuery("CALL ROOT(9)");
        rs.next();
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());

        stat.execute("CREATE ALIAS MAX_ID FOR \"" + getClass().getName() + ".selectMaxId\"");
        rs = stat.executeQuery("CALL MAX_ID()");
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM MAX_ID()");
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("CALL CASE WHEN -9 < 0 THEN 0 ELSE ROOT(-9) END");
        rs.next();
        assertEquals(0, rs.getInt(1));
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
        assertEquals(1, rs.getMetaData().getColumnCount());
        rs.next();
        assertEquals(null, rs.getString(1));
        assertFalse(rs.next());

        conn.close();
    }

    private void assertCallResult(String expected, Statement stat, String sql) throws SQLException {
        ResultSet rs = stat.executeQuery("CALL " + sql);
        rs.next();
        String s = rs.getString(1);
        assertEquals(expected, s);
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param value the blob
     * @return the input stream
     */
    public static BufferedInputStream blob2stream(Blob value) throws SQLException {
        if (value == null) {
            return null;
        }
        BufferedInputStream bufferedInStream = new BufferedInputStream(value.getBinaryStream());
        return bufferedInStream;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param value the input stream
     * @return the buffered input stream
     */
    public static BufferedInputStream stream2stream(InputStream value) {
        if (value == null) {
            return null;
        }
        BufferedInputStream bufferedInStream = new BufferedInputStream(value);
        return bufferedInStream;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param conn the connection
     * @param id the test id
     * @param name the text
     * @return the count
     */
    public static int addRow(Connection conn, int id, String name) throws SQLException {
        conn.createStatement().execute("INSERT INTO TEST VALUES(" + id + ", '" + name + "')");
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM TEST");
        rs.next();
        int result = rs.getInt(1);
        rs.close();
        return result;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param conn the connection
     * @param sql the SQL statement
     * @return the result set
     */
    public static ResultSet select(Connection conn, String sql) throws SQLException {
        Statement stat = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        return stat.executeQuery(sql);
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param conn the connection
     * @return the result set
     */
    public static ResultSet selectMaxId(Connection conn) throws SQLException {
        return conn.createStatement().executeQuery("SELECT MAX(ID) FROM TEST");
    }

    /**
     * This method is called via reflection from the database.
     *
     * @return the test array
     */
    public static Object[] getArray() {
        return new Object[] { new Integer(0), "Hello" };
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param conn the connection
     * @return the result set
     */
    public static ResultSet nullResultSet(Connection conn) throws SQLException {
        PreparedStatement statement = conn.prepareStatement("select null from system_range(1,1)");
        return statement.executeQuery();
    }

    /**
     * Test method to create a simple result set.
     *
     * @param rowCount the number of rows
     * @param ip an int
     * @param bp a boolean
     * @param fp a float
     * @param dp a double
     * @param lp a long
     * @param byParam a byte
     * @param sp a short
     * @return a result set
     */
    public static ResultSet simpleResultSet(Integer rowCount, int ip, boolean bp, float fp, double dp, long lp,
            byte byParam, short sp) {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("ID", Types.INTEGER, 10, 0);
        rs.addColumn("NAME", Types.VARCHAR, 255, 0);
        if (rowCount == null) {
            if (ip != 0 || bp || fp != 0.0 || dp != 0.0 || sp != 0 || lp != 0 || byParam != 0) {
                throw new AssertionError("params not 0/false");
            }
        }
        if (rowCount != null) {
            if (ip != 1 || !bp || fp != 1.0 || dp != 1.0 || sp != 1 || lp != 1 || byParam != 1) {
                throw new AssertionError("params not 1/true");
            }
            if (rowCount.intValue() >= 1) {
                rs.addRow(0, "Hello");
            }
            if (rowCount.intValue() >= 2) {
                rs.addRow(1, "World");
            }
        }
        return rs;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param value the value
     * @return the square root
     */
    public static int root(int value) {
        if (value < 0) {
            TestBase.logError("function called but should not", null);
        }
        return (int) Math.sqrt(value);
    }

    /**
     * This method is called via reflection from the database.
     *
     * @return 1
     */
    public static double mean() {
        return 1;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param dec the value
     * @return the value
     */
    public static BigDecimal noOp(BigDecimal dec) {
        return dec;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @return the count
     */
    public static int getCount() {
        return count++;
    }

    private static void setCount(int newCount) {
        count = newCount;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param s the string
     * @return the string, reversed
     */
    public static String reverse(String s) {
        return new StringBuilder(s).reverse().toString();
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param values the values
     * @return the mean value
     */
//## Java 1.5 begin ##
    public static double mean(double... values) {
        double sum = 0;
        for (double x : values) {
            sum += x;
        }
        return sum / values.length;
    }
//## Java 1.5 end ##

    /**
     * This method is called via reflection from the database.
     *
     * @param conn the connection
     * @param values the values
     * @return the mean value
     */
//## Java 1.5 begin ##
    public static double mean2(Connection conn, double... values) {
        conn.getClass();
        double sum = 0;
        for (double x : values) {
            sum += x;
        }
        return sum / values.length;
    }
//## Java 1.5 end ##

    /**
     * This method is called via reflection from the database.
     *
     * @param prefix the print prefix
     * @param values the values
     * @return the text
     */
//## Java 1.5 begin ##
    public static String printMean(String prefix, double... values) {
        double sum = 0;
        for (double x : values) {
            sum += x;
        }
        return prefix + ": " + (int) (sum / values.length);
    }
//## Java 1.5 end ##

    /**
     * This method is called via reflection from the database.
     *
     * @param a the first UUID
     * @param b the second UUID
     * @return a xor b
     */
    public static UUID xorUUID(UUID a, UUID b) {
        return new UUID(a.getMostSignificantBits() ^ b.getMostSignificantBits(),
                a.getLeastSignificantBits() ^ b.getLeastSignificantBits());
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param args the argument list
     * @return an array of one element
     */
    public static Object[] dynamic(Object[] args) {
        StringBuilder buff = new StringBuilder();
        for (Object a : args) {
            buff.append(a);
        }
        return new Object[] { buff.toString() };
    }

    public void add(Object value) {
        // ignore
    }

    public Object getResult() {
        return new BigDecimal("1.6");
    }

    public int getType(int[] inputTypes) {
        if (inputTypes.length != 1 || inputTypes[0] != Types.INTEGER) {
            throw new RuntimeException("unexpected data type");
        }
        return Types.DECIMAL;
    }

    public void init(Connection conn) {
        // ignore
    }

}
