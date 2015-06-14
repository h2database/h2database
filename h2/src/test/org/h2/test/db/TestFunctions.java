/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Currency;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

import org.h2.api.Aggregate;
import org.h2.api.AggregateFunction;
import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.tools.SimpleResultSet;
import org.h2.util.IOUtils;
import org.h2.util.New;
import org.h2.value.Value;

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

    @Override
    public void test() throws Exception {
        deleteDb("functions");
        testDataType();
        testVersion();
        testFunctionTable();
        testFunctionTableVarArgs();
        testArrayParameters();
        testDefaultConnection();
        testFunctionInSchema();
        testGreatest();
        testSource();
        testDynamicArgumentAndReturn();
        testUUID();
        testWhiteSpacesInParameters();
        testSchemaSearchPath();
        testDeterministic();
        testTransactionId();
        testPrecision();
        testMathFunctions();
        testVarArgs();
        testAggregate();
        testAggregateType();
        testFunctions();
        testFileRead();
        testValue();
        testNvl2();
        testConcatWs();
        testTruncate();
        testOraHash();
        testToCharFromDateTime();
        testToCharFromNumber();
        testToCharFromText();
        testTranslate();
        testGenerateSeries();

        deleteDb("functions");
    }

    private void testDataType() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        assertEquals(Types.DOUBLE, stat.executeQuery(
                "select radians(x) from dual").
                getMetaData().getColumnType(1));
        assertEquals(Types.DOUBLE, stat.executeQuery(
                "select power(10, 2*x) from dual").
                getMetaData().getColumnType(1));
        stat.close();
        conn.close();
    }

    private void testVersion() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        String query = "select h2version()";
        ResultSet rs = stat.executeQuery(query);
        assertTrue(rs.next());
        String version = rs.getString(1);
        assertEquals(Constants.getVersion(), version);
        assertFalse(rs.next());
        rs.close();
        stat.close();
        conn.close();
    }

    private void testFunctionTable() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        stat.execute("create alias simple_function_table for \"" +
                TestFunctions.class.getName() + ".simpleFunctionTable\"");
        stat.execute("select * from simple_function_table() " +
                "where a>0 and b in ('x', 'y')");
        conn.close();
    }

    private void testFunctionTableVarArgs() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        stat.execute("create alias varargs_function_table for \"" + TestFunctions.class.getName()
                + ".varArgsFunctionTable\"");
        ResultSet rs = stat.executeQuery("select * from varargs_function_table(1,2,3,5,8,13)");
        for (int i : new int[] { 1, 2, 3, 5, 8, 13 }) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
        }
        assertFalse(rs.next());
        conn.close();
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param conn the connection
     * @return a result set
     */
    public static ResultSet simpleFunctionTable(Connection conn) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("A", Types.INTEGER, 0, 0);
        result.addColumn("B", Types.CHAR, 0, 0);
        result.addRow(42, 'X');
        return result;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param values the value array
     * @return a result set
     */
    public static ResultSet varArgsFunctionTable(int... values) throws SQLException {
        if (values.length != 6) {
            throw new SQLException("Unexpected argument count");
        }
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("A", Types.INTEGER, 0, 0);
        for (int value : values) {
            result.addRow(value);
        }
        return result;
    }

    private void testNvl2() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();

        String createSQL = "CREATE TABLE testNvl2(id BIGINT, txt1 " +
                "varchar, txt2 varchar, num number(9, 0));";
        stat.execute(createSQL);
        stat.execute("insert into testNvl2(id, txt1, txt2, num) " +
                "values(1, 'test1', 'test2', null)");
        stat.execute("insert into testNvl2(id, txt1, txt2, num) " +
                "values(2, null, 'test4', null)");
        stat.execute("insert into testNvl2(id, txt1, txt2, num) " +
                "values(3, 'test5', null, null)");
        stat.execute("insert into testNvl2(id, txt1, txt2, num) " +
                "values(4, null, null, null)");
        stat.execute("insert into testNvl2(id, txt1, txt2, num) " +
                "values(5, '2', null, 1)");
        stat.execute("insert into testNvl2(id, txt1, txt2, num) " +
                "values(6, '2', null, null)");
        stat.execute("insert into testNvl2(id, txt1, txt2, num) " +
                "values(7, 'test2', null, null)");

        String query = "SELECT NVL2(txt1, txt1, txt2), txt1 " +
                "FROM testNvl2 order by id asc";
        ResultSet rs = stat.executeQuery(query);
        rs.next();
        String actual = rs.getString(1);
        assertEquals("test1", actual);
        rs.next();
        actual = rs.getString(1);
        assertEquals("test4", actual);
        rs.next();
        actual = rs.getString(1);
        assertEquals("test5", actual);
        rs.next();
        actual = rs.getString(1);
        assertEquals(null, actual);
        assertEquals(rs.getMetaData().getColumnType(2),
                rs.getMetaData().getColumnType(1));
        rs.close();

        rs = stat.executeQuery("SELECT NVL2(num, num, txt1), num " +
                "FROM testNvl2 where id in(5, 6) order by id asc");
        rs.next();
        assertEquals(rs.getMetaData().getColumnType(2),
                rs.getMetaData().getColumnType(1));

        assertThrows(ErrorCode.DATA_CONVERSION_ERROR_1, stat).
                executeQuery("SELECT NVL2(num, num, txt1), num " +
                        "FROM testNvl2 where id = 7 order by id asc");

        // nvl2 should return expr2's datatype, if expr2 is character data.
        rs = stat.executeQuery("SELECT NVL2(1, 'test', 123), 'test' FROM dual");
        rs.next();
        actual = rs.getString(1);
        assertEquals("test", actual);
        assertEquals(rs.getMetaData().getColumnType(2),
                rs.getMetaData().getColumnType(1));

        conn.close();
    }

    private void testConcatWs() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();

        String createSQL = "CREATE TABLE testConcat(id BIGINT, txt1 " +
                "varchar, txt2 varchar, txt3 varchar);";
        stat.execute(createSQL);
        stat.execute("insert into testConcat(id, txt1, txt2, txt3) " +
                "values(1, 'test1', 'test2', 'test3')");
        stat.execute("insert into testConcat(id, txt1, txt2, txt3) " +
                "values(2, 'test1', 'test2', null)");
        stat.execute("insert into testConcat(id, txt1, txt2, txt3) " +
                "values(3, 'test1', null, null)");
        stat.execute("insert into testConcat(id, txt1, txt2, txt3) " +
                "values(4, null, 'test2', null)");
        stat.execute("insert into testConcat(id, txt1, txt2, txt3) " +
                "values(5, null, null, null)");

        String query = "SELECT concat_ws('_',txt1, txt2, txt3), txt1 " +
                "FROM testConcat order by id asc";
        ResultSet rs = stat.executeQuery(query);
        rs.next();
        String actual = rs.getString(1);
        assertEquals("test1_test2_test3", actual);
        rs.next();
        actual = rs.getString(1);
        assertEquals("test1_test2", actual);
        rs.next();
        actual = rs.getString(1);
        assertEquals("test1", actual);
        rs.next();
        actual = rs.getString(1);
        assertEquals("test2", actual);
        rs.next();
        actual = rs.getString(1);
        assertEquals("", actual);
        rs.close();

        rs = stat.executeQuery("select concat_ws(null,null,null)");
        rs.next();
        assertNull(rs.getObject(1));

        stat.execute("drop table testConcat");
        conn.close();
    }

    private void testValue() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        ResultSet rs;
        stat.execute("create alias TO_CHAR_2 for \"" +
                getClass().getName() + ".toChar\"");
        rs = stat.executeQuery(
                "call TO_CHAR_2(TIMESTAMP '2001-02-03 04:05:06', 'format')");
        rs.next();
        assertEquals("2001-02-03 04:05:06.0", rs.getString(1));
        stat.execute("drop alias TO_CHAR_2");
        conn.close();
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param args the argument list
     * @return the value
     */
    public static Value toChar(Value... args) {
        if (args.length == 0) {
            return null;
        }
        return args[0].convertTo(Value.STRING);
    }

    private void testDefaultConnection() throws SQLException {
        Connection conn = getConnection("functions;DEFAULT_CONNECTION=TRUE");
        Statement stat = conn.createStatement();
        stat.execute("create alias test for \""+
                TestFunctions.class.getName()+".testDefaultConn\"");
        stat.execute("call test()");
        stat.execute("drop alias test");
        conn.close();
    }

    /**
     * This method is called via reflection from the database.
     */
    public static void testDefaultConn() throws SQLException {
        DriverManager.getConnection("jdbc:default:connection");
    }

    private void testFunctionInSchema() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();

        stat.execute("create schema schema2");
        stat.execute("create alias schema2.func as 'int x() { return 1; }'");
        stat.execute("create view test as select schema2.func()");
        ResultSet rs;
        rs = stat.executeQuery("select * from information_schema.views");
        rs.next();
        assertTrue(rs.getString("VIEW_DEFINITION").contains("SCHEMA2.FUNC"));

        stat.execute("drop view test");
        stat.execute("drop schema schema2");

        conn.close();
    }

    private void testGreatest() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();

        String createSQL = "CREATE TABLE testGreatest (id BIGINT);";
        stat.execute(createSQL);
        stat.execute("insert into testGreatest values (1)");

        String query = "SELECT GREATEST(id, " +
                ((long) Integer.MAX_VALUE) + ") FROM testGreatest";
        ResultSet rs = stat.executeQuery(query);
        rs.next();
        Object o = rs.getObject(1);
        assertEquals(Long.class.getName(), o.getClass().getName());

        String query2 = "SELECT GREATEST(id, " +
                ((long) Integer.MAX_VALUE + 1) + ") FROM testGreatest";
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
        rs = stat.executeQuery("SELECT ALIAS_NAME " +
                "FROM INFORMATION_SCHEMA.FUNCTION_ALIASES");
        rs.next();
        assertEquals("SAY" + "HI", rs.getString(1));
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
        stat.execute("create alias dynamic deterministic for \"" +
                getClass().getName() + ".dynamic\"");
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

        stat.execute("create alias xorUUID for \""+
                getClass().getName()+".xorUUID\"");
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

        stat.execute("create alias getCount for \""+
                getClass().getName()+".getCount\"");
        setCount(0);
        rs = stat.executeQuery("select getCount() from system_range(1, 2)");
        rs.next();
        assertEquals(0, rs.getInt(1));
        rs.next();
        assertEquals(1, rs.getInt(1));
        stat.execute("drop alias getCount");

        stat.execute("create alias getCount deterministic for \""+
                getClass().getName()+".getCount\"");
        setCount(0);
        rs = stat.executeQuery("select getCount() from system_range(1, 2)");
        rs.next();
        assertEquals(0, rs.getInt(1));
        rs.next();
        assertEquals(0, rs.getInt(1));
        stat.execute("drop alias getCount");
        rs = stat.executeQuery("SELECT * FROM " +
                "INFORMATION_SCHEMA.FUNCTION_ALIASES " +
                "WHERE UPPER(ALIAS_NAME) = 'GET' || 'COUNT'");
        assertFalse(rs.next());
        stat.execute("create alias reverse deterministic for \""+
                getClass().getName()+".reverse\"");
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
        PreparedStatement prep = conn.prepareStatement(
                "select * from dual where no_op(1.6)=?");
        prep.setBigDecimal(1, new BigDecimal("1.6"));
        ResultSet rs = prep.executeQuery();
        assertTrue(rs.next());

        stat.execute("create aggregate agg_sum for \""+getClass().getName()+"\"");
        rs = stat.executeQuery("select agg_sum(1), sum(1.6) from dual");
        rs.next();
        assertEquals(1, rs.getMetaData().getScale(2));
        assertEquals(32767, rs.getMetaData().getScale(1));
        stat.executeQuery("select * from information_schema.function_aliases");
        conn.close();
    }

    private void testMathFunctions() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("CALL SINH(50)");
        assertTrue(rs.next());
        assertEquals(Math.sinh(50), rs.getDouble(1));
        rs = stat.executeQuery("CALL COSH(50)");
        assertTrue(rs.next());
        assertEquals(Math.cosh(50), rs.getDouble(1));
        rs = stat.executeQuery("CALL TANH(50)");
        assertTrue(rs.next());
        assertEquals(Math.tanh(50), rs.getDouble(1));
        conn.close();
    }

    private void testVarArgs() throws SQLException {
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

        DatabaseMetaData meta = conn.getMetaData();
        rs = meta.getProcedureColumns(null, null, "MEAN2", null);
        assertTrue(rs.next());
        assertEquals("P0", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("FUNCTIONS", rs.getString("PROCEDURE_CAT"));
        assertEquals("PUBLIC", rs.getString("PROCEDURE_SCHEM"));
        assertEquals("MEAN2", rs.getString("PROCEDURE_NAME"));
        assertEquals("P2", rs.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.procedureColumnIn,
                rs.getInt("COLUMN_TYPE"));
        assertEquals("OTHER", rs.getString("TYPE_NAME"));
        assertEquals(Integer.MAX_VALUE, rs.getInt("PRECISION"));
        assertEquals(Integer.MAX_VALUE, rs.getInt("LENGTH"));
        assertEquals(0, rs.getInt("SCALE"));
        assertEquals(DatabaseMetaData.columnNullable,
                rs.getInt("NULLABLE"));
        assertEquals("", rs.getString("REMARKS"));
        assertEquals(null, rs.getString("COLUMN_DEF"));
        assertEquals(0, rs.getInt("SQL_DATA_TYPE"));
        assertEquals(0, rs.getInt("SQL_DATETIME_SUB"));
        assertEquals(0, rs.getInt("CHAR_OCTET_LENGTH"));
        assertEquals(1, rs.getInt("ORDINAL_POSITION"));
        assertEquals("YES", rs.getString("IS_NULLABLE"));
        assertEquals("MEAN2", rs.getString("SPECIFIC_NAME"));
        assertFalse(rs.next());

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
    }

    private void testFileRead() throws Exception {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        String fileName = getBaseDir() + "/test.txt";
        Properties prop = System.getProperties();
        OutputStream out = FileUtils.newOutputStream(fileName, false);
        prop.store(out, "");
        out.close();
        ResultSet rs = stat.executeQuery("SELECT LENGTH(FILE_READ('" +
                fileName + "')) LEN");
        rs.next();
        assertEquals(FileUtils.size(fileName), rs.getInt(1));
        rs = stat.executeQuery("SELECT FILE_READ('" +
                fileName + "') PROP");
        rs.next();
        Properties p2 = new Properties();
        p2.load(rs.getBinaryStream(1));
        assertEquals(prop.size(), p2.size());
        rs = stat.executeQuery("SELECT FILE_READ('" +
                fileName + "', NULL) PROP");
        rs.next();
        String ps = rs.getString(1);
        InputStreamReader r = new InputStreamReader(FileUtils.newInputStream(fileName));
        String ps2 = IOUtils.readStringAndClose(r, -1);
        assertEquals(ps, ps2);
        conn.close();
        FileUtils.delete(fileName);
    }

    /**
     * This median implementation keeps all objects in memory.
     */
    public static class MedianString implements AggregateFunction {

        private final ArrayList<String> list = New.arrayList();

        @Override
        public void add(Object value) {
            list.add(value.toString());
        }

        @Override
        public Object getResult() {
            return list.get(list.size() / 2);
        }

        @Override
        public int getType(int[] inputType) {
            return Types.VARCHAR;
        }

        @Override
        public void init(Connection conn) {
            // nothing to do
        }

    }

    /**
     * This median implementation keeps all objects in memory.
     */
    public static class MedianStringType implements Aggregate {

        private final ArrayList<String> list = New.arrayList();

        @Override
        public void add(Object value) {
            list.add(value.toString());
        }

        @Override
        public Object getResult() {
            return list.get(list.size() / 2);
        }

        @Override
        public int getInternalType(int[] inputTypes) throws SQLException {
            return Value.STRING;
        }

        @Override
        public void init(Connection conn) {
            // nothing to do
        }

    }

    private void testAggregateType() throws SQLException {
        deleteDb("functions");
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        stat.execute("CREATE AGGREGATE MEDIAN FOR \"" +
                MedianStringType.class.getName() + "\"");
        stat.execute("CREATE AGGREGATE IF NOT EXISTS MEDIAN FOR \"" +
                MedianStringType.class.getName() + "\"");
        ResultSet rs = stat.executeQuery(
                "SELECT MEDIAN(X) FROM SYSTEM_RANGE(1, 9)");
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
            if (sql.contains("MEDIAN")) {
                found = true;
            }
        }
        assertTrue(found);
        stat.execute("DROP AGGREGATE MEDIAN");
        stat.execute("DROP AGGREGATE IF EXISTS MEDIAN");
        conn.close();
    }

    private void testAggregate() throws SQLException {
        deleteDb("functions");
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        stat.execute("CREATE AGGREGATE MEDIAN FOR \"" +
                MedianString.class.getName() + "\"");
        stat.execute("CREATE AGGREGATE IF NOT EXISTS MEDIAN FOR \"" +
                MedianString.class.getName() + "\"");
        ResultSet rs = stat.executeQuery(
                "SELECT MEDIAN(X) FROM SYSTEM_RANGE(1, 9)");
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
            if (sql.contains("MEDIAN")) {
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
        stat.execute("CREATE ALIAS ADD_ROW FOR \"" +
                getClass().getName() + ".addRow\"");
        ResultSet rs;
        rs = stat.executeQuery("CALL ADD_ROW(1, 'Hello')");
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs = stat.executeQuery("SELECT * FROM TEST");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());

        DatabaseMetaData meta = conn.getMetaData();
        rs = meta.getProcedureColumns(null, null, "ADD_ROW", null);
        assertTrue(rs.next());
        assertEquals("P0", rs.getString("COLUMN_NAME"));
        assertTrue(rs.next());
        assertEquals("FUNCTIONS", rs.getString("PROCEDURE_CAT"));
        assertEquals("PUBLIC", rs.getString("PROCEDURE_SCHEM"));
        assertEquals("ADD_ROW", rs.getString("PROCEDURE_NAME"));
        assertEquals("P2", rs.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.procedureColumnIn,
                rs.getInt("COLUMN_TYPE"));
        assertEquals("INTEGER", rs.getString("TYPE_NAME"));
        assertEquals(10, rs.getInt("PRECISION"));
        assertEquals(10, rs.getInt("LENGTH"));
        assertEquals(0, rs.getInt("SCALE"));
        assertEquals(DatabaseMetaData.columnNoNulls, rs.getInt("NULLABLE"));
        assertEquals("", rs.getString("REMARKS"));
        assertEquals(null, rs.getString("COLUMN_DEF"));
        assertEquals(0, rs.getInt("SQL_DATA_TYPE"));
        assertEquals(0, rs.getInt("SQL_DATETIME_SUB"));
        assertEquals(0, rs.getInt("CHAR_OCTET_LENGTH"));
        assertEquals(1, rs.getInt("ORDINAL_POSITION"));
        assertEquals("YES", rs.getString("IS_NULLABLE"));
        assertEquals("ADD_ROW", rs.getString("SPECIFIC_NAME"));
        assertTrue(rs.next());
        assertEquals("P3", rs.getString("COLUMN_NAME"));
        assertEquals("VARCHAR", rs.getString("TYPE_NAME"));
        assertFalse(rs.next());

        stat.executeQuery("CALL ADD_ROW(2, 'World')");

        stat.execute("CREATE ALIAS SELECT_F FOR \"" +
                getClass().getName() + ".select\"");
        rs = stat.executeQuery("CALL SELECT_F('SELECT * " +
                "FROM TEST ORDER BY ID')");
        assertEquals(2, rs.getMetaData().getColumnCount());
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals("World", rs.getString(2));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT NAME FROM SELECT_F('SELECT * " +
                "FROM TEST ORDER BY NAME') ORDER BY NAME DESC");
        assertEquals(1, rs.getMetaData().getColumnCount());
        rs.next();
        assertEquals("World", rs.getString(1));
        rs.next();
        assertEquals("Hello", rs.getString(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT SELECT_F('SELECT * " +
                "FROM TEST WHERE ID=' || ID) FROM TEST ORDER BY ID");
        assertEquals(1, rs.getMetaData().getColumnCount());
        rs.next();
        assertEquals("((1, Hello))", rs.getString(1));
        rs.next();
        assertEquals("((2, World))", rs.getString(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT SELECT_F('SELECT * " +
                "FROM TEST ORDER BY ID') FROM DUAL");
        assertEquals(1, rs.getMetaData().getColumnCount());
        rs.next();
        assertEquals("((1, Hello), (2, World))", rs.getString(1));
        assertFalse(rs.next());
        assertThrows(ErrorCode.SYNTAX_ERROR_2, stat).
                executeQuery("CALL SELECT_F('ERROR')");
        stat.execute("CREATE ALIAS SIMPLE FOR \"" +
                getClass().getName() + ".simpleResultSet\"");
        rs = stat.executeQuery("CALL SIMPLE(2, 1, 1, 1, 1, 1, 1, 1)");
        assertEquals(2, rs.getMetaData().getColumnCount());
        rs.next();
        assertEquals(0, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("World", rs.getString(2));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM SIMPLE(1, 1, 1, 1, 1, 1, 1, 1)");
        assertEquals(2, rs.getMetaData().getColumnCount());
        rs.next();
        assertEquals(0, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());

        stat.execute("CREATE ALIAS ARRAY FOR \"" +
                getClass().getName() + ".getArray\"");
        rs = stat.executeQuery("CALL ARRAY()");
        assertEquals(1, rs.getMetaData().getColumnCount());
        rs.next();
        Array a = rs.getArray(1);
        Object[] array = (Object[]) a.getArray();
        assertEquals(2, array.length);
        assertEquals(0, ((Integer) array[0]).intValue());
        assertEquals("Hello", (String) array[1]);
        assertThrows(ErrorCode.INVALID_VALUE_2, a).getArray(1, -1);
        assertThrows(ErrorCode.INVALID_VALUE_2, a).getArray(1, 3);
        assertEquals(0, ((Object[]) a.getArray(1, 0)).length);
        assertEquals(0, ((Object[]) a.getArray(2, 0)).length);
        assertThrows(ErrorCode.INVALID_VALUE_2, a).getArray(0, 0);
        assertThrows(ErrorCode.INVALID_VALUE_2, a).getArray(3, 0);
        HashMap<String, Class<?>> map = New.hashMap();
        assertEquals(0, ((Object[]) a.getArray(1, 0, map)).length);
        assertEquals(2, ((Object[]) a.getArray(map)).length);
        assertEquals(2, ((Object[]) a.getArray(null)).length);
        map.put("x", Object.class);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, a).getArray(1, 0, map);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, a).getArray(map);

        ResultSet rs2;
        rs2 = a.getResultSet();
        rs2.next();
        assertEquals(1, rs2.getInt(1));
        assertEquals(0, rs2.getInt(2));
        rs2.next();
        assertEquals(2, rs2.getInt(1));
        assertEquals("Hello", rs2.getString(2));
        assertFalse(rs.next());

        map.clear();
        rs2 = a.getResultSet(map);
        rs2.next();
        assertEquals(1, rs2.getInt(1));
        assertEquals(0, rs2.getInt(2));
        rs2.next();
        assertEquals(2, rs2.getInt(1));
        assertEquals("Hello", rs2.getString(2));
        assertFalse(rs.next());

        rs2 = a.getResultSet(2, 1);
        rs2.next();
        assertEquals(2, rs2.getInt(1));
        assertEquals("Hello", rs2.getString(2));
        assertFalse(rs.next());

        rs2 = a.getResultSet(1, 1, map);
        rs2.next();
        assertEquals(1, rs2.getInt(1));
        assertEquals(0, rs2.getInt(2));
        assertFalse(rs.next());

        map.put("x", Object.class);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, a).getResultSet(map);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, a).getResultSet(0, 1, map);

        a.free();
        assertThrows(ErrorCode.OBJECT_CLOSED, a).getArray();
        assertThrows(ErrorCode.OBJECT_CLOSED, a).getResultSet();

        stat.execute("CREATE ALIAS ROOT FOR \"" + getClass().getName() + ".root\"");
        rs = stat.executeQuery("CALL ROOT(9)");
        rs.next();
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());

        stat.execute("CREATE ALIAS MAX_ID FOR \"" +
                getClass().getName() + ".selectMaxId\"");
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

        stat.execute("CREATE ALIAS blob FOR \"" + getClass().getName() + ".blob\"");
        rs = stat.executeQuery("SELECT blob(CAST('0102' AS BLOB)) FROM DUAL");
        while (rs.next()) {
            // ignore
        }
        rs.close();

        stat.execute("CREATE ALIAS clob FOR \"" + getClass().getName() + ".clob\"");
        rs = stat.executeQuery("SELECT clob(CAST('Hello' AS CLOB)) FROM DUAL");
        while (rs.next()) {
            // ignore
        }
        rs.close();

        stat.execute("create alias sql as " +
                "'ResultSet sql(Connection conn, String sql) " +
                "throws SQLException { return conn.createStatement().executeQuery(sql); }'");
        rs = stat.executeQuery("select * from sql('select cast(''Hello'' as clob)')");
        assertTrue(rs.next());
        assertEquals("Hello", rs.getString(1));

        rs = stat.executeQuery("select * from sql('select cast(''4869'' as blob)')");
        assertTrue(rs.next());
        assertEquals("Hi", new String(rs.getBytes(1)));

        rs = stat.executeQuery("select sql('select 1 a, ''Hello'' b')");
        assertTrue(rs.next());
        rs2 = (ResultSet) rs.getObject(1);
        rs2.next();
        assertEquals(1, rs2.getInt(1));
        assertEquals("Hello", rs2.getString(2));
        ResultSetMetaData meta2 = rs2.getMetaData();
        assertEquals(Types.INTEGER, meta2.getColumnType(1));
        assertEquals("INTEGER", meta2.getColumnTypeName(1));
        assertEquals("java.lang.Integer", meta2.getColumnClassName(1));
        assertEquals(Types.VARCHAR, meta2.getColumnType(2));
        assertEquals("VARCHAR", meta2.getColumnTypeName(2));
        assertEquals("java.lang.String", meta2.getColumnClassName(2));

        stat.execute("CREATE ALIAS blob2stream FOR \"" +
                getClass().getName() + ".blob2stream\"");
        stat.execute("CREATE ALIAS stream2stream FOR \"" +
                getClass().getName() + ".stream2stream\"");
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

        stat.execute("CREATE ALIAS NULL_RESULT FOR \"" +
                getClass().getName() + ".nullResultSet\"");
        rs = stat.executeQuery("CALL NULL_RESULT()");
        assertEquals(1, rs.getMetaData().getColumnCount());
        rs.next();
        assertEquals(null, rs.getString(1));
        assertFalse(rs.next());

        rs = meta.getProcedures(null, null, "NULL_RESULT");
        rs.next();
        assertEquals("FUNCTIONS", rs.getString("PROCEDURE_CAT"));
        assertEquals("PUBLIC", rs.getString("PROCEDURE_SCHEM"));
        assertEquals("NULL_RESULT", rs.getString("PROCEDURE_NAME"));
        assertEquals(0, rs.getInt("NUM_INPUT_PARAMS"));
        assertEquals(0, rs.getInt("NUM_OUTPUT_PARAMS"));
        assertEquals(0, rs.getInt("NUM_RESULT_SETS"));
        assertEquals("", rs.getString("REMARKS"));
        assertEquals(DatabaseMetaData.procedureReturnsResult,
                rs.getInt("PROCEDURE_TYPE"));
        assertEquals("NULL_RESULT", rs.getString("SPECIFIC_NAME"));

        rs = meta.getProcedureColumns(null, null, "NULL_RESULT", null);
        assertTrue(rs.next());
        assertEquals("P0", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());

        stat.execute("CREATE ALIAS RESULT_WITH_NULL FOR \"" +
        getClass().getName() + ".resultSetWithNull\"");
        rs = stat.executeQuery("CALL RESULT_WITH_NULL()");
        assertEquals(1, rs.getMetaData().getColumnCount());
        rs.next();
        assertEquals(null, rs.getString(1));
        assertFalse(rs.next());

        conn.close();
    }

    private void testWhiteSpacesInParameters() throws SQLException {
        deleteDb("functions");
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        // with white space
        stat.execute("CREATE ALIAS PARSE_INT2 FOR " +
                "\"java.lang.Integer.parseInt(java.lang.String, int)\"");
        ResultSet rs;
        rs = stat.executeQuery("CALL PARSE_INT2('473', 10)");
        rs.next();
        assertEquals(473, rs.getInt(1));
        stat.execute("DROP ALIAS PARSE_INT2");
        // without white space
        stat.execute("CREATE ALIAS PARSE_INT2 FOR " +
                "\"java.lang.Integer.parseInt(java.lang.String,int)\"");
        stat.execute("DROP ALIAS PARSE_INT2");
        conn.close();
    }

    private void testSchemaSearchPath() throws SQLException {
        deleteDb("functions");
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        ResultSet rs;
        stat.execute("CREATE SCHEMA TEST");
        stat.execute("SET SCHEMA TEST");
        stat.execute("CREATE ALIAS PARSE_INT2 FOR " +
                "\"java.lang.Integer.parseInt(java.lang.String, int)\";");
        rs = stat.executeQuery("SELECT ALIAS_NAME FROM " +
                "INFORMATION_SCHEMA.FUNCTION_ALIASES WHERE ALIAS_SCHEMA ='TEST'");
        rs.next();
        assertEquals("PARSE_INT2", rs.getString(1));
        stat.execute("DROP ALIAS PARSE_INT2");

        stat.execute("SET SCHEMA PUBLIC");
        stat.execute("CREATE ALIAS TEST.PARSE_INT2 FOR " +
                "\"java.lang.Integer.parseInt(java.lang.String, int)\";");
        stat.execute("SET SCHEMA_SEARCH_PATH PUBLIC, TEST");

        rs = stat.executeQuery("CALL PARSE_INT2('-FF', 16)");
        rs.next();
        assertEquals(-255, rs.getInt(1));
        rs = stat.executeQuery("SELECT ALIAS_NAME FROM " +
                "INFORMATION_SCHEMA.FUNCTION_ALIASES WHERE ALIAS_SCHEMA ='TEST'");
        rs.next();
        assertEquals("PARSE_INT2", rs.getString(1));
        rs = stat.executeQuery("CALL TEST.PARSE_INT2('-2147483648', 10)");
        rs.next();
        assertEquals(-2147483648, rs.getInt(1));
        rs = stat.executeQuery("CALL FUNCTIONS.TEST.PARSE_INT2('-2147483648', 10)");
        rs.next();
        assertEquals(-2147483648, rs.getInt(1));
        conn.close();
    }

    private void testArrayParameters() throws SQLException {
        deleteDb("functions");
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        ResultSet rs;
        stat.execute("create alias array_test AS "
                + "$$ Integer[] array_test(Integer[] in_array) "
                + "{ return in_array; } $$;");

        PreparedStatement stmt = conn.prepareStatement(
                "select array_test(?) from dual");
        stmt.setObject(1, new Integer[] { 1, 2 });
        rs = stmt.executeQuery();
        rs.next();
        assertEquals(Integer[].class.getName(), rs.getObject(1).getClass()
                .getName());

        CallableStatement call = conn.prepareCall("{ ? = call array_test(?) }");
        call.setObject(2, new Integer[] { 2, 1 });
        call.registerOutParameter(1, Types.ARRAY);
        call.execute();
        assertEquals(Integer[].class.getName(), call.getArray(1).getArray()
                .getClass().getName());
        assertEquals(new Integer[] { 2, 1 }, (Integer[]) call.getObject(1));

        stat.execute("drop alias array_test");

        conn.close();
    }

    private void testTruncate() throws SQLException {
        deleteDb("functions");
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();

        ResultSet rs = stat.executeQuery("SELECT TRUNCATE(1.234, 2) FROM dual");
        rs.next();
        assertEquals(1.23d, rs.getDouble(1));

        rs = stat.executeQuery(
                "SELECT CURRENT_TIMESTAMP(), " +
                "TRUNCATE(CURRENT_TIMESTAMP()) FROM dual");
        rs.next();
        Calendar c = Calendar.getInstance();
        c.setTime(rs.getTimestamp(1));
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        java.util.Date nowDate = c.getTime();
        assertEquals(nowDate, rs.getTimestamp(2));

        try {
            rs = stat.executeQuery("SELECT TRUNCATE('bad', 1) FROM dual");
            fail("expected exception");
        } catch (SQLException ex) {
            // expected
        }

        // check for passing wrong data type
        try {
            rs = stat.executeQuery("SELECT TRUNCATE('bad') FROM dual");
            fail("expected exception");
        } catch (SQLException ex) {
            // expected
        }

        // check for too many parameters
        try {
            rs = stat.executeQuery("SELECT TRUNCATE(1,2,3) FROM dual");
            fail("expected exception");
        } catch (SQLException ex) {
            // expected
        }

        conn.close();
    }

    private void testTranslate() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();

        String createSQL = "CREATE TABLE testTranslate(id BIGINT, " +
                "txt1 varchar);";
        stat.execute(createSQL);
        stat.execute("insert into testTranslate(id, txt1) " +
                "values(1, 'test1')");
        stat.execute("insert into testTranslate(id, txt1) " +
                "values(2, null)");
        stat.execute("insert into testTranslate(id, txt1) " +
                "values(3, '')");
        stat.execute("insert into testTranslate(id, txt1) " +
                "values(4, 'caps')");

        String query = "SELECT translate(txt1, 'p', 'r') " +
                "FROM testTranslate order by id asc";
        ResultSet rs = stat.executeQuery(query);
        rs.next();
        String actual = rs.getString(1);
        assertEquals("test1", actual);
        rs.next();
        actual = rs.getString(1);
        assertNull(actual);
        rs.next();
        actual = rs.getString(1);
        assertEquals("", actual);
        rs.next();
        actual = rs.getString(1);
        assertEquals("cars", actual);
        rs.close();

        rs = stat.executeQuery("select translate(null,null,null)");
        rs.next();
        assertNull(rs.getObject(1));

        stat.execute("drop table testTranslate");
        conn.close();
    }

    private void testOraHash() throws SQLException {
        deleteDb("functions");
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        String testStr = "foo";
        assertResult(String.valueOf("foo".hashCode()), stat, String.format("SELECT ORA_HASH('%s') FROM DUAL", testStr));
        assertResult(String.valueOf("foo".hashCode()), stat,
                String.format("SELECT ORA_HASH('%s', 0) FROM DUAL", testStr));
        assertResult(String.valueOf("foo".hashCode()), stat,
                String.format("SELECT ORA_HASH('%s', 0, 0) FROM DUAL", testStr));
    }

    private void testToCharFromDateTime() throws SQLException {
        deleteDb("functions");
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();

        TimeZone tz = TimeZone.getDefault();
        boolean daylight = tz.inDaylightTime(new Date());
        String tzShortName = tz.getDisplayName(daylight, TimeZone.SHORT);
        String tzLongName = tz.getID();

        stat.executeUpdate("CREATE TABLE T (X TIMESTAMP(6))");
        stat.executeUpdate("INSERT INTO T VALUES " +
                "(TIMESTAMP '1979-11-12 08:12:34.560')");
        stat.executeUpdate("CREATE TABLE U (X TIMESTAMP(6))");
        stat.executeUpdate("INSERT INTO U VALUES " +
                "(TIMESTAMP '-100-01-15 14:04:02.120')");

        assertResult("1979-11-12 08:12:34.56", stat, "SELECT X FROM T");
        assertResult("-100-01-15 14:04:02.12", stat, "SELECT X FROM U");
        String expected = String.format("%tb",
                Timestamp.valueOf("1979-11-12 08:12:34.560")).toUpperCase();
        assertResult("12-" + expected + "-79 08.12.34.560000 AM", stat,
                "SELECT TO_CHAR(X) FROM T");
        assertResult("- / , . ; : text - /", stat,
                "SELECT TO_CHAR(X, '- / , . ; : \"text\" - /') FROM T");
        assertResult("1979-11-12", stat,
                "SELECT TO_CHAR(X, 'YYYY-MM-DD') FROM T");
        assertResult("1979/11/12", stat,
                "SELECT TO_CHAR(X, 'YYYY/MM/DD') FROM T");
        assertResult("1979,11,12", stat,
                "SELECT TO_CHAR(X, 'YYYY,MM,DD') FROM T");
        assertResult("1979.11.12", stat,
                "SELECT TO_CHAR(X, 'YYYY.MM.DD') FROM T");
        assertResult("1979;11;12", stat,
                "SELECT TO_CHAR(X, 'YYYY;MM;DD') FROM T");
        assertResult("1979:11:12", stat,
                "SELECT TO_CHAR(X, 'YYYY:MM:DD') FROM T");
        assertResult("year 1979!", stat,
                "SELECT TO_CHAR(X, '\"year \"YYYY\"!\"') FROM T");
        assertResult("1979 AD", stat,
                "SELECT TO_CHAR(X, 'YYYY AD') FROM T");
        assertResult("1979 A.D.", stat,
                "SELECT TO_CHAR(X, 'YYYY A.D.') FROM T");
        assertResult("0100 B.C.", stat,
                "SELECT TO_CHAR(X, 'YYYY A.D.') FROM U");
        assertResult("1979 AD", stat,
                "SELECT TO_CHAR(X, 'YYYY BC') FROM T");
        assertResult("100 BC", stat,
                "SELECT TO_CHAR(X, 'YYY BC') FROM U");
        assertResult("00 BC", stat,
                "SELECT TO_CHAR(X, 'YY BC') FROM U");
        assertResult("0 BC", stat,
                "SELECT TO_CHAR(X, 'Y BC') FROM U");
        assertResult("1979 A.D.", stat, "SELECT TO_CHAR(X, 'YYYY B.C.') FROM T");
        assertResult("08:12 AM", stat, "SELECT TO_CHAR(X, 'HH:MI AM') FROM T");
        assertResult("08:12 A.M.", stat, "SELECT TO_CHAR(X, 'HH:MI A.M.') FROM T");
        assertResult("02:04 P.M.", stat, "SELECT TO_CHAR(X, 'HH:MI A.M.') FROM U");
        assertResult("08:12 AM", stat, "SELECT TO_CHAR(X, 'HH:MI PM') FROM T");
        assertResult("02:04 PM", stat, "SELECT TO_CHAR(X, 'HH:MI PM') FROM U");
        assertResult("08:12 A.M.", stat, "SELECT TO_CHAR(X, 'HH:MI P.M.') FROM T");
        assertResult("A.M.", stat, "SELECT TO_CHAR(X, 'P.M.') FROM T");
        assertResult("a.m.", stat, "SELECT TO_CHAR(X, 'p.M.') FROM T");
        assertResult("a.m.", stat, "SELECT TO_CHAR(X, 'p.m.') FROM T");
        assertResult("AM", stat, "SELECT TO_CHAR(X, 'PM') FROM T");
        assertResult("Am", stat, "SELECT TO_CHAR(X, 'Pm') FROM T");
        assertResult("am", stat, "SELECT TO_CHAR(X, 'pM') FROM T");
        assertResult("am", stat, "SELECT TO_CHAR(X, 'pm') FROM T");
        assertResult("2", stat, "SELECT TO_CHAR(X, 'D') FROM T");
        assertResult("2", stat, "SELECT TO_CHAR(X, 'd') FROM T");
        expected = String.format("%tA",
                Timestamp.valueOf("1979-11-12 08:12:34.560"));
        expected = expected.substring(0, 1).toUpperCase() + expected.substring(1);
        String spaces = "         ";
        String first9 = (expected + spaces).substring(0, 9);
        assertResult(first9.toUpperCase(),
                stat, "SELECT TO_CHAR(X, 'DAY') FROM T");
        assertResult(first9,
                stat, "SELECT TO_CHAR(X, 'Day') FROM T");
        assertResult(first9.toLowerCase(),
                stat, "SELECT TO_CHAR(X, 'day') FROM T");
        assertResult(first9.toLowerCase(),
                stat, "SELECT TO_CHAR(X, 'dAY') FROM T");
        assertResult(expected,
                stat, "SELECT TO_CHAR(X, 'fmDay') FROM T");
        assertResult("12", stat, "SELECT TO_CHAR(X, 'DD') FROM T");
        assertResult("316", stat, "SELECT TO_CHAR(X, 'DDD') FROM T");
        assertResult("316", stat, "SELECT TO_CHAR(X, 'DdD') FROM T");
        assertResult("316", stat, "SELECT TO_CHAR(X, 'dDD') FROM T");
        assertResult("316", stat, "SELECT TO_CHAR(X, 'ddd') FROM T");
        expected = String.format("%1$tA, %1$tB %1$te, %1$tY",
                Timestamp.valueOf("1979-11-12 08:12:34.560"));
        assertResult(expected, stat,
                "SELECT TO_CHAR(X, 'DL') FROM T");
        // assertResult("Monday, November 12, 1979", stat,
        //        "SELECT TO_CHAR(X, 'DL', 'NLS_DATE_LANGUAGE = English') FROM T");
        assertResult("11/12/1979", stat, "SELECT TO_CHAR(X, 'DS') FROM T");
        assertResult("11/12/1979", stat, "SELECT TO_CHAR(X, 'Ds') FROM T");
        assertResult("11/12/1979", stat, "SELECT TO_CHAR(X, 'dS') FROM T");
        assertResult("11/12/1979", stat, "SELECT TO_CHAR(X, 'ds') FROM T");
        expected = String.format("%1$ta",
                Timestamp.valueOf("1979-11-12 08:12:34.560"));
        assertResult(expected.toUpperCase(), stat, "SELECT TO_CHAR(X, 'DY') FROM T");
        assertResult(expected, stat, "SELECT TO_CHAR(X, 'Dy') FROM T");
        assertResult(expected.toLowerCase(), stat, "SELECT TO_CHAR(X, 'dy') FROM T");
        assertResult(expected.toLowerCase(), stat, "SELECT TO_CHAR(X, 'dY') FROM T");
        assertResult("08:12:34.560000", stat,
                "SELECT TO_CHAR(X, 'HH:MI:SS.FF') FROM T");
        assertResult("08:12:34.5", stat,
                "SELECT TO_CHAR(X, 'HH:MI:SS.FF1') FROM T");
        assertResult("08:12:34.56", stat,
                "SELECT TO_CHAR(X, 'HH:MI:SS.FF2') FROM T");
        assertResult("08:12:34.560", stat,
                "SELECT TO_CHAR(X, 'HH:MI:SS.FF3') FROM T");
        assertResult("08:12:34.5600", stat,
                "SELECT TO_CHAR(X, 'HH:MI:SS.FF4') FROM T");
        assertResult("08:12:34.56000", stat,
                "SELECT TO_CHAR(X, 'HH:MI:SS.FF5') FROM T");
        assertResult("08:12:34.560000", stat,
                "SELECT TO_CHAR(X, 'HH:MI:SS.FF6') FROM T");
        assertResult("08:12:34.5600000", stat,
                "SELECT TO_CHAR(X, 'HH:MI:SS.FF7') FROM T");
        assertResult("08:12:34.56000000", stat,
                "SELECT TO_CHAR(X, 'HH:MI:SS.FF8') FROM T");
        assertResult("08:12:34.560000000", stat,
                "SELECT TO_CHAR(X, 'HH:MI:SS.FF9') FROM T");
        assertResult("08:12:34.560000000", stat,
                "SELECT TO_CHAR(X, 'HH:MI:SS.ff9') FROM T");
        assertResult("08:12:34.560000000", stat,
                "SELECT TO_CHAR(X, 'HH:MI:SS.fF9') FROM T");
        assertResult("08:12", stat, "SELECT TO_CHAR(X, 'HH:MI') FROM T");
        assertResult("08:12", stat, "SELECT TO_CHAR(X, 'HH12:MI') FROM T");
        assertResult("08:12", stat, "SELECT TO_CHAR(X, 'HH24:MI') FROM T");
        assertResult("46", stat, "SELECT TO_CHAR(X, 'IW') FROM T");
        assertResult("46", stat, "SELECT TO_CHAR(X, 'WW') FROM T");
        assertResult("2", stat, "SELECT TO_CHAR(X, 'W') FROM T");
        assertResult("9", stat, "SELECT TO_CHAR(X, 'I') FROM T");
        assertResult("79", stat, "SELECT TO_CHAR(X, 'IY') FROM T");
        assertResult("979", stat, "SELECT TO_CHAR(X, 'IYY') FROM T");
        assertResult("1979", stat, "SELECT TO_CHAR(X, 'IYYY') FROM T");
        assertResult("2444190", stat, "SELECT TO_CHAR(X, 'J') FROM T");
        assertResult("12", stat, "SELECT TO_CHAR(X, 'MI') FROM T");
        assertResult("11", stat, "SELECT TO_CHAR(X, 'MM') FROM T");
        assertResult("11", stat, "SELECT TO_CHAR(X, 'Mm') FROM T");
        assertResult("11", stat, "SELECT TO_CHAR(X, 'mM') FROM T");
        assertResult("11", stat, "SELECT TO_CHAR(X, 'mm') FROM T");
        expected = String.format("%1$tb",
                Timestamp.valueOf("1979-11-12 08:12:34.560"));
        expected = expected.substring(0, 1).toUpperCase() + expected.substring(1);
        assertResult(expected.toUpperCase(), stat,
                "SELECT TO_CHAR(X, 'MON') FROM T");
        assertResult(expected, stat,
                "SELECT TO_CHAR(X, 'Mon') FROM T");
        assertResult(expected.toLowerCase(), stat,
                "SELECT TO_CHAR(X, 'mon') FROM T");
        expected = String.format("%1$tB",
                Timestamp.valueOf("1979-11-12 08:12:34.560"));
        expected = (expected + "        ").substring(0, 9);
        assertResult(expected.toUpperCase(), stat,
                "SELECT TO_CHAR(X, 'MONTH') FROM T");
        assertResult(expected, stat,
                "SELECT TO_CHAR(X, 'Month') FROM T");
        assertResult(expected.toLowerCase(), stat,
                "SELECT TO_CHAR(X, 'month') FROM T");
        assertResult(expected.trim(), stat,
                "SELECT TO_CHAR(X, 'fmMonth') FROM T");
        assertResult("4", stat, "SELECT TO_CHAR(X, 'Q') FROM T");
        assertResult("XI", stat, "SELECT TO_CHAR(X, 'RM') FROM T");
        assertResult("xi", stat, "SELECT TO_CHAR(X, 'rm') FROM T");
        assertResult("Xi", stat, "SELECT TO_CHAR(X, 'Rm') FROM T");
        assertResult("79", stat, "SELECT TO_CHAR(X, 'RR') FROM T");
        assertResult("1979", stat, "SELECT TO_CHAR(X, 'RRRR') FROM T");
        assertResult("34", stat, "SELECT TO_CHAR(X, 'SS') FROM T");
        assertResult("29554", stat, "SELECT TO_CHAR(X, 'SSSSS') FROM T");
        assertResult("8:12:34 AM", stat, "SELECT TO_CHAR(X, 'TS') FROM T");
        assertResult(tzLongName, stat, "SELECT TO_CHAR(X, 'TZR') FROM T");
        assertResult(tzShortName, stat, "SELECT TO_CHAR(X, 'TZD') FROM T");
        expected = String.format("%f", 1.1).substring(1, 2);
        assertResult(expected, stat, "SELECT TO_CHAR(X, 'X') FROM T");
        expected = String.format("%,d", 1979);
        assertResult(expected, stat, "SELECT TO_CHAR(X, 'Y,YYY') FROM T");
        assertResult("1979", stat, "SELECT TO_CHAR(X, 'YYYY') FROM T");
        assertResult("1979", stat, "SELECT TO_CHAR(X, 'SYYYY') FROM T");
        assertResult("-0100", stat, "SELECT TO_CHAR(X, 'SYYYY') FROM U");
        assertResult("979", stat, "SELECT TO_CHAR(X, 'YYY') FROM T");
        assertResult("79", stat, "SELECT TO_CHAR(X, 'YY') FROM T");
        assertResult("9", stat, "SELECT TO_CHAR(X, 'Y') FROM T");
        assertResult("7979", stat, "SELECT TO_CHAR(X, 'yyfxyy') FROM T");
        assertThrows(ErrorCode.INVALID_TO_CHAR_FORMAT, stat,
                "SELECT TO_CHAR(X, 'A') FROM T");

        // check a bug we had when the month or day of the month is 1 digit
        stat.executeUpdate("TRUNCATE TABLE T");
        stat.executeUpdate("INSERT INTO T VALUES (TIMESTAMP '1985-01-01 08:12:34.560')");
        assertResult("19850101", stat, "SELECT TO_CHAR(X, 'YYYYMMDD') FROM T");

        conn.close();
    }

    private void testToCharFromNumber() throws SQLException {
        deleteDb("functions");
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();

        Currency currency = Currency.getInstance(Locale.getDefault());
        String cc = currency.getCurrencyCode();
        String cs = currency.getSymbol();

        assertResult(".45", stat,
                "SELECT TO_CHAR(0.45) FROM DUAL");
        assertResult("12923", stat,
                "SELECT TO_CHAR(12923) FROM DUAL");
        assertResult(" 12923.00", stat,
                "SELECT TO_CHAR(12923, '99999.99', 'NLS_CURRENCY = BTC') FROM DUAL");
        assertResult("12923.", stat,
                "SELECT TO_CHAR(12923, 'FM99999.99', 'NLS_CURRENCY = BTC') FROM DUAL");
        assertResult("######", stat,
                "SELECT TO_CHAR(12345, '9,999') FROM DUAL");
        assertResult("######", stat,
                "SELECT TO_CHAR(1234567, '9,999') FROM DUAL");
        assertResult(" 12,345", stat,
                "SELECT TO_CHAR(12345, '99,999') FROM DUAL");
        assertResult(" 123,45", stat,
                "SELECT TO_CHAR(12345, '999,99') FROM DUAL");
        assertResult("######", stat,
                "SELECT TO_CHAR(12345, '9.999') FROM DUAL");
        assertResult("#######", stat,
                "SELECT TO_CHAR(12345, '99.999') FROM DUAL");
        assertResult("########", stat,
                "SELECT TO_CHAR(12345, '999.999') FROM DUAL");
        assertResult("#########", stat,
                "SELECT TO_CHAR(12345, '9999.999') FROM DUAL");
        assertResult(" 12345.000", stat,
                "SELECT TO_CHAR(12345, '99999.999') FROM DUAL");
        assertResult("###", stat,
                "SELECT TO_CHAR(12345, '$9') FROM DUAL");
        assertResult("#####", stat,
                "SELECT TO_CHAR(12345, '$999') FROM DUAL");
        assertResult("######", stat,
                "SELECT TO_CHAR(12345, '$9999') FROM DUAL");
        assertResult("    " + cs + "12345", stat,
                "SELECT TO_CHAR(12345, '$99999999') FROM DUAL");
        assertResult("     " + cs + "12,345.35", stat,
                "SELECT TO_CHAR(12345.345, '$99,999,999.99') FROM DUAL");
        assertResult("     " + cs + "12,345", stat,
                "SELECT TO_CHAR(12345.345, '$99g999g999') FROM DUAL");
        assertResult("     12,345.35", stat,
                "SELECT TO_CHAR(12345.345, '99,999,999.99') FROM DUAL");
        assertResult("12,345.35", stat,
                "SELECT TO_CHAR(12345.345, 'FM99,999,999.99') FROM DUAL");
        assertResult(" 00,012,345.35", stat,
                "SELECT TO_CHAR(12345.345, '00,000,000.00') FROM DUAL");
        assertResult("00,012,345.35", stat,
                "SELECT TO_CHAR(12345.345, 'FM00,000,000.00') FROM DUAL");
        assertResult("###", stat,
                "SELECT TO_CHAR(12345, '09') FROM DUAL");
        assertResult("#####", stat,
                "SELECT TO_CHAR(12345, '0999') FROM DUAL");
        assertResult(" 00012345", stat,
                "SELECT TO_CHAR(12345, '09999999') FROM DUAL");
        assertResult(" 0000012345", stat,
                "SELECT TO_CHAR(12345, '0009999999') FROM DUAL");
        assertResult("###", stat,
                "SELECT TO_CHAR(12345, '90') FROM DUAL");
        assertResult("#####", stat,
                "SELECT TO_CHAR(12345, '9990') FROM DUAL");
        assertResult("    12345", stat,
                "SELECT TO_CHAR(12345, '99999990') FROM DUAL");
        assertResult("      12345", stat,
                "SELECT TO_CHAR(12345, '9999999000') FROM DUAL");
        assertResult("      12345", stat,
                "SELECT TO_CHAR(12345, '9999999990') FROM DUAL");
        assertResult("12345", stat,
                "SELECT TO_CHAR(12345, 'FM9999999990') FROM DUAL");
        assertResult("   12345.2300", stat,
                "SELECT TO_CHAR(12345.23, '9999999.9000') FROM DUAL");
        assertResult("   12345", stat,
                "SELECT TO_CHAR(12345, '9999999') FROM DUAL");
        assertResult("  12345", stat,
                "SELECT TO_CHAR(12345, '999999') FROM DUAL");
        assertResult(" 12345", stat,
                "SELECT TO_CHAR(12345, '99999') FROM DUAL");
        assertResult(" 12345", stat,
                "SELECT TO_CHAR(12345, '00000') FROM DUAL");
        assertResult("#####", stat,
                "SELECT TO_CHAR(12345, '9999') FROM DUAL");
        assertResult("#####", stat,
                "SELECT TO_CHAR(12345, '0000') FROM DUAL");
        assertResult("   -12345", stat,
                "SELECT TO_CHAR(-12345, '99999999') FROM DUAL");
        assertResult("  -12345", stat,
                "SELECT TO_CHAR(-12345, '9999999') FROM DUAL");
        assertResult(" -12345", stat,
                "SELECT TO_CHAR(-12345, '999999') FROM DUAL");
        assertResult("-12345", stat,
                "SELECT TO_CHAR(-12345, '99999') FROM DUAL");
        assertResult("#####", stat,
                "SELECT TO_CHAR(-12345, '9999') FROM DUAL");
        assertResult("####", stat,
                "SELECT TO_CHAR(-12345, '999') FROM DUAL");
        assertResult("       0", stat,
                "SELECT TO_CHAR(0, '9999999') FROM DUAL");
        assertResult(" 00.30", stat,
                "SELECT TO_CHAR(0.3, '00.99') FROM DUAL");
        assertResult("00.3", stat,
                "SELECT TO_CHAR(0.3, 'FM00.99') FROM DUAL");
        assertResult(" 00.30", stat,
                "SELECT TO_CHAR(0.3, '00.00') FROM DUAL");
        assertResult("   .30000", stat,
                "SELECT TO_CHAR(0.3, '99.00000') FROM DUAL");
        assertResult(".30000", stat,
                "SELECT TO_CHAR(0.3, 'FM99.00000') FROM DUAL");
        assertResult(" 00.30", stat,
                "SELECT TO_CHAR(0.3, 'B00.99') FROM DUAL");
        assertResult("   .30", stat,
                "SELECT TO_CHAR(0.3, 'B99.99') FROM DUAL");
        assertResult("   .30", stat,
                "SELECT TO_CHAR(0.3, '99.99') FROM DUAL");
        assertResult(".3", stat,
                "SELECT TO_CHAR(0.3, 'FMB99.99') FROM DUAL");
        assertResult(" 00.30", stat,
                "SELECT TO_CHAR(0.3, 'B09.99') FROM DUAL");
        assertResult(" 00.30", stat,
                "SELECT TO_CHAR(0.3, 'B00.00') FROM DUAL");
        assertResult("     " + cc + "123.45", stat,
                "SELECT TO_CHAR(123.45, 'C999.99') FROM DUAL");
        assertResult("    -" + cc + "123.45", stat,
                "SELECT TO_CHAR(-123.45, 'C999.99') FROM DUAL");
        assertResult("         " + cc + "123.45", stat,
                "SELECT TO_CHAR(123.45, 'C999,999.99') FROM DUAL");
        assertResult("         " + cc + "123", stat,
                "SELECT TO_CHAR(123.45, 'C999g999') FROM DUAL");
        assertResult(cc + "123.45", stat,
                "SELECT TO_CHAR(123.45, 'FMC999,999.99') FROM DUAL");
        assertResult("          " + cs + "123.45", stat,
                "SELECT TO_CHAR(123.45, 'L999.99') FROM DUAL");
        assertResult("         -" + cs + "123.45", stat,
                "SELECT TO_CHAR(-123.45, 'L999.99') FROM DUAL");
        assertResult(cs + "123.45", stat,
                "SELECT TO_CHAR(123.45, 'FML999.99') FROM DUAL");
        assertResult("          " + cs + "123.45", stat,
                "SELECT TO_CHAR(123.45, 'U999.99') FROM DUAL");
        assertResult("          " + cs + "123.45", stat,
                "SELECT TO_CHAR(123.45, 'u999.99') FROM DUAL");
        assertResult("   .33", stat,
                "SELECT TO_CHAR(0.326, '99D99') FROM DUAL");
        assertResult("  1.2E+02", stat,
                "SELECT TO_CHAR(123.456, '9.9EEEE') FROM DUAL");
        assertResult("  1.2E+14", stat,
                "SELECT TO_CHAR(123456789012345, '9.9EEEE') FROM DUAL");
        assertResult("  1E+02", stat, "SELECT TO_CHAR(123.456, '9EEEE') FROM DUAL");
        assertResult("  1E+02", stat, "SELECT TO_CHAR(123.456, '999EEEE') FROM DUAL");
        assertResult("  1E-03", stat, "SELECT TO_CHAR(.00123456, '999EEEE') FROM DUAL");
        assertResult("  1E+00", stat, "SELECT TO_CHAR(1, '999EEEE') FROM DUAL");
        assertResult(" -1E+00", stat, "SELECT TO_CHAR(-1, '999EEEE') FROM DUAL");
        assertResult("  1.23456000E+02", stat,
                "SELECT TO_CHAR(123.456, '00.00000000EEEE') FROM DUAL");
        assertResult("1.23456000E+02", stat,
                "SELECT TO_CHAR(123.456, 'fm00.00000000EEEE') FROM DUAL");
        assertResult(" 1,234,567", stat,
                "SELECT TO_CHAR(1234567, '9G999G999') FROM DUAL");
        assertResult("-1,234,567", stat,
                "SELECT TO_CHAR(-1234567, '9G999G999') FROM DUAL");
        assertResult("123.45-", stat, "SELECT TO_CHAR(-123.45, '999.99MI') FROM DUAL");
        assertResult("123.45-", stat, "SELECT TO_CHAR(-123.45, '999.99mi') FROM DUAL");
        assertResult("123.45-", stat, "SELECT TO_CHAR(-123.45, '999.99mI') FROM DUAL");
        assertResult("230.00-", stat, "SELECT TO_CHAR(-230, '999.99MI') FROM DUAL");
        assertResult("230-", stat, "SELECT TO_CHAR(-230, '999MI') FROM DUAL");
        assertResult("123.45 ", stat, "SELECT TO_CHAR(123.45, '999.99MI') FROM DUAL");
        assertResult("230.00 ", stat, "SELECT TO_CHAR(230, '999.99MI') FROM DUAL");
        assertResult("230 ", stat, "SELECT TO_CHAR(230, '999MI') FROM DUAL");
        assertResult("230", stat, "SELECT TO_CHAR(230, 'FM999MI') FROM DUAL");
        assertResult("<230>", stat, "SELECT TO_CHAR(-230, '999PR') FROM DUAL");
        assertResult("<230>", stat, "SELECT TO_CHAR(-230, '999pr') FROM DUAL");
        assertResult("<230>", stat, "SELECT TO_CHAR(-230, 'fm999pr') FROM DUAL");
        assertResult(" 230 ", stat, "SELECT TO_CHAR(230, '999PR') FROM DUAL");
        assertResult("230", stat, "SELECT TO_CHAR(230, 'FM999PR') FROM DUAL");
        assertResult("0", stat, "SELECT TO_CHAR(0, 'fm999pr') FROM DUAL");
        assertResult("             XI", stat, "SELECT TO_CHAR(11, 'RN') FROM DUAL");
        assertResult("XI", stat, "SELECT TO_CHAR(11, 'FMRN') FROM DUAL");
        assertResult("xi", stat, "SELECT TO_CHAR(11, 'FMrN') FROM DUAL");
        assertResult("             XI", stat, "SELECT TO_CHAR(11, 'RN') FROM DUAL;");
        assertResult("             xi", stat, "SELECT TO_CHAR(11, 'rN') FROM DUAL");
        assertResult("             xi", stat, "SELECT TO_CHAR(11, 'rn') FROM DUAL");
        assertResult(" +42", stat, "SELECT TO_CHAR(42, 'S999') FROM DUAL");
        assertResult(" +42", stat, "SELECT TO_CHAR(42, 's999') FROM DUAL");
        assertResult(" 42+", stat, "SELECT TO_CHAR(42, '999S') FROM DUAL");
        assertResult(" -42", stat, "SELECT TO_CHAR(-42, 'S999') FROM DUAL");
        assertResult(" 42-", stat, "SELECT TO_CHAR(-42, '999S') FROM DUAL");
        assertResult("42", stat, "SELECT TO_CHAR(42, 'TM') FROM DUAL");
        assertResult("-42", stat, "SELECT TO_CHAR(-42, 'TM') FROM DUAL");
        assertResult("4212341241234.23412342", stat,
                "SELECT TO_CHAR(4212341241234.23412342, 'tm') FROM DUAL");
        assertResult(".23412342", stat, "SELECT TO_CHAR(0.23412342, 'tm') FROM DUAL");
        assertResult(" 12300", stat, "SELECT TO_CHAR(123, '999V99') FROM DUAL");
        assertResult("######", stat, "SELECT TO_CHAR(1234, '999V99') FROM DUAL");
        assertResult("123400", stat, "SELECT TO_CHAR(1234, 'FM9999v99') FROM DUAL");
        assertResult("1234", stat, "SELECT TO_CHAR(123.4, 'FM9999V9') FROM DUAL");
        assertResult("123", stat, "SELECT TO_CHAR(123.4, 'FM9999V') FROM DUAL");
        assertResult("123400000", stat,
                "SELECT TO_CHAR(123.4, 'FM9999V090909') FROM DUAL");
        assertResult("##", stat, "SELECT TO_CHAR(123, 'X') FROM DUAL");
        assertResult(" 7B", stat, "SELECT TO_CHAR(123, 'XX') FROM DUAL");
        assertResult(" 7b", stat, "SELECT TO_CHAR(123, 'Xx') FROM DUAL");
        assertResult(" 7b", stat, "SELECT TO_CHAR(123, 'xX') FROM DUAL");
        assertResult("   7B", stat, "SELECT TO_CHAR(123, 'XXXX') FROM DUAL");
        assertResult(" 007B", stat, "SELECT TO_CHAR(123, '000X') FROM DUAL");
        assertResult(" 007B", stat, "SELECT TO_CHAR(123, '0XXX') FROM DUAL");
        assertResult("####", stat, "SELECT TO_CHAR(123456789, 'FMXXX') FROM DUAL");
        assertResult("7B", stat, "SELECT TO_CHAR(123, 'FMXX') FROM DUAL");
        assertResult("C6", stat, "SELECT TO_CHAR(197.6, 'FMXX') FROM DUAL");
        assertResult("  7", stat, "SELECT TO_CHAR(7, 'XX') FROM DUAL");
        assertResult("123", stat, "SELECT TO_CHAR(123, 'TM') FROM DUAL");
        assertResult("123", stat, "SELECT TO_CHAR(123, 'tm') FROM DUAL");
        assertResult("123", stat, "SELECT TO_CHAR(123, 'tM9') FROM DUAL");
        assertResult("1.23E+02", stat, "SELECT TO_CHAR(123, 'TME') FROM DUAL");
        assertResult("1.23456789012345E+14", stat,
                "SELECT TO_CHAR(123456789012345, 'TME') FROM DUAL");
        assertResult("4.5E-01", stat, "SELECT TO_CHAR(0.45, 'TME') FROM DUAL");
        assertResult("4.5E-01", stat, "SELECT TO_CHAR(0.45, 'tMe') FROM DUAL");
        assertThrows(ErrorCode.INVALID_TO_CHAR_FORMAT, stat,
                "SELECT TO_CHAR(123.45, '999.99q') FROM DUAL");
        assertThrows(ErrorCode.INVALID_TO_CHAR_FORMAT, stat,
                "SELECT TO_CHAR(123.45, 'fm999.99q') FROM DUAL");
        assertThrows(ErrorCode.INVALID_TO_CHAR_FORMAT, stat,
                "SELECT TO_CHAR(123.45, 'q999.99') FROM DUAL");

        conn.close();
    }

    private void testToCharFromText() throws SQLException {
        deleteDb("functions");
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        assertResult("abc", stat, "SELECT TO_CHAR('abc') FROM DUAL");
        conn.close();
    }

    private void testGenerateSeries() throws SQLException {
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();

        ResultSet rs = stat.executeQuery("select * from system_range(1,3)");
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.next();
        assertEquals(2, rs.getInt(1));
        rs.next();
        assertEquals(3, rs.getInt(1));

        rs = stat.executeQuery("select * from system_range(2,2)");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));

        rs = stat.executeQuery("select * from system_range(2,1)");
        assertFalse(rs.next());

        rs = stat.executeQuery("select * from system_range(1,2,-1)");
        assertFalse(rs.next());

        assertThrows(ErrorCode.STEP_SIZE_MUST_NOT_BE_ZERO, stat).executeQuery(
                "select * from system_range(1,2,0)");

        rs = stat.executeQuery("select * from system_range(2,1,-1)");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));

        rs = stat.executeQuery("select * from system_range(1,5,2)");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));

        rs = stat.executeQuery("select * from system_range(1,6,2)");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));

        conn.close();
    }

    private void assertCallResult(String expected, Statement stat, String sql)
            throws SQLException {
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
    public static BufferedInputStream blob2stream(Blob value)
            throws SQLException {
        if (value == null) {
            return null;
        }
        BufferedInputStream bufferedInStream = new BufferedInputStream(
                value.getBinaryStream());
        return bufferedInStream;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param value the blob
     * @return the blob
     */
    public static Blob blob(Blob value) {
        return value;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param value the blob
     * @return the blob
     */
    public static Clob clob(Clob value) {
        return value;
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
    public static int addRow(Connection conn, int id, String name)
            throws SQLException {
        conn.createStatement().execute(
                "INSERT INTO TEST VALUES(" + id + ", '" + name + "')");
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT COUNT(*) FROM TEST");
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
    public static ResultSet select(Connection conn, String sql)
            throws SQLException {
        Statement stat = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        return stat.executeQuery(sql);
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param conn the connection
     * @return the result set
     */
    public static ResultSet selectMaxId(Connection conn) throws SQLException {
        return conn.createStatement().executeQuery(
                "SELECT MAX(ID) FROM TEST");
    }

    /**
     * This method is called via reflection from the database.
     *
     * @return the test array
     */
    public static Object[] getArray() {
        return new Object[] { 0, "Hello" };
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param conn the connection
     * @return the result set
     */
    public static ResultSet resultSetWithNull(Connection conn) throws SQLException {
        PreparedStatement statement = conn.prepareStatement(
                "select null from system_range(1,1)");
        return statement.executeQuery();
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param conn the connection
     * @return the result set
     */
    public static ResultSet nullResultSet(Connection conn) {
        return null;
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
    public static ResultSet simpleResultSet(Integer rowCount, int ip,
            boolean bp, float fp, double dp, long lp, byte byParam, short sp) {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("ID", Types.INTEGER, 10, 0);
        rs.addColumn("NAME", Types.VARCHAR, 255, 0);
        if (rowCount == null) {
            if (ip != 0 || bp || fp != 0.0 || dp != 0.0 ||
                    sp != 0 || lp != 0 || byParam != 0) {
                throw new AssertionError("params not 0/false");
            }
        }
        if (rowCount != null) {
            if (ip != 1 || !bp || fp != 1.0 || dp != 1.0 ||
                    sp != 1 || lp != 1 || byParam != 1) {
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
    public static double mean(double... values) {
        double sum = 0;
        for (double x : values) {
            sum += x;
        }
        return sum / values.length;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param conn the connection
     * @param values the values
     * @return the mean value
     */
    public static double mean2(Connection conn, double... values) {
        conn.getClass();
        double sum = 0;
        for (double x : values) {
            sum += x;
        }
        return sum / values.length;
    }

    /**
     * This method is called via reflection from the database.
     *
     * @param prefix the print prefix
     * @param values the values
     * @return the text
     */
    public static String printMean(String prefix, double... values) {
        double sum = 0;
        for (double x : values) {
            sum += x;
        }
        return prefix + ": " + (int) (sum / values.length);
    }

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

    @Override
    public void add(Object value) {
        // ignore
    }

    @Override
    public Object getResult() {
        return new BigDecimal("1.6");
    }

    @Override
    public int getType(int[] inputTypes) {
        if (inputTypes.length != 1 || inputTypes[0] != Types.INTEGER) {
            throw new RuntimeException("unexpected data type");
        }
        return Types.DECIMAL;
    }

    @Override
    public void init(Connection conn) {
        // ignore
    }

}
