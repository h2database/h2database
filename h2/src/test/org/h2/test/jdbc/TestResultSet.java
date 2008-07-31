/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;

import org.h2.test.TestBase;

/**
 * Tests for the ResultSet implementation.
 */
public class TestResultSet extends TestBase {
    
    private Connection conn;
    private Statement stat;

    public void test() throws Exception {
        deleteDb("resultSet");
        conn = getConnection("resultSet");

        stat = conn.createStatement();

        testFetchSize();
        testOwnUpdates();
        testFindColumn();
        testSubstringPrecision();
        testColumnLength();
        testArray();
        testLimitMaxRows();

        trace("max rows=" + stat.getMaxRows());
        stat.setMaxRows(6);
        trace("max rows after set to 6=" + stat.getMaxRows());
        assertTrue(stat.getMaxRows() == 6);

        testInt();
        testVarchar();
        testDecimal();
        testDoubleFloat();
        testDatetime();
        testDatetimeWithCalendar();
        testBlob();
        testClob();
        testAutoIncrement();

        conn.close();

    }
    
    private void testFetchSize() throws Exception {
        if (!config.networked || config.memory) {
            return;
        }
        ResultSet rs = stat.executeQuery("SELECT * FROM SYSTEM_RANGE(1, 100)");
        int a = stat.getFetchSize();
        int b = rs.getFetchSize();
        assertEquals(a, b);
        rs.setFetchSize(b + 1);
        b = rs.getFetchSize();
        assertEquals(a + 1, b);
    }
    
    private void testOwnUpdates() throws Exception {
        DatabaseMetaData meta = conn.getMetaData();
        for (int i = 0; i < 3; i++) {
            int type = i == 0 ? ResultSet.TYPE_FORWARD_ONLY : i == 1 ? ResultSet.TYPE_SCROLL_INSENSITIVE : ResultSet.TYPE_SCROLL_SENSITIVE;
            assertTrue(meta.ownUpdatesAreVisible(type));
            assertFalse(meta.ownDeletesAreVisible(type));
            assertFalse(meta.ownInsertsAreVisible(type));
        }
        Statement stat = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
                ResultSet.CONCUR_UPDATABLE);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        stat.execute("INSERT INTO TEST VALUES(2, 'World')");
        ResultSet rs;
        rs = stat.executeQuery("SELECT ID, NAME FROM TEST ORDER BY ID");
        rs.next();
        rs.next();
        rs.updateString(2, "Hallo");
        rs.updateRow();
        assertEquals("Hallo", rs.getString(2));
        stat.execute("DROP TABLE TEST");
    }
    
    private void checkPrecision(int expected, String sql) throws Exception {
        ResultSetMetaData meta = stat.executeQuery(sql).getMetaData();
        assertEquals(expected, meta.getPrecision(1));
    }
    
    private void testSubstringPrecision() throws Exception {
        trace("testSubstringPrecision");
        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR(10))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'WorldPeace')");
        checkPrecision(9, "SELECT SUBSTR(NAME, 2) FROM TEST");
        checkPrecision(10, "SELECT SUBSTR(NAME, ID) FROM TEST");
        checkPrecision(4, "SELECT SUBSTR(NAME, 2, 4) FROM TEST");
        checkPrecision(0, "SELECT SUBSTR(NAME, 12, 4) FROM TEST");
        checkPrecision(3, "SELECT SUBSTR(NAME, 8, 4) FROM TEST");
        checkPrecision(4, "SELECT SUBSTR(NAME, 7, 4) FROM TEST");
        checkPrecision(8, "SELECT SUBSTR(NAME, 3, ID*0) FROM TEST");
        stat.execute("DROP TABLE TEST");
    }

    private void testFindColumn() throws Exception {
        trace("testFindColumn");
        ResultSet rs;
        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR)");
        rs = stat.executeQuery("SELECT * FROM TEST");
        assertEquals(rs.findColumn("ID"), 1);
        assertEquals(rs.findColumn("NAME"), 2);
        assertEquals(rs.findColumn("id"), 1);
        assertEquals(rs.findColumn("name"), 2);
        assertEquals(rs.findColumn("Id"), 1);
        assertEquals(rs.findColumn("Name"), 2);
        assertEquals(rs.findColumn("TEST.ID"), 1);
        assertEquals(rs.findColumn("TEST.NAME"), 2);
        assertEquals(rs.findColumn("Test.Id"), 1);
        assertEquals(rs.findColumn("Test.Name"), 2);
        stat.execute("DROP TABLE TEST");

        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR, DATA VARCHAR)");
        rs = stat.executeQuery("SELECT * FROM TEST");
        assertEquals(rs.findColumn("ID"), 1);
        assertEquals(rs.findColumn("NAME"), 2);
        assertEquals(rs.findColumn("DATA"), 3);
        assertEquals(rs.findColumn("id"), 1);
        assertEquals(rs.findColumn("name"), 2);
        assertEquals(rs.findColumn("data"), 3);
        assertEquals(rs.findColumn("Id"), 1);
        assertEquals(rs.findColumn("Name"), 2);
        assertEquals(rs.findColumn("Data"), 3);
        assertEquals(rs.findColumn("TEST.ID"), 1);
        assertEquals(rs.findColumn("TEST.NAME"), 2);
        assertEquals(rs.findColumn("TEST.DATA"), 3);
        assertEquals(rs.findColumn("Test.Id"), 1);
        assertEquals(rs.findColumn("Test.Name"), 2);
        assertEquals(rs.findColumn("Test.Data"), 3);
        stat.execute("DROP TABLE TEST");

    }

    private void testColumnLength() throws Exception {
        trace("testColumnDisplayLength");
        ResultSet rs;
        ResultSetMetaData meta;

        stat.execute("CREATE TABLE one (ID INT, NAME VARCHAR(255))");
        rs = stat.executeQuery("select * from one");
        meta = rs.getMetaData();
        assertEquals("ID", meta.getColumnLabel(1));
        assertEquals(11, meta.getColumnDisplaySize(1));
        assertEquals("NAME", meta.getColumnLabel(2));
        assertEquals(255, meta.getColumnDisplaySize(2));
        stat.execute("DROP TABLE one");

        rs = stat.executeQuery("select 1, 'Hello' union select 2, 'Hello World!'");
        meta = rs.getMetaData();
        assertEquals(11, meta.getColumnDisplaySize(1));
        assertEquals(12, meta.getColumnDisplaySize(2));

        rs = stat.executeQuery("explain select * from dual");
        meta = rs.getMetaData();
        assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(1));
        assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

        rs = stat.executeQuery("script");
        meta = rs.getMetaData();
        assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(1));
        assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

        rs = stat.executeQuery("select group_concat(table_name) from information_schema.tables");
        rs.next();
        meta = rs.getMetaData();
        assertEquals(Integer.MAX_VALUE, meta.getColumnDisplaySize(1));
        assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

    }

    private void testLimitMaxRows() throws Exception {
        trace("Test LimitMaxRows");
        ResultSet rs;
        stat.execute("CREATE TABLE one (C CHARACTER(10))");
        rs = stat.executeQuery("SELECT C || C FROM one;");
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(20, md.getPrecision(1));
        ResultSet rs2 = stat.executeQuery("SELECT UPPER (C)  FROM one;");
        ResultSetMetaData md2 = rs2.getMetaData();
        assertEquals(10, md2.getPrecision(1));
        rs = stat.executeQuery("SELECT UPPER (C), CHAR(10), CONCAT(C,C,C), HEXTORAW(C), RAWTOHEX(C) FROM one");
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(10, meta.getPrecision(1));
        assertEquals(1, meta.getPrecision(2));
        assertEquals(30, meta.getPrecision(3));
        assertEquals(3, meta.getPrecision(4));
        assertEquals(40, meta.getPrecision(5));
        stat.execute("DROP TABLE one");
    }

    private void testAutoIncrement() throws Exception {
        trace("Test AutoIncrement");
        stat.execute("DROP TABLE IF EXISTS TEST");
        ResultSet rs;
        stat.execute("CREATE TABLE TEST(ID IDENTITY NOT NULL, NAME VARCHAR NULL)");

        stat.execute("INSERT INTO TEST(NAME) VALUES('Hello')");
        rs = stat.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 1);

        stat.execute("INSERT INTO TEST(NAME) VALUES('World')");
        rs = stat.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 2);

        rs = stat.executeQuery("SELECT ID AS I, NAME AS N, ID+1 AS IP1 FROM TEST");
        ResultSetMetaData meta = rs.getMetaData();
        assertTrue(meta.isAutoIncrement(1));
        assertFalse(meta.isAutoIncrement(2));
        assertFalse(meta.isAutoIncrement(3));
        assertEquals(meta.isNullable(1), ResultSetMetaData.columnNoNulls);
        assertEquals(meta.isNullable(2), ResultSetMetaData.columnNullable);
        assertEquals(meta.isNullable(3), ResultSetMetaData.columnNullableUnknown);
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());

    }

    private void testInt() throws Exception {
        trace("Test INT");
        ResultSet rs;
        Object o;

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE INT)");
        stat.execute("INSERT INTO TEST VALUES(1,-1)");
        stat.execute("INSERT INTO TEST VALUES(2,0)");
        stat.execute("INSERT INTO TEST VALUES(3,1)");
        stat.execute("INSERT INTO TEST VALUES(4," + Integer.MAX_VALUE + ")");
        stat.execute("INSERT INTO TEST VALUES(5," + Integer.MIN_VALUE + ")");
        stat.execute("INSERT INTO TEST VALUES(6,NULL)");
        // this should not be read - maxrows=6
        stat.execute("INSERT INTO TEST VALUES(7,NULL)");

        // MySQL compatibility (is this required?)
        // rs=stat.executeQuery("SELECT * FROM TEST T ORDER BY ID");
        // check(rs.findColumn("T.ID"), 1);
        // check(rs.findColumn("T.NAME"), 2);

        rs = stat.executeQuery("SELECT *, NULL AS N FROM TEST ORDER BY ID");

        // MySQL compatibility
        assertEquals(rs.findColumn("TEST.ID"), 1);
        assertEquals(rs.findColumn("TEST.VALUE"), 2);

        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(meta.getColumnCount(), 3);
        assertEquals(meta.getCatalogName(1), "resultSet".toUpperCase());
        assertTrue("PUBLIC".equals(meta.getSchemaName(2)));
        assertTrue("TEST".equals(meta.getTableName(1)));
        assertTrue("ID".equals(meta.getColumnName(1)));
        assertTrue("VALUE".equals(meta.getColumnName(2)));
        assertTrue(!meta.isAutoIncrement(1));
        assertTrue(meta.isCaseSensitive(1));
        assertTrue(meta.isSearchable(1));
        assertFalse(meta.isCurrency(1));
        assertTrue(meta.getColumnDisplaySize(1) > 0);
        assertTrue(meta.isSigned(1));
        assertTrue(meta.isSearchable(2));
        assertEquals(meta.isNullable(1), ResultSetMetaData.columnNoNulls);
        assertFalse(meta.isReadOnly(1));
        assertTrue(meta.isWritable(1));
        assertFalse(meta.isDefinitelyWritable(1));
        assertTrue(meta.getColumnDisplaySize(1) > 0);
        assertTrue(meta.getColumnDisplaySize(2) > 0);
        assertEquals(meta.getColumnClassName(3), null);

        assertTrue(rs.getRow() == 0);
        assertResultSetMeta(rs, 3, new String[] { "ID", "VALUE", "N" }, new int[] { Types.INTEGER, Types.INTEGER,
                Types.NULL }, new int[] { 10, 10, 1 }, new int[] { 0, 0, 0 });
        rs.next();
        assertEquals(rs.getConcurrency(), ResultSet.CONCUR_READ_ONLY);
        assertEquals(rs.getFetchDirection(), ResultSet.FETCH_FORWARD);
        trace("default fetch size=" + rs.getFetchSize());
        // 0 should be an allowed value (but it's not defined what is actually
        // means)
        rs.setFetchSize(0);
        trace("after set to 0, fetch size=" + rs.getFetchSize());
        // this should break
        try {
            rs.setFetchSize(-1);
            fail("fetch size -1 is not allowed");
        } catch (SQLException e) {
            assertKnownException(e);
            trace(e.toString());
        }
        trace("after try to set to -1, fetch size=" + rs.getFetchSize());
        try {
            rs.setFetchSize(100);
            fail("fetch size 100 is bigger than maxrows - not allowed");
        } catch (SQLException e) {
            assertKnownException(e);
            trace(e.toString());
        }
        trace("after try set to 100, fetch size=" + rs.getFetchSize());
        rs.setFetchSize(6);

        assertTrue(rs.getRow() == 1);
        assertEquals(rs.findColumn("VALUE"), 2);
        assertEquals(rs.findColumn("value"), 2);
        assertEquals(rs.findColumn("Value"), 2);
        assertEquals(rs.findColumn("Value"), 2);
        assertEquals(rs.findColumn("ID"), 1);
        assertEquals(rs.findColumn("id"), 1);
        assertEquals(rs.findColumn("Id"), 1);
        assertEquals(rs.findColumn("iD"), 1);
        assertTrue(rs.getInt(2) == -1 && !rs.wasNull());
        assertTrue(rs.getInt("VALUE") == -1 && !rs.wasNull());
        assertTrue(rs.getInt("value") == -1 && !rs.wasNull());
        assertTrue(rs.getInt("Value") == -1 && !rs.wasNull());
        assertTrue(rs.getString("Value").equals("-1") && !rs.wasNull());

        o = rs.getObject("value");
        trace(o.getClass().getName());
        assertTrue(o instanceof Integer);
        assertTrue(((Integer) o).intValue() == -1);
        o = rs.getObject(2);
        trace(o.getClass().getName());
        assertTrue(o instanceof Integer);
        assertTrue(((Integer) o).intValue() == -1);
        assertTrue(rs.getBoolean("Value"));
        assertTrue(rs.getByte("Value") == (byte) -1);
        assertTrue(rs.getShort("Value") == (short) -1);
        assertTrue(rs.getLong("Value") == -1);
        assertTrue(rs.getFloat("Value") == -1.0);
        assertTrue(rs.getDouble("Value") == -1.0);

        assertTrue(rs.getString("Value").equals("-1") && !rs.wasNull());
        assertTrue(rs.getInt("ID") == 1 && !rs.wasNull());
        assertTrue(rs.getInt("id") == 1 && !rs.wasNull());
        assertTrue(rs.getInt("Id") == 1 && !rs.wasNull());
        assertTrue(rs.getInt(1) == 1 && !rs.wasNull());
        rs.next();
        assertTrue(rs.getRow() == 2);
        assertTrue(rs.getInt(2) == 0 && !rs.wasNull());
        assertTrue(!rs.getBoolean(2));
        assertTrue(rs.getByte(2) == 0);
        assertTrue(rs.getShort(2) == 0);
        assertTrue(rs.getLong(2) == 0);
        assertTrue(rs.getFloat(2) == 0.0);
        assertTrue(rs.getDouble(2) == 0.0);
        assertTrue(rs.getString(2).equals("0") && !rs.wasNull());
        assertTrue(rs.getInt(1) == 2 && !rs.wasNull());
        rs.next();
        assertTrue(rs.getRow() == 3);
        assertTrue(rs.getInt("ID") == 3 && !rs.wasNull());
        assertTrue(rs.getInt("VALUE") == 1 && !rs.wasNull());
        rs.next();
        assertTrue(rs.getRow() == 4);
        assertTrue(rs.getInt("ID") == 4 && !rs.wasNull());
        assertTrue(rs.getInt("VALUE") == Integer.MAX_VALUE && !rs.wasNull());
        rs.next();
        assertTrue(rs.getRow() == 5);
        assertTrue(rs.getInt("id") == 5 && !rs.wasNull());
        assertTrue(rs.getInt("value") == Integer.MIN_VALUE && !rs.wasNull());
        assertTrue(rs.getString(1).equals("5") && !rs.wasNull());
        rs.next();
        assertTrue(rs.getRow() == 6);
        assertTrue(rs.getInt("id") == 6 && !rs.wasNull());
        assertTrue(rs.getInt("value") == 0 && rs.wasNull());
        assertTrue(rs.getInt(2) == 0 && rs.wasNull());
        assertTrue(rs.getInt(1) == 6 && !rs.wasNull());
        assertTrue(rs.getString(1).equals("6") && !rs.wasNull());
        assertTrue(rs.getString(2) == null && rs.wasNull());
        o = rs.getObject(2);
        assertTrue(o == null);
        assertTrue(rs.wasNull());
        assertFalse(rs.next());
        assertEquals(rs.getRow(), 0);
        // there is one more row, but because of setMaxRows we don't get it

        stat.execute("DROP TABLE TEST");
        stat.setMaxRows(0);
    }

    private void testVarchar() throws Exception {
        trace("Test VARCHAR");
        ResultSet rs;
        Object o;

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1,'')");
        stat.execute("INSERT INTO TEST VALUES(2,' ')");
        stat.execute("INSERT INTO TEST VALUES(3,'  ')");
        stat.execute("INSERT INTO TEST VALUES(4,NULL)");
        stat.execute("INSERT INTO TEST VALUES(5,'Hi')");
        stat.execute("INSERT INTO TEST VALUES(6,' Hi ')");
        stat.execute("INSERT INTO TEST VALUES(7,'Joe''s')");
        stat.execute("INSERT INTO TEST VALUES(8,'{escape}')");
        stat.execute("INSERT INTO TEST VALUES(9,'\\n')");
        stat.execute("INSERT INTO TEST VALUES(10,'\\''')");
        stat.execute("INSERT INTO TEST VALUES(11,'\\%')");
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 2, new String[] { "ID", "VALUE" }, new int[] { Types.INTEGER, Types.VARCHAR }, new int[] {
                10, 255 }, new int[] { 0, 0 });
        String value;
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <>)");
        assertTrue(value != null && value.equals("") && !rs.wasNull());
        assertTrue(rs.getInt(1) == 1 && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: < >)");
        assertTrue(rs.getString(2).equals(" ") && !rs.wasNull());
        assertTrue(rs.getInt(1) == 2 && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <  >)");
        assertTrue(rs.getString(2).equals("  ") && !rs.wasNull());
        assertTrue(rs.getInt(1) == 3 && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <null>)");
        assertTrue(rs.getString(2) == null && rs.wasNull());
        assertTrue(rs.getInt(1) == 4 && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <Hi>)");
        assertTrue(rs.getInt(1) == 5 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("Hi") && !rs.wasNull());
        o = rs.getObject("value");
        trace(o.getClass().getName());
        assertTrue(o instanceof String);
        assertTrue(o.toString().equals("Hi"));
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: < Hi >)");
        assertTrue(rs.getInt(1) == 6 && !rs.wasNull());
        assertTrue(rs.getString(2).equals(" Hi ") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <Joe's>)");
        assertTrue(rs.getInt(1) == 7 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("Joe's") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <{escape}>)");
        assertTrue(rs.getInt(1) == 8 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("{escape}") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <\\n>)");
        assertTrue(rs.getInt(1) == 9 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("\\n") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <\\'>)");
        assertTrue(rs.getInt(1) == 10 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("\\'") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <\\%>)");
        assertTrue(rs.getInt(1) == 11 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("\\%") && !rs.wasNull());
        assertTrue(!rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private void testDecimal() throws Exception {
        trace("Test DECIMAL");
        ResultSet rs;
        Object o;

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE DECIMAL(10,2))");
        stat.execute("INSERT INTO TEST VALUES(1,-1)");
        stat.execute("INSERT INTO TEST VALUES(2,.0)");
        stat.execute("INSERT INTO TEST VALUES(3,1.)");
        stat.execute("INSERT INTO TEST VALUES(4,12345678.89)");
        stat.execute("INSERT INTO TEST VALUES(6,99999998.99)");
        stat.execute("INSERT INTO TEST VALUES(7,-99999998.99)");
        stat.execute("INSERT INTO TEST VALUES(8,NULL)");
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 2, new String[] { "ID", "VALUE" }, new int[] { Types.INTEGER, Types.DECIMAL }, new int[] {
                10, 10 }, new int[] { 0, 2 });
        BigDecimal bd;
        rs.next();
        assertTrue(rs.getInt(1) == 1);
        assertTrue(!rs.wasNull());
        assertTrue(rs.getInt(2) == -1);
        assertTrue(!rs.wasNull());
        bd = rs.getBigDecimal(2);
        assertTrue(bd.compareTo(new BigDecimal("-1.00")) == 0);
        assertTrue(!rs.wasNull());
        o = rs.getObject(2);
        trace(o.getClass().getName());
        assertTrue(o instanceof BigDecimal);
        assertTrue(((BigDecimal) o).compareTo(new BigDecimal("-1.00")) == 0);
        rs.next();
        assertTrue(rs.getInt(1) == 2);
        assertTrue(!rs.wasNull());
        assertTrue(rs.getInt(2) == 0);
        assertTrue(!rs.wasNull());
        bd = rs.getBigDecimal(2);
        assertTrue(bd.compareTo(new BigDecimal("0.00")) == 0);
        assertTrue(!rs.wasNull());
        rs.next();
        checkColumnBigDecimal(rs, 2, 1, "1.00");
        rs.next();
        checkColumnBigDecimal(rs, 2, 12345679, "12345678.89");
        rs.next();
        checkColumnBigDecimal(rs, 2, 99999999, "99999998.99");
        rs.next();
        checkColumnBigDecimal(rs, 2, -99999999, "-99999998.99");
        rs.next();
        checkColumnBigDecimal(rs, 2, 0, null);
        assertTrue(!rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private void testDoubleFloat() throws Exception {
        trace("Test DOUBLE - FLOAT");
        ResultSet rs;
        Object o;

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, D DOUBLE, R REAL)");
        stat.execute("INSERT INTO TEST VALUES(1, -1, -1)");
        stat.execute("INSERT INTO TEST VALUES(2,.0, .0)");
        stat.execute("INSERT INTO TEST VALUES(3, 1., 1.)");
        stat.execute("INSERT INTO TEST VALUES(4, 12345678.89, 12345678.89)");
        stat.execute("INSERT INTO TEST VALUES(6, 99999999.99, 99999999.99)");
        stat.execute("INSERT INTO TEST VALUES(7, -99999999.99, -99999999.99)");
        stat.execute("INSERT INTO TEST VALUES(8, NULL, NULL)");
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 3, new String[] { "ID", "D", "R" },
                new int[] { Types.INTEGER, Types.DOUBLE, Types.REAL }, new int[] { 10, 17, 7 }, new int[] { 0, 0, 0 });
        BigDecimal bd;
        rs.next();
        assertTrue(rs.getInt(1) == 1);
        assertTrue(!rs.wasNull());
        assertTrue(rs.getInt(2) == -1);
        assertTrue(rs.getInt(3) == -1);
        assertTrue(!rs.wasNull());
        bd = rs.getBigDecimal(2);
        assertTrue(bd.compareTo(new BigDecimal("-1.00")) == 0);
        assertTrue(!rs.wasNull());
        o = rs.getObject(2);
        trace(o.getClass().getName());
        assertTrue(o instanceof Double);
        assertTrue(((Double) o).compareTo(new Double("-1.00")) == 0);
        o = rs.getObject(3);
        trace(o.getClass().getName());
        assertTrue(o instanceof Float);
        assertTrue(((Float) o).compareTo(new Float("-1.00")) == 0);
        rs.next();
        assertTrue(rs.getInt(1) == 2);
        assertTrue(!rs.wasNull());
        assertTrue(rs.getInt(2) == 0);
        assertTrue(!rs.wasNull());
        assertTrue(rs.getInt(3) == 0);
        assertTrue(!rs.wasNull());
        bd = rs.getBigDecimal(2);
        assertTrue(bd.compareTo(new BigDecimal("0.00")) == 0);
        assertTrue(!rs.wasNull());
        bd = rs.getBigDecimal(3);
        assertTrue(bd.compareTo(new BigDecimal("0.00")) == 0);
        assertTrue(!rs.wasNull());
        rs.next();
        assertEquals(rs.getDouble(2), 1.0);
        assertEquals(rs.getFloat(3), 1.0f);
        rs.next();
        assertEquals(rs.getDouble(2), 12345678.89);
        assertEquals(rs.getFloat(3), 12345678.89f);
        rs.next();
        assertEquals(rs.getDouble(2), 99999999.99);
        assertEquals(rs.getFloat(3), 99999999.99f);
        rs.next();
        assertEquals(rs.getDouble(2), -99999999.99);
        assertEquals(rs.getFloat(3), -99999999.99f);
        rs.next();
        checkColumnBigDecimal(rs, 2, 0, null);
        checkColumnBigDecimal(rs, 3, 0, null);
        assertTrue(!rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private void testDatetime() throws Exception {
        trace("Test DATETIME");
        ResultSet rs;
        Object o;

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE DATETIME)");
        stat.execute("INSERT INTO TEST VALUES(1,DATE '2011-11-11')");
        stat.execute("INSERT INTO TEST VALUES(2,TIMESTAMP '2002-02-02 02:02:02')");
        stat.execute("INSERT INTO TEST VALUES(3,TIMESTAMP '1800-1-1 0:0:0')");
        stat.execute("INSERT INTO TEST VALUES(4,TIMESTAMP '9999-12-31 23:59:59')");
        stat.execute("INSERT INTO TEST VALUES(5,NULL)");
        rs = stat.executeQuery("SELECT 0 ID, TIMESTAMP '9999-12-31 23:59:59' VALUE FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 2, new String[] { "ID", "VALUE" }, new int[] { Types.INTEGER, Types.TIMESTAMP },
                new int[] { 10, 23 }, new int[] { 0, 10 });
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 2, new String[] { "ID", "VALUE" }, new int[] { Types.INTEGER, Types.TIMESTAMP },
                new int[] { 10, 23 }, new int[] { 0, 10 });
        rs.next();
        java.sql.Date date;
        java.sql.Time time;
        java.sql.Timestamp ts;
        date = rs.getDate(2);
        assertTrue(!rs.wasNull());
        time = rs.getTime(2);
        assertTrue(!rs.wasNull());
        ts = rs.getTimestamp(2);
        assertTrue(!rs.wasNull());
        trace("Date: " + date.toString() + " Time:" + time.toString() + " Timestamp:" + ts.toString());
        trace("Date ms: " + date.getTime() + " Time ms:" + time.getTime() + " Timestamp ms:" + ts.getTime());
        trace("1970 ms: " + java.sql.Timestamp.valueOf("1970-01-01 00:00:00.0").getTime());
        assertEquals(date.getTime(), java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0").getTime());
        assertEquals(time.getTime(), java.sql.Timestamp.valueOf("1970-01-01 00:00:00.0").getTime());
        assertEquals(ts.getTime(), java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0").getTime());
        assertTrue(date.equals(java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0")));
        assertTrue(time.equals(java.sql.Timestamp.valueOf("1970-01-01 00:00:00.0")));
        assertTrue(ts.equals(java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0")));
        assertFalse(rs.wasNull());
        o = rs.getObject(2);
        trace(o.getClass().getName());
        assertTrue(o instanceof java.sql.Timestamp);
        assertTrue(((java.sql.Timestamp) o).equals(java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0")));
        assertFalse(rs.wasNull());
        rs.next();

        date = rs.getDate("VALUE");
        assertTrue(!rs.wasNull());
        time = rs.getTime("VALUE");
        assertTrue(!rs.wasNull());
        ts = rs.getTimestamp("VALUE");
        assertTrue(!rs.wasNull());
        trace("Date: " + date.toString() + " Time:" + time.toString() + " Timestamp:" + ts.toString());
        assertEquals(date.toString(), "2002-02-02");
        assertEquals(time.toString(), "02:02:02");
        assertEquals(ts.toString(), "2002-02-02 02:02:02.0");
        rs.next();
        assertEquals(rs.getDate("value").toString(), "1800-01-01");
        assertEquals(rs.getTime("value").toString(), "00:00:00");
        assertEquals(rs.getTimestamp("value").toString(), "1800-01-01 00:00:00.0");
        rs.next();
        assertEquals(rs.getDate("Value").toString(), "9999-12-31");
        assertEquals(rs.getTime("Value").toString(), "23:59:59");
        assertEquals(rs.getTimestamp("Value").toString(), "9999-12-31 23:59:59.0");
        rs.next();
        assertTrue(rs.getDate("Value") == null && rs.wasNull());
        assertTrue(rs.getTime("vALUe") == null && rs.wasNull());
        assertTrue(rs.getTimestamp(2) == null && rs.wasNull());
        assertTrue(!rs.next());

        rs = stat
                .executeQuery("SELECT DATE '2001-02-03' D, TIME '14:15:16', TIMESTAMP '2007-08-09 10:11:12.141516171' TS FROM TEST");
        rs.next();
        date = (Date) rs.getObject(1);
        time = (Time) rs.getObject(2);
        ts = (Timestamp) rs.getObject(3);
        assertEquals(date.toString(), "2001-02-03");
        assertEquals(time.toString(), "14:15:16");
        assertEquals(ts.toString(), "2007-08-09 10:11:12.141516171");

        stat.execute("DROP TABLE TEST");
    }

    private void testDatetimeWithCalendar() throws Exception {
        trace("Test DATETIME with Calendar");
        ResultSet rs;

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, D DATE, T TIME, TS TIMESTAMP)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?, ?, ?)");
        Calendar regular = Calendar.getInstance();
        Calendar other = null;
        String[] timeZones = TimeZone.getAvailableIDs();
        // search a locale that has a _different_ raw offset
        for (int i = 0; i < timeZones.length; i++) {
            TimeZone zone = TimeZone.getTimeZone(timeZones[i]);
            if (regular.getTimeZone().getRawOffset() != zone.getRawOffset()) {
                other = Calendar.getInstance(zone);
                break;
            }
        }
        trace("regular offset = " + regular.getTimeZone().getRawOffset() + " other = "
                + other.getTimeZone().getRawOffset());

        prep.setInt(1, 0);
        prep.setDate(2, null, regular);
        prep.setTime(3, null, regular);
        prep.setTimestamp(4, null, regular);
        prep.execute();

        prep.setInt(1, 1);
        prep.setDate(2, null, other);
        prep.setTime(3, null, other);
        prep.setTimestamp(4, null, other);
        prep.execute();

        prep.setInt(1, 2);
        prep.setDate(2, java.sql.Date.valueOf("2001-02-03"), regular);
        prep.setTime(3, java.sql.Time.valueOf("04:05:06"), regular);
        prep.setTimestamp(4, java.sql.Timestamp.valueOf("2007-08-09 10:11:12.131415"), regular);
        prep.execute();

        prep.setInt(1, 3);
        prep.setDate(2, java.sql.Date.valueOf("2101-02-03"), other);
        prep.setTime(3, java.sql.Time.valueOf("14:05:06"), other);
        prep.setTimestamp(4, java.sql.Timestamp.valueOf("2107-08-09 10:11:12.131415"), other);
        prep.execute();

        prep.setInt(1, 4);
        prep.setDate(2, java.sql.Date.valueOf("2101-02-03"));
        prep.setTime(3, java.sql.Time.valueOf("14:05:06"));
        prep.setTimestamp(4, java.sql.Timestamp.valueOf("2107-08-09 10:11:12.131415"));
        prep.execute();

        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 4, new String[] { "ID", "D", "T", "TS" }, new int[] { Types.INTEGER, Types.DATE,
                Types.TIME, Types.TIMESTAMP }, new int[] { 10, 8, 6, 23 }, new int[] { 0, 0, 0, 10 });

        rs.next();
        assertEquals(rs.getInt(1), 0);
        assertTrue(rs.getDate(2, regular) == null && rs.wasNull());
        assertTrue(rs.getTime(3, regular) == null && rs.wasNull());
        assertTrue(rs.getTimestamp(3, regular) == null && rs.wasNull());

        rs.next();
        assertEquals(rs.getInt(1), 1);
        assertTrue(rs.getDate(2, other) == null && rs.wasNull());
        assertTrue(rs.getTime(3, other) == null && rs.wasNull());
        assertTrue(rs.getTimestamp(3, other) == null && rs.wasNull());

        rs.next();
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getDate(2, regular).toString(), "2001-02-03");
        assertEquals(rs.getTime(3, regular).toString(), "04:05:06");
        assertFalse(rs.getTime(3, other).toString().equals("04:05:06"));
        assertEquals(rs.getTimestamp(4, regular).toString(), "2007-08-09 10:11:12.131415");
        assertFalse(rs.getTimestamp(4, other).toString().equals("2007-08-09 10:11:12.131415"));

        rs.next();
        assertEquals(rs.getInt("ID"), 3);
        assertFalse(rs.getTimestamp("TS", regular).toString().equals("2107-08-09 10:11:12.131415"));
        assertEquals(rs.getTimestamp("TS", other).toString(), "2107-08-09 10:11:12.131415");
        assertFalse(rs.getTime("T", regular).toString().equals("14:05:06"));
        assertEquals(rs.getTime("T", other).toString(), "14:05:06");
        // checkFalse(rs.getDate(2, regular).toString(), "2101-02-03");
        // check(rs.getDate("D", other).toString(), "2101-02-03");

        rs.next();
        assertEquals(rs.getInt("ID"), 4);
        assertEquals(rs.getTimestamp("TS").toString(), "2107-08-09 10:11:12.131415");
        assertEquals(rs.getTime("T").toString(), "14:05:06");
        assertEquals(rs.getDate("D").toString(), "2101-02-03");

        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private void testBlob() throws Exception {
        trace("Test BLOB");
        ResultSet rs;

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE BLOB)");
        stat.execute("INSERT INTO TEST VALUES(1,X'01010101')");
        stat.execute("INSERT INTO TEST VALUES(2,X'02020202')");
        stat.execute("INSERT INTO TEST VALUES(3,X'00')");
        stat.execute("INSERT INTO TEST VALUES(4,X'ffffff')");
        stat.execute("INSERT INTO TEST VALUES(5,X'0bcec1')");
        stat.execute("INSERT INTO TEST VALUES(6,NULL)");
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 2, new String[] { "ID", "VALUE" }, new int[] { Types.INTEGER, Types.BLOB }, new int[] {
                10, Integer.MAX_VALUE }, new int[] { 0, 0 });
        rs.next();
        checkBytes(rs.getBytes(2), new byte[] { (byte) 0x01, (byte) 0x01, (byte) 0x01, (byte) 0x01 });
        assertTrue(!rs.wasNull());
        rs.next();
        checkBytes(rs.getBytes("value"), new byte[] { (byte) 0x02, (byte) 0x02, (byte) 0x02, (byte) 0x02 });
        assertTrue(!rs.wasNull());
        rs.next();
        checkBytes(readAllBytes(rs.getBinaryStream(2)), new byte[] { (byte) 0x00 });
        assertTrue(!rs.wasNull());
        rs.next();
        checkBytes(readAllBytes(rs.getBinaryStream("VaLuE")), new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff });
        assertTrue(!rs.wasNull());
        rs.next();
        InputStream in = rs.getBinaryStream("value");
        byte[] b = readAllBytes(in);
        checkBytes(b, new byte[] { (byte) 0x0b, (byte) 0xce, (byte) 0xc1 });
        assertTrue(!rs.wasNull());
        rs.next();
        checkBytes(readAllBytes(rs.getBinaryStream("VaLuE")), null);
        assertTrue(rs.wasNull());
        assertTrue(!rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private void testClob() throws Exception {
        trace("Test CLOB");
        ResultSet rs;
        String string;
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,VALUE CLOB)");
        stat.execute("INSERT INTO TEST VALUES(1,'Test')");
        stat.execute("INSERT INTO TEST VALUES(2,'Hello')");
        stat.execute("INSERT INTO TEST VALUES(3,'World!')");
        stat.execute("INSERT INTO TEST VALUES(4,'Hallo')");
        stat.execute("INSERT INTO TEST VALUES(5,'Welt!')");
        stat.execute("INSERT INTO TEST VALUES(6,NULL)");
        stat.execute("INSERT INTO TEST VALUES(7,NULL)");
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 2, new String[] { "ID", "VALUE" }, new int[] { Types.INTEGER, Types.CLOB }, new int[] {
                10, Integer.MAX_VALUE }, new int[] { 0, 0 });
        rs.next();
        string = rs.getString(2);
        assertTrue(string != null && string.equals("Test"));
        assertTrue(!rs.wasNull());
        rs.next();
        InputStreamReader reader = null;
        try {
            reader = new InputStreamReader(rs.getAsciiStream(2), "ISO-8859-1");
        } catch (Exception e) {
            assertTrue(false);
        }
        string = readString(reader);
        assertTrue(!rs.wasNull());
        trace(string);
        assertTrue(string != null && string.equals("Hello"));
        rs.next();
        try {
            reader = new InputStreamReader(rs.getAsciiStream("value"), "ISO-8859-1");
        } catch (Exception e) {
            assertTrue(false);
        }
        string = readString(reader);
        assertTrue(!rs.wasNull());
        trace(string);
        assertTrue(string != null && string.equals("World!"));
        rs.next();
        string = readString(rs.getCharacterStream(2));
        assertTrue(!rs.wasNull());
        trace(string);
        assertTrue(string != null && string.equals("Hallo"));
        rs.next();
        string = readString(rs.getCharacterStream("value"));
        assertTrue(!rs.wasNull());
        trace(string);
        assertTrue(string != null && string.equals("Welt!"));
        rs.next();
        assertTrue(rs.getCharacterStream(2) == null);
        assertTrue(rs.wasNull());
        rs.next();
        assertTrue(rs.getAsciiStream("Value") == null);
        assertTrue(rs.wasNull());

        assertTrue(rs.getStatement() == stat);
        assertTrue(rs.getWarnings() == null);
        rs.clearWarnings();
        assertTrue(rs.getWarnings() == null);
        assertEquals(rs.getFetchDirection(), ResultSet.FETCH_FORWARD);
        assertEquals(rs.getConcurrency(), ResultSet.CONCUR_UPDATABLE);
        rs.next();
        stat.execute("DROP TABLE TEST");
    }

    private void testArray() throws Exception {
        trace("Test ARRAY");
        ResultSet rs;
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, VALUE ARRAY)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        prep.setInt(1, 1);
        prep.setObject(2, new Object[] { new Integer(1), new Integer(2) });
        prep.execute();
        prep.setInt(1, 2);
        prep.setObject(2, new Object[] { new Integer(11), new Integer(12) });
        prep.execute();
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        assertEquals(rs.getInt(1), 1);
        Object[] list = (Object[]) rs.getObject(2);
        assertEquals(((Integer) list[0]).intValue(), 1);
        assertEquals(((Integer) list[1]).intValue(), 2);
        Array array = rs.getArray(2);
        Object[] list2 = (Object[]) array.getArray();
        assertEquals(((Integer) list2[0]).intValue(), 1);
        assertEquals(((Integer) list2[1]).intValue(), 2);
        list2 = (Object[]) array.getArray(2, 1);
        assertEquals(((Integer) list2[0]).intValue(), 2);
        rs.next();
        assertEquals(rs.getInt(1), 2);
        list = (Object[]) rs.getObject(2);
        assertEquals(((Integer) list[0]).intValue(), 11);
        assertEquals(((Integer) list[1]).intValue(), 12);
        array = rs.getArray(2);
        list2 = (Object[]) array.getArray();
        assertEquals(((Integer) list2[0]).intValue(), 11);
        assertEquals(((Integer) list2[1]).intValue(), 12);
        list2 = (Object[]) array.getArray(2, 1);
        assertEquals(((Integer) list2[0]).intValue(), 12);
        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private byte[] readAllBytes(InputStream in) throws Exception {
        if (in == null) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            while (true) {
                int b = in.read();
                if (b == -1) {
                    break;
                }
                out.write(b);
            }
            return out.toByteArray();
        } catch (IOException e) {
            assertTrue(false);
            return null;
        }
    }

    private void checkBytes(byte[] test, byte[] good) throws Exception {
        if (test == null || good == null) {
            assertTrue(test == good);
        } else {
            assertEquals(test, good);
        }
    }

    private void checkColumnBigDecimal(ResultSet rs, int column, int i, String bd) throws Exception {
        BigDecimal bd1 = rs.getBigDecimal(column);
        int i1 = rs.getInt(column);
        if (bd == null) {
            trace("should be: null");
            assertTrue(rs.wasNull());
        } else {
            trace("BigDecimal i=" + i + " bd=" + bd + " ; i1=" + i1 + " bd1=" + bd1);
            assertTrue(!rs.wasNull());
            assertTrue(i1 == i);
            assertTrue(bd1.compareTo(new BigDecimal(bd)) == 0);
        }
    }

}
