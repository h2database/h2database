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
    Connection conn;
    Statement stat;

    public void test() throws Exception {
        deleteDb("resultSet");
        conn = getConnection("resultSet");

        stat = conn.createStatement();

        testOwnUpdates();
        testFindColumn();
        testSubstringPrecision();
        testColumnLength();
        testArray();
        testLimitMaxRows();

        trace("max rows=" + stat.getMaxRows());
        stat.setMaxRows(6);
        trace("max rows after set to 6=" + stat.getMaxRows());
        check(stat.getMaxRows() == 6);

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
    
    private void testOwnUpdates() throws Exception {
        DatabaseMetaData meta = conn.getMetaData();
        for (int i = 0; i < 3; i++) {
            int type = i == 0 ? ResultSet.TYPE_FORWARD_ONLY : i == 1 ? ResultSet.TYPE_SCROLL_INSENSITIVE : ResultSet.TYPE_SCROLL_SENSITIVE;
            check(meta.ownUpdatesAreVisible(type));
            checkFalse(meta.ownDeletesAreVisible(type));
            checkFalse(meta.ownInsertsAreVisible(type));
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
        check("Hallo", rs.getString(2));
        stat.execute("DROP TABLE TEST");
    }
    
    private void checkPrecision(int expected, String sql) throws Exception {
        ResultSetMetaData meta = stat.executeQuery(sql).getMetaData();
        check(expected, meta.getPrecision(1));
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
        check(rs.findColumn("ID"), 1);
        check(rs.findColumn("NAME"), 2);
        check(rs.findColumn("id"), 1);
        check(rs.findColumn("name"), 2);
        check(rs.findColumn("Id"), 1);
        check(rs.findColumn("Name"), 2);
        check(rs.findColumn("TEST.ID"), 1);
        check(rs.findColumn("TEST.NAME"), 2);
        check(rs.findColumn("Test.Id"), 1);
        check(rs.findColumn("Test.Name"), 2);
        stat.execute("DROP TABLE TEST");

        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR, DATA VARCHAR)");
        rs = stat.executeQuery("SELECT * FROM TEST");
        check(rs.findColumn("ID"), 1);
        check(rs.findColumn("NAME"), 2);
        check(rs.findColumn("DATA"), 3);
        check(rs.findColumn("id"), 1);
        check(rs.findColumn("name"), 2);
        check(rs.findColumn("data"), 3);
        check(rs.findColumn("Id"), 1);
        check(rs.findColumn("Name"), 2);
        check(rs.findColumn("Data"), 3);
        check(rs.findColumn("TEST.ID"), 1);
        check(rs.findColumn("TEST.NAME"), 2);
        check(rs.findColumn("TEST.DATA"), 3);
        check(rs.findColumn("Test.Id"), 1);
        check(rs.findColumn("Test.Name"), 2);
        check(rs.findColumn("Test.Data"), 3);
        stat.execute("DROP TABLE TEST");

    }

    private void testColumnLength() throws Exception {
        trace("testColumnDisplayLength");
        ResultSet rs;
        ResultSetMetaData meta;

        stat.execute("CREATE TABLE one (ID INT, NAME VARCHAR(255))");
        rs = stat.executeQuery("select * from one");
        meta = rs.getMetaData();
        check("ID", meta.getColumnLabel(1));
        check(11, meta.getColumnDisplaySize(1));
        check("NAME", meta.getColumnLabel(2));
        check(255, meta.getColumnDisplaySize(2));
        stat.execute("DROP TABLE one");

        rs = stat.executeQuery("select 1, 'Hello' union select 2, 'Hello World!'");
        meta = rs.getMetaData();
        check(11, meta.getColumnDisplaySize(1));
        check(12, meta.getColumnDisplaySize(2));

        rs = stat.executeQuery("explain select * from dual");
        meta = rs.getMetaData();
        check(Integer.MAX_VALUE, meta.getColumnDisplaySize(1));
        check(Integer.MAX_VALUE, meta.getPrecision(1));

        rs = stat.executeQuery("script");
        meta = rs.getMetaData();
        check(Integer.MAX_VALUE, meta.getColumnDisplaySize(1));
        check(Integer.MAX_VALUE, meta.getPrecision(1));

        rs = stat.executeQuery("select group_concat(table_name) from information_schema.tables");
        rs.next();
        meta = rs.getMetaData();
        check(Integer.MAX_VALUE, meta.getColumnDisplaySize(1));
        check(Integer.MAX_VALUE, meta.getPrecision(1));

    }

    private void testLimitMaxRows() throws Exception {
        trace("Test LimitMaxRows");
        ResultSet rs;
        stat.execute("CREATE TABLE one (C CHARACTER(10))");
        rs = stat.executeQuery("SELECT C || C FROM one;");
        ResultSetMetaData md = rs.getMetaData();
        check(20, md.getPrecision(1));
        ResultSet rs2 = stat.executeQuery("SELECT UPPER (C)  FROM one;");
        ResultSetMetaData md2 = rs2.getMetaData();
        check(10, md2.getPrecision(1));
        rs = stat.executeQuery("SELECT UPPER (C), CHAR(10), CONCAT(C,C,C), HEXTORAW(C), RAWTOHEX(C) FROM one");
        ResultSetMetaData meta = rs.getMetaData();
        check(10, meta.getPrecision(1));
        check(1, meta.getPrecision(2));
        check(30, meta.getPrecision(3));
        check(3, meta.getPrecision(4));
        check(40, meta.getPrecision(5));
        stat.execute("DROP TABLE one");
    }

    void testAutoIncrement() throws Exception {
        trace("Test AutoIncrement");
        stat.execute("DROP TABLE IF EXISTS TEST");
        ResultSet rs;
        stat.execute("CREATE TABLE TEST(ID IDENTITY NOT NULL, NAME VARCHAR NULL)");

        stat.execute("INSERT INTO TEST(NAME) VALUES('Hello')");
        rs = stat.getGeneratedKeys();
        check(rs.next());
        check(rs.getInt(1), 1);

        stat.execute("INSERT INTO TEST(NAME) VALUES('World')");
        rs = stat.getGeneratedKeys();
        check(rs.next());
        check(rs.getInt(1), 2);

        rs = stat.executeQuery("SELECT ID AS I, NAME AS N, ID+1 AS IP1 FROM TEST");
        ResultSetMetaData meta = rs.getMetaData();
        check(meta.isAutoIncrement(1));
        checkFalse(meta.isAutoIncrement(2));
        checkFalse(meta.isAutoIncrement(3));
        check(meta.isNullable(1), ResultSetMetaData.columnNoNulls);
        check(meta.isNullable(2), ResultSetMetaData.columnNullable);
        check(meta.isNullable(3), ResultSetMetaData.columnNullableUnknown);
        check(rs.next());
        check(rs.next());
        checkFalse(rs.next());

    }

    void testInt() throws Exception {
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
        check(rs.findColumn("TEST.ID"), 1);
        check(rs.findColumn("TEST.VALUE"), 2);

        ResultSetMetaData meta = rs.getMetaData();
        check(meta.getColumnCount(), 3);
        check(meta.getCatalogName(1), "resultSet".toUpperCase());
        check("PUBLIC".equals(meta.getSchemaName(2)));
        check("TEST".equals(meta.getTableName(1)));
        check("ID".equals(meta.getColumnName(1)));
        check("VALUE".equals(meta.getColumnName(2)));
        check(!meta.isAutoIncrement(1));
        check(meta.isCaseSensitive(1));
        check(meta.isSearchable(1));
        checkFalse(meta.isCurrency(1));
        check(meta.getColumnDisplaySize(1) > 0);
        check(meta.isSigned(1));
        check(meta.isSearchable(2));
        check(meta.isNullable(1), ResultSetMetaData.columnNoNulls);
        checkFalse(meta.isReadOnly(1));
        check(meta.isWritable(1));
        checkFalse(meta.isDefinitelyWritable(1));
        check(meta.getColumnDisplaySize(1) > 0);
        check(meta.getColumnDisplaySize(2) > 0);
        check(meta.getColumnClassName(3), null);

        check(rs.getRow() == 0);
        testResultSetMeta(rs, 3, new String[] { "ID", "VALUE", "N" }, new int[] { Types.INTEGER, Types.INTEGER,
                Types.NULL }, new int[] { 10, 10, 1 }, new int[] { 0, 0, 0 });
        rs.next();
        check(rs.getConcurrency(), ResultSet.CONCUR_READ_ONLY);
        check(rs.getFetchDirection(), ResultSet.FETCH_FORWARD);
        trace("default fetch size=" + rs.getFetchSize());
        // 0 should be an allowed value (but it's not defined what is actually
        // means)
        rs.setFetchSize(0);
        trace("after set to 0, fetch size=" + rs.getFetchSize());
        // this should break
        try {
            rs.setFetchSize(-1);
            error("fetch size -1 is not allowed");
        } catch (SQLException e) {
            checkNotGeneralException(e);
            trace(e.toString());
        }
        trace("after try to set to -1, fetch size=" + rs.getFetchSize());
        try {
            rs.setFetchSize(100);
            error("fetch size 100 is bigger than maxrows - not allowed");
        } catch (SQLException e) {
            checkNotGeneralException(e);
            trace(e.toString());
        }
        trace("after try set to 100, fetch size=" + rs.getFetchSize());
        rs.setFetchSize(6);

        check(rs.getRow() == 1);
        check(rs.findColumn("VALUE"), 2);
        check(rs.findColumn("value"), 2);
        check(rs.findColumn("Value"), 2);
        check(rs.findColumn("Value"), 2);
        check(rs.findColumn("ID"), 1);
        check(rs.findColumn("id"), 1);
        check(rs.findColumn("Id"), 1);
        check(rs.findColumn("iD"), 1);
        check(rs.getInt(2) == -1 && !rs.wasNull());
        check(rs.getInt("VALUE") == -1 && !rs.wasNull());
        check(rs.getInt("value") == -1 && !rs.wasNull());
        check(rs.getInt("Value") == -1 && !rs.wasNull());
        check(rs.getString("Value").equals("-1") && !rs.wasNull());

        o = rs.getObject("value");
        trace(o.getClass().getName());
        check(o instanceof Integer);
        check(((Integer) o).intValue() == -1);
        o = rs.getObject(2);
        trace(o.getClass().getName());
        check(o instanceof Integer);
        check(((Integer) o).intValue() == -1);
        check(rs.getBoolean("Value"));
        check(rs.getByte("Value") == (byte) -1);
        check(rs.getShort("Value") == (short) -1);
        check(rs.getLong("Value") == -1);
        check(rs.getFloat("Value") == -1.0);
        check(rs.getDouble("Value") == -1.0);

        check(rs.getString("Value").equals("-1") && !rs.wasNull());
        check(rs.getInt("ID") == 1 && !rs.wasNull());
        check(rs.getInt("id") == 1 && !rs.wasNull());
        check(rs.getInt("Id") == 1 && !rs.wasNull());
        check(rs.getInt(1) == 1 && !rs.wasNull());
        rs.next();
        check(rs.getRow() == 2);
        check(rs.getInt(2) == 0 && !rs.wasNull());
        check(!rs.getBoolean(2));
        check(rs.getByte(2) == 0);
        check(rs.getShort(2) == 0);
        check(rs.getLong(2) == 0);
        check(rs.getFloat(2) == 0.0);
        check(rs.getDouble(2) == 0.0);
        check(rs.getString(2).equals("0") && !rs.wasNull());
        check(rs.getInt(1) == 2 && !rs.wasNull());
        rs.next();
        check(rs.getRow() == 3);
        check(rs.getInt("ID") == 3 && !rs.wasNull());
        check(rs.getInt("VALUE") == 1 && !rs.wasNull());
        rs.next();
        check(rs.getRow() == 4);
        check(rs.getInt("ID") == 4 && !rs.wasNull());
        check(rs.getInt("VALUE") == Integer.MAX_VALUE && !rs.wasNull());
        rs.next();
        check(rs.getRow() == 5);
        check(rs.getInt("id") == 5 && !rs.wasNull());
        check(rs.getInt("value") == Integer.MIN_VALUE && !rs.wasNull());
        check(rs.getString(1).equals("5") && !rs.wasNull());
        rs.next();
        check(rs.getRow() == 6);
        check(rs.getInt("id") == 6 && !rs.wasNull());
        check(rs.getInt("value") == 0 && rs.wasNull());
        check(rs.getInt(2) == 0 && rs.wasNull());
        check(rs.getInt(1) == 6 && !rs.wasNull());
        check(rs.getString(1).equals("6") && !rs.wasNull());
        check(rs.getString(2) == null && rs.wasNull());
        o = rs.getObject(2);
        check(o == null);
        check(rs.wasNull());
        checkFalse(rs.next());
        check(rs.getRow(), 0);
        // there is one more row, but because of setMaxRows we don't get it

        stat.execute("DROP TABLE TEST");
        stat.setMaxRows(0);
    }

    void testVarchar() throws Exception {
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
        testResultSetMeta(rs, 2, new String[] { "ID", "VALUE" }, new int[] { Types.INTEGER, Types.VARCHAR }, new int[] {
                10, 255 }, new int[] { 0, 0 });
        String value;
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <>)");
        check(value != null && value.equals("") && !rs.wasNull());
        check(rs.getInt(1) == 1 && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: < >)");
        check(rs.getString(2).equals(" ") && !rs.wasNull());
        check(rs.getInt(1) == 2 && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <  >)");
        check(rs.getString(2).equals("  ") && !rs.wasNull());
        check(rs.getInt(1) == 3 && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <null>)");
        check(rs.getString(2) == null && rs.wasNull());
        check(rs.getInt(1) == 4 && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <Hi>)");
        check(rs.getInt(1) == 5 && !rs.wasNull());
        check(rs.getString(2).equals("Hi") && !rs.wasNull());
        o = rs.getObject("value");
        trace(o.getClass().getName());
        check(o instanceof String);
        check(o.toString().equals("Hi"));
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: < Hi >)");
        check(rs.getInt(1) == 6 && !rs.wasNull());
        check(rs.getString(2).equals(" Hi ") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <Joe's>)");
        check(rs.getInt(1) == 7 && !rs.wasNull());
        check(rs.getString(2).equals("Joe's") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <{escape}>)");
        check(rs.getInt(1) == 8 && !rs.wasNull());
        check(rs.getString(2).equals("{escape}") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <\\n>)");
        check(rs.getInt(1) == 9 && !rs.wasNull());
        check(rs.getString(2).equals("\\n") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <\\'>)");
        check(rs.getInt(1) == 10 && !rs.wasNull());
        check(rs.getString(2).equals("\\'") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        trace("Value: <" + value + "> (should be: <\\%>)");
        check(rs.getInt(1) == 11 && !rs.wasNull());
        check(rs.getString(2).equals("\\%") && !rs.wasNull());
        check(!rs.next());
        stat.execute("DROP TABLE TEST");
    }

    void testDecimal() throws Exception {
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
        testResultSetMeta(rs, 2, new String[] { "ID", "VALUE" }, new int[] { Types.INTEGER, Types.DECIMAL }, new int[] {
                10, 10 }, new int[] { 0, 2 });
        BigDecimal bd;
        rs.next();
        check(rs.getInt(1) == 1);
        check(!rs.wasNull());
        check(rs.getInt(2) == -1);
        check(!rs.wasNull());
        bd = rs.getBigDecimal(2);
        check(bd.compareTo(new BigDecimal("-1.00")) == 0);
        check(!rs.wasNull());
        o = rs.getObject(2);
        trace(o.getClass().getName());
        check(o instanceof BigDecimal);
        check(((BigDecimal) o).compareTo(new BigDecimal("-1.00")) == 0);
        rs.next();
        check(rs.getInt(1) == 2);
        check(!rs.wasNull());
        check(rs.getInt(2) == 0);
        check(!rs.wasNull());
        bd = rs.getBigDecimal(2);
        check(bd.compareTo(new BigDecimal("0.00")) == 0);
        check(!rs.wasNull());
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
        check(!rs.next());
        stat.execute("DROP TABLE TEST");
    }

    void testDoubleFloat() throws Exception {
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
        testResultSetMeta(rs, 3, new String[] { "ID", "D", "R" },
                new int[] { Types.INTEGER, Types.DOUBLE, Types.REAL }, new int[] { 10, 17, 7 }, new int[] { 0, 0, 0 });
        BigDecimal bd;
        rs.next();
        check(rs.getInt(1) == 1);
        check(!rs.wasNull());
        check(rs.getInt(2) == -1);
        check(rs.getInt(3) == -1);
        check(!rs.wasNull());
        bd = rs.getBigDecimal(2);
        check(bd.compareTo(new BigDecimal("-1.00")) == 0);
        check(!rs.wasNull());
        o = rs.getObject(2);
        trace(o.getClass().getName());
        check(o instanceof Double);
        check(((Double) o).compareTo(new Double("-1.00")) == 0);
        o = rs.getObject(3);
        trace(o.getClass().getName());
        check(o instanceof Float);
        check(((Float) o).compareTo(new Float("-1.00")) == 0);
        rs.next();
        check(rs.getInt(1) == 2);
        check(!rs.wasNull());
        check(rs.getInt(2) == 0);
        check(!rs.wasNull());
        check(rs.getInt(3) == 0);
        check(!rs.wasNull());
        bd = rs.getBigDecimal(2);
        check(bd.compareTo(new BigDecimal("0.00")) == 0);
        check(!rs.wasNull());
        bd = rs.getBigDecimal(3);
        check(bd.compareTo(new BigDecimal("0.00")) == 0);
        check(!rs.wasNull());
        rs.next();
        check(rs.getDouble(2), 1.0);
        check(rs.getFloat(3), 1.0f);
        rs.next();
        check(rs.getDouble(2), 12345678.89);
        check(rs.getFloat(3), 12345678.89f);
        rs.next();
        check(rs.getDouble(2), 99999999.99);
        check(rs.getFloat(3), 99999999.99f);
        rs.next();
        check(rs.getDouble(2), -99999999.99);
        check(rs.getFloat(3), -99999999.99f);
        rs.next();
        checkColumnBigDecimal(rs, 2, 0, null);
        checkColumnBigDecimal(rs, 3, 0, null);
        check(!rs.next());
        stat.execute("DROP TABLE TEST");
    }

    void testDatetime() throws Exception {
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
        testResultSetMeta(rs, 2, new String[] { "ID", "VALUE" }, new int[] { Types.INTEGER, Types.TIMESTAMP },
                new int[] { 10, 23 }, new int[] { 0, 10 });
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        testResultSetMeta(rs, 2, new String[] { "ID", "VALUE" }, new int[] { Types.INTEGER, Types.TIMESTAMP },
                new int[] { 10, 23 }, new int[] { 0, 10 });
        rs.next();
        java.sql.Date date;
        java.sql.Time time;
        java.sql.Timestamp ts;
        date = rs.getDate(2);
        check(!rs.wasNull());
        time = rs.getTime(2);
        check(!rs.wasNull());
        ts = rs.getTimestamp(2);
        check(!rs.wasNull());
        trace("Date: " + date.toString() + " Time:" + time.toString() + " Timestamp:" + ts.toString());
        trace("Date ms: " + date.getTime() + " Time ms:" + time.getTime() + " Timestamp ms:" + ts.getTime());
        trace("1970 ms: " + java.sql.Timestamp.valueOf("1970-01-01 00:00:00.0").getTime());
        check(date.getTime(), java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0").getTime());
        check(time.getTime(), java.sql.Timestamp.valueOf("1970-01-01 00:00:00.0").getTime());
        check(ts.getTime(), java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0").getTime());
        check(date.equals(java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0")));
        check(time.equals(java.sql.Timestamp.valueOf("1970-01-01 00:00:00.0")));
        check(ts.equals(java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0")));
        checkFalse(rs.wasNull());
        o = rs.getObject(2);
        trace(o.getClass().getName());
        check(o instanceof java.sql.Timestamp);
        check(((java.sql.Timestamp) o).equals(java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0")));
        checkFalse(rs.wasNull());
        rs.next();

        date = rs.getDate("VALUE");
        check(!rs.wasNull());
        time = rs.getTime("VALUE");
        check(!rs.wasNull());
        ts = rs.getTimestamp("VALUE");
        check(!rs.wasNull());
        trace("Date: " + date.toString() + " Time:" + time.toString() + " Timestamp:" + ts.toString());
        check(date.toString(), "2002-02-02");
        check(time.toString(), "02:02:02");
        check(ts.toString(), "2002-02-02 02:02:02.0");
        rs.next();
        check(rs.getDate("value").toString(), "1800-01-01");
        check(rs.getTime("value").toString(), "00:00:00");
        check(rs.getTimestamp("value").toString(), "1800-01-01 00:00:00.0");
        rs.next();
        check(rs.getDate("Value").toString(), "9999-12-31");
        check(rs.getTime("Value").toString(), "23:59:59");
        check(rs.getTimestamp("Value").toString(), "9999-12-31 23:59:59.0");
        rs.next();
        check(rs.getDate("Value") == null && rs.wasNull());
        check(rs.getTime("vALUe") == null && rs.wasNull());
        check(rs.getTimestamp(2) == null && rs.wasNull());
        check(!rs.next());

        rs = stat
                .executeQuery("SELECT DATE '2001-02-03' D, TIME '14:15:16', TIMESTAMP '2007-08-09 10:11:12.141516171' TS FROM TEST");
        rs.next();
        date = (Date) rs.getObject(1);
        time = (Time) rs.getObject(2);
        ts = (Timestamp) rs.getObject(3);
        check(date.toString(), "2001-02-03");
        check(time.toString(), "14:15:16");
        check(ts.toString(), "2007-08-09 10:11:12.141516171");

        stat.execute("DROP TABLE TEST");
    }

    void testDatetimeWithCalendar() throws Exception {
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
        testResultSetMeta(rs, 4, new String[] { "ID", "D", "T", "TS" }, new int[] { Types.INTEGER, Types.DATE,
                Types.TIME, Types.TIMESTAMP }, new int[] { 10, 8, 6, 23 }, new int[] { 0, 0, 0, 10 });

        rs.next();
        check(rs.getInt(1), 0);
        check(rs.getDate(2, regular) == null && rs.wasNull());
        check(rs.getTime(3, regular) == null && rs.wasNull());
        check(rs.getTimestamp(3, regular) == null && rs.wasNull());

        rs.next();
        check(rs.getInt(1), 1);
        check(rs.getDate(2, other) == null && rs.wasNull());
        check(rs.getTime(3, other) == null && rs.wasNull());
        check(rs.getTimestamp(3, other) == null && rs.wasNull());

        rs.next();
        check(rs.getInt(1), 2);
        check(rs.getDate(2, regular).toString(), "2001-02-03");
        check(rs.getTime(3, regular).toString(), "04:05:06");
        checkFalse(rs.getTime(3, other).toString(), "04:05:06");
        check(rs.getTimestamp(4, regular).toString(), "2007-08-09 10:11:12.131415");
        checkFalse(rs.getTimestamp(4, other).toString(), "2007-08-09 10:11:12.131415");

        rs.next();
        check(rs.getInt("ID"), 3);
        checkFalse(rs.getTimestamp("TS", regular).toString(), "2107-08-09 10:11:12.131415");
        check(rs.getTimestamp("TS", other).toString(), "2107-08-09 10:11:12.131415");
        checkFalse(rs.getTime("T", regular).toString(), "14:05:06");
        check(rs.getTime("T", other).toString(), "14:05:06");
        // checkFalse(rs.getDate(2, regular).toString(), "2101-02-03");
        // check(rs.getDate("D", other).toString(), "2101-02-03");

        rs.next();
        check(rs.getInt("ID"), 4);
        check(rs.getTimestamp("TS").toString(), "2107-08-09 10:11:12.131415");
        check(rs.getTime("T").toString(), "14:05:06");
        check(rs.getDate("D").toString(), "2101-02-03");

        checkFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

    void testBlob() throws Exception {
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
        testResultSetMeta(rs, 2, new String[] { "ID", "VALUE" }, new int[] { Types.INTEGER, Types.BLOB }, new int[] {
                10, Integer.MAX_VALUE }, new int[] { 0, 0 });
        rs.next();
        checkBytes(rs.getBytes(2), new byte[] { (byte) 0x01, (byte) 0x01, (byte) 0x01, (byte) 0x01 });
        check(!rs.wasNull());
        rs.next();
        checkBytes(rs.getBytes("value"), new byte[] { (byte) 0x02, (byte) 0x02, (byte) 0x02, (byte) 0x02 });
        check(!rs.wasNull());
        rs.next();
        checkBytes(readAllBytes(rs.getBinaryStream(2)), new byte[] { (byte) 0x00 });
        check(!rs.wasNull());
        rs.next();
        checkBytes(readAllBytes(rs.getBinaryStream("VaLuE")), new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff });
        check(!rs.wasNull());
        rs.next();
        InputStream in = rs.getBinaryStream("value");
        byte[] b = readAllBytes(in);
        checkBytes(b, new byte[] { (byte) 0x0b, (byte) 0xce, (byte) 0xc1 });
        check(!rs.wasNull());
        rs.next();
        checkBytes(readAllBytes(rs.getBinaryStream("VaLuE")), null);
        check(rs.wasNull());
        check(!rs.next());
        stat.execute("DROP TABLE TEST");
    }

    void testClob() throws Exception {
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
        testResultSetMeta(rs, 2, new String[] { "ID", "VALUE" }, new int[] { Types.INTEGER, Types.CLOB }, new int[] {
                10, Integer.MAX_VALUE }, new int[] { 0, 0 });
        rs.next();
        string = rs.getString(2);
        check(string != null && string.equals("Test"));
        check(!rs.wasNull());
        rs.next();
        InputStreamReader reader = null;
        try {
            reader = new InputStreamReader(rs.getAsciiStream(2), "ISO-8859-1");
        } catch (Exception e) {
            check(false);
        }
        string = readString(reader);
        check(!rs.wasNull());
        trace(string);
        check(string != null && string.equals("Hello"));
        rs.next();
        try {
            reader = new InputStreamReader(rs.getAsciiStream("value"), "ISO-8859-1");
        } catch (Exception e) {
            check(false);
        }
        string = readString(reader);
        check(!rs.wasNull());
        trace(string);
        check(string != null && string.equals("World!"));
        rs.next();
        string = readString(rs.getCharacterStream(2));
        check(!rs.wasNull());
        trace(string);
        check(string != null && string.equals("Hallo"));
        rs.next();
        string = readString(rs.getCharacterStream("value"));
        check(!rs.wasNull());
        trace(string);
        check(string != null && string.equals("Welt!"));
        rs.next();
        check(rs.getCharacterStream(2) == null);
        check(rs.wasNull());
        rs.next();
        check(rs.getAsciiStream("Value") == null);
        check(rs.wasNull());

        check(rs.getStatement() == stat);
        check(rs.getWarnings() == null);
        rs.clearWarnings();
        check(rs.getWarnings() == null);
        check(rs.getFetchDirection(), ResultSet.FETCH_FORWARD);
        check(rs.getConcurrency(), ResultSet.CONCUR_UPDATABLE);
        rs.next();
        stat.execute("DROP TABLE TEST");
    }

    void testArray() throws Exception {
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
        check(rs.getInt(1), 1);
        Object[] list = (Object[]) rs.getObject(2);
        check(((Integer) list[0]).intValue(), 1);
        check(((Integer) list[1]).intValue(), 2);
        Array array = rs.getArray(2);
        Object[] list2 = (Object[]) array.getArray();
        check(((Integer) list2[0]).intValue(), 1);
        check(((Integer) list2[1]).intValue(), 2);
        list2 = (Object[]) array.getArray(2, 1);
        check(((Integer) list2[0]).intValue(), 2);
        rs.next();
        check(rs.getInt(1), 2);
        list = (Object[]) rs.getObject(2);
        check(((Integer) list[0]).intValue(), 11);
        check(((Integer) list[1]).intValue(), 12);
        array = rs.getArray(2);
        list2 = (Object[]) array.getArray();
        check(((Integer) list2[0]).intValue(), 11);
        check(((Integer) list2[1]).intValue(), 12);
        list2 = (Object[]) array.getArray(2, 1);
        check(((Integer) list2[0]).intValue(), 12);
        checkFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

    byte[] readAllBytes(InputStream in) throws Exception {
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
            check(false);
            return null;
        }
    }

    void checkBytes(byte[] test, byte[] good) throws Exception {
        if (test == null || good == null) {
            check(test == null && good == null);
        } else {
            trace("test.length=" + test.length + " good.length=" + good.length);
            check(test.length, good.length);
            for (int i = 0; i < good.length; i++) {
                check(test[i] == good[i]);
            }
        }
    }

    void checkColumnBigDecimal(ResultSet rs, int column, int i, String bd) throws Exception {
        BigDecimal bd1 = rs.getBigDecimal(column);
        int i1 = rs.getInt(column);
        if (bd == null) {
            trace("should be: null");
            check(rs.wasNull());
        } else {
            trace("BigDecimal i=" + i + " bd=" + bd + " ; i1=" + i1 + " bd1=" + bd1);
            check(!rs.wasNull());
            check(i1 == i);
            check(bd1.compareTo(new BigDecimal(bd)) == 0);
        }
    }

}
