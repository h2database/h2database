/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import org.h2.api.ErrorCode;
import org.h2.api.TimestampWithTimeZone;
import org.h2.engine.SysProperties;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.util.DateTimeUtils;
import org.h2.util.JSR310;

/**
 * Updatable result set tests.
 */
public class TestUpdatableResultSet extends TestDb {

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
        testDetectUpdatable();
        testUpdateLob();
        testScroll();
        testUpdateDeleteInsert();
        testUpdateDataType();
        testUpdateResetRead();
        deleteDb("updatableResultSet");
    }

    private void testDetectUpdatable() throws SQLException {
        deleteDb("updatableResultSet");
        Connection conn = getConnection("updatableResultSet");
        Statement stat;
        ResultSet rs;
        stat = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

        stat.execute("create table test(id int primary key, name varchar)");
        rs = stat.executeQuery("select * from test");
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs = stat.executeQuery("select name from test");
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        stat.execute("drop table test");

        stat.execute("create table test(a int, b int, " +
                "name varchar, primary key(a, b))");
        rs = stat.executeQuery("select * from test");
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs = stat.executeQuery("select a, name from test");
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        rs = stat.executeQuery("select b, name from test");
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        rs = stat.executeQuery("select b, name, a from test");
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        stat.execute("drop table test");

        stat.execute("create table test(a int, b int, name varchar)");
        stat.execute("create unique index on test(b, a)");
        rs = stat.executeQuery("select * from test");
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs = stat.executeQuery("select a, name from test");
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        rs = stat.executeQuery("select b, name from test");
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        rs = stat.executeQuery("select b, name, a from test");
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        stat.execute("drop table test");

        stat.execute("create table test(a int, b int, c int unique, " +
                "name varchar, primary key(a, b))");
        rs = stat.executeQuery("select * from test");
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs = stat.executeQuery("select a, name, c from test");
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs = stat.executeQuery("select b, a, name, c from test");
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        stat.execute("drop table test");

        stat.execute("create table test(id int primary key, " +
                "a int, b int, i int, j int, k int, name varchar)");
        stat.execute("create unique index on test(b, a)");
        stat.execute("create unique index on test(i, j)");
        stat.execute("create unique index on test(a, j)");
        rs = stat.executeQuery("select * from test");
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs = stat.executeQuery("select a, name, b from test");
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs = stat.executeQuery("select a, name, b from test");
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs = stat.executeQuery("select i, b, k, name from test");
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        rs = stat.executeQuery("select a, i, name from test");
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        rs = stat.executeQuery("select b, i, k, name from test");
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        rs = stat.executeQuery("select a, k, j, name from test");
        assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        stat.execute("drop table test");

        conn.close();
    }

    private void testUpdateLob() throws SQLException {
        deleteDb("updatableResultSet");
        Connection conn = getConnection("updatableResultSet");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE object_index " +
                "(id integer primary key, object other, number integer)");

        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO object_index (id,object)  VALUES (1,?)");
        prep.setObject(1, "hello", Types.JAVA_OBJECT);
        prep.execute();

        ResultSet rs = stat.executeQuery(
                "SELECT object,id,number FROM object_index WHERE id =1");
        rs.next();
        assertEquals("hello", rs.getObject(1).toString());
        stat = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);
        rs = stat.executeQuery("SELECT object,id,number FROM object_index WHERE id =1");
        rs.next();
        assertEquals("hello", rs.getObject(1).toString());
        rs.updateInt(2, 1);
        rs.updateRow();
        rs.close();
        stat = conn.createStatement();
        rs = stat.executeQuery("SELECT object,id,number FROM object_index WHERE id =1");
        rs.next();
        assertEquals("hello", rs.getObject(1).toString());
        conn.close();
    }

    private void testUpdateResetRead() throws SQLException {
        deleteDb("updatableResultSet");
        Connection conn = getConnection("updatableResultSet");
        Statement stat = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        stat.execute("INSERT INTO TEST VALUES(2, 'World')");
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        rs.updateInt(1, 10);
        rs.updateRow();
        rs.next();

        rs.updateString(2, "Welt");
        rs.cancelRowUpdates();
        rs.updateString(2, "Welt");

        rs.updateRow();
        rs.beforeFirst();
        rs.next();
        assertEquals(10, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals("Welt", rs.getString(2));

        assertFalse(rs.isClosed());
        rs.close();
        assertTrue(rs.isClosed());

        conn.close();
    }

    private void testScroll() throws SQLException {
        deleteDb("updatableResultSet");
        Connection conn = getConnection("updatableResultSet");
        Statement stat = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World'), (3, 'Test')");

        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");

        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertEquals(0, rs.getRow());

        rs.next();
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getRow());

        rs.next();
        assertThrows(ErrorCode.RESULT_SET_READONLY, rs).insertRow();
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertEquals(2, rs.getInt(1));
        assertEquals(2, rs.getRow());

        rs.next();
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        assertEquals(3, rs.getInt(1));
        assertEquals(3, rs.getRow());

        assertFalse(rs.next());
        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        assertEquals(0, rs.getRow());

        assertTrue(rs.first());
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getRow());

        assertTrue(rs.last());
        assertEquals(3, rs.getInt(1));
        assertEquals(3, rs.getRow());

        assertTrue(rs.relative(0));
        assertEquals(3, rs.getRow());

        assertTrue(rs.relative(-1));
        assertEquals(2, rs.getRow());

        assertTrue(rs.relative(1));
        assertEquals(3, rs.getRow());

        assertFalse(rs.relative(100));
        assertTrue(rs.isAfterLast());

        assertFalse(rs.absolute(0));
        assertEquals(0, rs.getRow());

        assertTrue(rs.absolute(1));
        assertEquals(1, rs.getRow());

        assertTrue(rs.absolute(2));
        assertEquals(2, rs.getRow());

        assertTrue(rs.absolute(3));
        assertEquals(3, rs.getRow());

        assertFalse(rs.absolute(4));
        assertEquals(0, rs.getRow());

        // allowed for compatibility
        assertFalse(rs.absolute(0));

        assertTrue(rs.absolute(3));
        assertEquals(3, rs.getRow());

        if (!config.lazy) {
            assertTrue(rs.absolute(-1));
            assertEquals(3, rs.getRow());

            assertTrue(rs.absolute(-2));
            assertEquals(2, rs.getRow());
        }

        assertFalse(rs.absolute(4));
        assertTrue(rs.isAfterLast());

        assertFalse(rs.absolute(5));
        assertTrue(rs.isAfterLast());

        assertTrue(rs.previous());
        assertEquals(3, rs.getRow());

        assertTrue(rs.previous());
        assertEquals(2, rs.getRow());

        conn.close();
    }

    private void testUpdateDataType() throws Exception {
        deleteDb("updatableResultSet");
        Connection conn = getConnection("updatableResultSet");
        Statement stat = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255), "
                + "DEC DECIMAL(10,2), BOO BIT, BYE TINYINT, BIN BINARY(100), "
                + "D DATE, T TIME, TS TIMESTAMP(9), TSTZ TIMESTAMP(9) WITH TIME ZONE, DB DOUBLE, R REAL, L BIGINT, "
                + "O_I INT, SH SMALLINT, CL CLOB, BL BLOB)");
        final int clobIndex = 16, blobIndex = 17;
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        ResultSetMetaData meta = rs.getMetaData();
        int c = 0;
        assertEquals("java.lang.Integer", meta.getColumnClassName(++c));
        assertEquals("java.lang.String", meta.getColumnClassName(++c));
        assertEquals("java.math.BigDecimal", meta.getColumnClassName(++c));
        assertEquals("java.lang.Boolean", meta.getColumnClassName(++c));
        assertEquals(SysProperties.OLD_RESULT_SET_GET_OBJECT ? "java.lang.Byte" : "java.lang.Integer",
                meta.getColumnClassName(++c));
        assertEquals("[B", meta.getColumnClassName(++c));
        assertEquals("java.sql.Date", meta.getColumnClassName(++c));
        assertEquals("java.sql.Time", meta.getColumnClassName(++c));
        assertEquals("java.sql.Timestamp", meta.getColumnClassName(++c));
        assertEquals(SysProperties.RETURN_OFFSET_DATE_TIME && JSR310.PRESENT //
                ? "java.time.OffsetDateTime" : "org.h2.api.TimestampWithTimeZone", //
                meta.getColumnClassName(++c));
        assertEquals("java.lang.Double", meta.getColumnClassName(++c));
        assertEquals("java.lang.Float", meta.getColumnClassName(++c));
        assertEquals("java.lang.Long", meta.getColumnClassName(++c));
        assertEquals("java.lang.Integer", meta.getColumnClassName(++c));
        assertEquals(SysProperties.OLD_RESULT_SET_GET_OBJECT ? "java.lang.Short" : "java.lang.Integer",
                meta.getColumnClassName(++c));
        assertEquals("java.sql.Clob", meta.getColumnClassName(++c));
        assertEquals("java.sql.Blob", meta.getColumnClassName(++c));
        rs.moveToInsertRow();
        rs.updateInt(1, 0);
        rs.updateNull(2);
        rs.updateNull("DEC");
        // 'not set' values are set to null
        assertThrows(ErrorCode.NO_DATA_AVAILABLE, rs).cancelRowUpdates();
        rs.insertRow();

        rs.moveToInsertRow();
        c = 0;
        rs.updateInt(++c, 1);
        rs.updateString(++c, null);
        rs.updateBigDecimal(++c, null);
        rs.updateBoolean(++c, false);
        rs.updateByte(++c, (byte) 0);
        rs.updateBytes(++c, null);
        rs.updateDate(++c, null);
        rs.updateTime(++c, null);
        rs.updateTimestamp(++c, null);
        rs.updateObject(++c, null);
        rs.updateDouble(++c, 0.0);
        rs.updateFloat(++c, 0.0f);
        rs.updateLong(++c, 0L);
        rs.updateObject(++c, null);
        rs.updateShort(++c, (short) 0);
        rs.updateCharacterStream(++c, new StringReader("test"), 0);
        rs.updateBinaryStream(++c,
                new ByteArrayInputStream(new byte[] { (byte) 0xff, 0x00 }), 0);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 2);
        rs.updateString("NAME", "+");
        rs.updateBigDecimal("DEC", new BigDecimal("1.2"));
        rs.updateBoolean("BOO", true);
        rs.updateByte("BYE", (byte) 0xff);
        rs.updateBytes("BIN", new byte[] { 0x00, (byte) 0xff });
        rs.updateDate("D", Date.valueOf("2005-09-21"));
        rs.updateTime("T", Time.valueOf("21:46:28"));
        rs.updateTimestamp("TS",
                Timestamp.valueOf("2005-09-21 21:47:09.567890123"));
        rs.updateObject("TSTZ",
                new TimestampWithTimeZone(DateTimeUtils.dateValue(2005, 9, 21), 81_189_123_456_789L, 60 * 60));
        rs.updateDouble("DB", 1.725);
        rs.updateFloat("R", 2.5f);
        rs.updateLong("L", Long.MAX_VALUE);
        rs.updateObject("O_I", 10);
        rs.updateShort("SH", Short.MIN_VALUE);
        // auml, ouml, uuml
        rs.updateCharacterStream("CL", new StringReader("\u00ef\u00f6\u00fc"), 0);
        rs.updateBinaryStream("BL",
                new ByteArrayInputStream(new byte[] { (byte) 0xab, 0x12 }), 0);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 3);
        rs.updateCharacterStream("CL", new StringReader("\u00ef\u00f6\u00fc"));
        rs.updateBinaryStream("BL",
                new ByteArrayInputStream(new byte[] { (byte) 0xab, 0x12 }));
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 4);
        rs.updateCharacterStream(clobIndex, new StringReader("\u00ef\u00f6\u00fc"));
        rs.updateBinaryStream(blobIndex,
                new ByteArrayInputStream(new byte[] { (byte) 0xab, 0x12 }));
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 5);
        rs.updateClob("CL", new StringReader("\u00ef\u00f6\u00fc"));
        rs.updateBlob("BL",
                new ByteArrayInputStream(new byte[] { (byte) 0xab, 0x12 }));
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 6);
        rs.updateClob(clobIndex, new StringReader("\u00ef\u00f6\u00fc"));
        rs.updateBlob(blobIndex,
                new ByteArrayInputStream(new byte[] { (byte) 0xab, 0x12 }));
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 7);
        rs.updateNClob("CL", new StringReader("\u00ef\u00f6\u00fc"));
        Blob b = conn.createBlob();
        OutputStream out = b.setBinaryStream(1);
        out.write(new byte[] { (byte) 0xab, 0x12 });
        out.close();
        rs.updateBlob("BL", b);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 8);
        rs.updateNClob(clobIndex, new StringReader("\u00ef\u00f6\u00fc"));
        rs.updateBlob(blobIndex, b);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 9);
        rs.updateNClob("CL", new StringReader("\u00ef\u00f6\u00fc"), -1);
        rs.updateBlob("BL", b);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 10);
        rs.updateNClob(clobIndex, new StringReader("\u00ef\u00f6\u00fc"), -1);
        rs.updateBlob(blobIndex, b);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 11);
        rs.updateNCharacterStream("CL",
                new StringReader("\u00ef\u00f6\u00fc"), -1);
        rs.updateBlob("BL", b);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 12);
        rs.updateNCharacterStream(clobIndex,
                new StringReader("\u00ef\u00f6\u00fc"), -1);
        rs.updateBlob(blobIndex, b);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 13);
        rs.updateNCharacterStream("CL",
                new StringReader("\u00ef\u00f6\u00fc"));
        rs.updateBlob("BL", b);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 14);
        rs.updateNCharacterStream(clobIndex,
                new StringReader("\u00ef\u00f6\u00fc"));
        rs.updateBlob(blobIndex, b);
        rs.insertRow();

        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID NULLS FIRST");
        rs.next();
        c = 0;
        assertTrue(rs.getInt(++c) == 0);
        assertTrue(rs.getString(++c) == null && rs.wasNull());
        assertTrue(rs.getBigDecimal(++c) == null && rs.wasNull());
        assertTrue(!rs.getBoolean(++c) && rs.wasNull());
        assertTrue(rs.getByte(++c) == 0 && rs.wasNull());
        assertTrue(rs.getBytes(++c) == null && rs.wasNull());
        assertTrue(rs.getDate(++c) == null && rs.wasNull());
        assertTrue(rs.getTime(++c) == null && rs.wasNull());
        assertTrue(rs.getTimestamp(++c) == null && rs.wasNull());
        assertTrue(rs.getDouble(++c) == 0.0 && rs.wasNull());
        assertTrue(rs.getFloat(++c) == 0.0 && rs.wasNull());
        assertTrue(rs.getLong(++c) == 0 && rs.wasNull());
        assertTrue(rs.getObject(++c) == null && rs.wasNull());
        assertTrue(rs.getShort(++c) == 0 && rs.wasNull());
        assertTrue(rs.getCharacterStream(++c) == null && rs.wasNull());
        assertTrue(rs.getBinaryStream(++c) == null && rs.wasNull());

        rs.next();
        c = 0;
        assertTrue(rs.getInt(++c) == 1);
        assertTrue(rs.getString(++c) == null && rs.wasNull());
        assertTrue(rs.getBigDecimal(++c) == null && rs.wasNull());
        assertTrue(!rs.getBoolean(++c) && !rs.wasNull());
        assertTrue(rs.getByte(++c) == 0 && !rs.wasNull());
        assertTrue(rs.getBytes(++c) == null && rs.wasNull());
        assertTrue(rs.getDate(++c) == null && rs.wasNull());
        assertTrue(rs.getTime(++c) == null && rs.wasNull());
        assertTrue(rs.getTimestamp(++c) == null && rs.wasNull());
        assertTrue(rs.getObject(++c) == null && rs.wasNull());
        assertTrue(rs.getDouble(++c) == 0.0 && !rs.wasNull());
        assertTrue(rs.getFloat(++c) == 0.0 && !rs.wasNull());
        assertTrue(rs.getLong(++c) == 0 && !rs.wasNull());
        assertTrue(rs.getObject(++c) == null && rs.wasNull());
        assertTrue(rs.getShort(++c) == 0 && !rs.wasNull());
        assertEquals("test", rs.getString(++c));
        assertEquals(new byte[] { (byte) 0xff, 0x00 }, rs.getBytes(++c));

        rs.next();
        c = 0;
        assertTrue(rs.getInt(++c) == 2);
        assertEquals("+", rs.getString(++c));
        assertEquals("1.20", rs.getBigDecimal(++c).toString());
        assertTrue(rs.getBoolean(++c));
        assertTrue((rs.getByte(++c) & 0xff) == 0xff);
        assertEquals(new byte[] { 0x00, (byte) 0xff }, rs.getBytes(++c));
        assertEquals("2005-09-21", rs.getDate(++c).toString());
        assertEquals("21:46:28", rs.getTime(++c).toString());
        assertEquals("2005-09-21 21:47:09.567890123", rs.getTimestamp(++c).toString());
        assertEquals(SysProperties.RETURN_OFFSET_DATE_TIME && JSR310.PRESENT //
                ? "2005-09-21T22:33:09.123456789+01:00" : "2005-09-21 22:33:09.123456789+01", //
                rs.getObject(++c).toString());
        assertTrue(rs.getDouble(++c) == 1.725);
        assertTrue(rs.getFloat(++c) == 2.5f);
        assertTrue(rs.getLong(++c) == Long.MAX_VALUE);
        assertEquals(10, ((Integer) rs.getObject(++c)).intValue());
        assertTrue(rs.getShort(++c) == Short.MIN_VALUE);
        // auml ouml uuml
        assertEquals("\u00ef\u00f6\u00fc", rs.getString(++c));
        assertEquals(new byte[] { (byte) 0xab, 0x12 }, rs.getBytes(++c));
        c = 1;
        rs.updateString(++c, "-");
        rs.updateBigDecimal(++c, new BigDecimal("1.30"));
        rs.updateBoolean(++c, false);
        rs.updateByte(++c, (byte) 0x55);
        rs.updateBytes(++c, new byte[] { 0x01, (byte) 0xfe });
        rs.updateDate(++c, Date.valueOf("2005-09-22"));
        rs.updateTime(++c, Time.valueOf("21:46:29"));
        rs.updateTimestamp(++c, Timestamp.valueOf("2005-09-21 21:47:10.111222333"));
        rs.updateObject(++c, new TimestampWithTimeZone(DateTimeUtils.dateValue(2005, 9, 22), 10_111_222_333L,
                2 * 60 * 60));
        rs.updateDouble(++c, 2.25);
        rs.updateFloat(++c, 3.5f);
        rs.updateLong(++c, Long.MAX_VALUE - 1);
        rs.updateInt(++c, 11);
        rs.updateShort(++c, (short) -1_000);
        rs.updateString(++c, "ABCD");
        rs.updateBytes(++c, new byte[] { 1, 2 });
        rs.updateRow();

        for (int i = 3; i <= 14; i++) {
            rs.next();
            assertEquals(i, rs.getInt(1));
            assertEquals("\u00ef\u00f6\u00fc", rs.getString(clobIndex));
            assertEquals(new byte[] { (byte) 0xab, 0x12 }, rs.getBytes(blobIndex));
        }
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM TEST WHERE ID = 2");
        rs.next();
        c = 0;
        assertTrue(rs.getInt(++c) == 2);
        assertEquals("-", rs.getString(++c));
        assertEquals("1.30", rs.getBigDecimal(++c).toString());
        assertFalse(rs.getBoolean(++c));
        assertTrue((rs.getByte(++c) & 0xff) == 0x55);
        assertEquals(new byte[] { 0x01, (byte) 0xfe }, rs.getBytes(++c));
        assertEquals("2005-09-22", rs.getDate(++c).toString());
        assertEquals("21:46:29", rs.getTime(++c).toString());
        assertEquals("2005-09-21 21:47:10.111222333", rs.getTimestamp(++c).toString());
        assertEquals(SysProperties.RETURN_OFFSET_DATE_TIME && JSR310.PRESENT //
                ? "2005-09-22T00:00:10.111222333+02:00" : "2005-09-22 00:00:10.111222333+02", //
                rs.getObject(++c).toString());
        assertTrue(rs.getDouble(++c) == 2.25);
        assertTrue(rs.getFloat(++c) == 3.5f);
        assertTrue(rs.getLong(++c) == Long.MAX_VALUE - 1);
        assertEquals(11, ((Integer) rs.getObject(++c)).intValue());
        assertTrue(rs.getShort(++c) == -1_000);
        assertEquals("ABCD", rs.getString(++c));
        assertEquals(new byte[] { 1, 2 }, rs.getBytes(++c));
        assertFalse(rs.next());

        stat.execute("DROP TABLE TEST");
        conn.close();
    }

    private void testUpdateDeleteInsert() throws SQLException {
        deleteDb("updatableResultSet");
        Connection c1 = getConnection("updatableResultSet");
        Connection c2 = getConnection("updatableResultSet");
        Statement stat = c1.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        int max = 8;
        for (int i = 0; i < max; i++) {
            stat.execute("INSERT INTO TEST VALUES(" + i + ", 'Hello" + i + "')");
        }
        ResultSet rs;
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        assertEquals(0, rs.getInt(1));
        rs.moveToInsertRow();
        rs.updateInt(1, 100);
        rs.moveToCurrentRow();
        assertEquals(0, rs.getInt(1));

        rs = stat.executeQuery("SELECT * FROM TEST");
        int j = max;
        while (rs.next()) {
            int id = rs.getInt(1);
            if (id % 2 == 0) {
                Statement s2 = c2.createStatement();
                s2.execute("UPDATE TEST SET NAME = NAME || '+' WHERE ID = " + rs.getInt(1));
                if (id % 4 == 0) {
                    rs.refreshRow();
                }
                rs.updateString(2, "Updated " + rs.getString(2));
                rs.updateRow();
            } else {
                rs.deleteRow();
            }
            // the driver does not detect it in any case
            assertFalse(rs.rowUpdated());
            assertFalse(rs.rowInserted());
            assertFalse(rs.rowDeleted());

            rs.moveToInsertRow();
            rs.updateString(2, "Inserted " + j);
            rs.updateInt(1, j);
            j += 2;
            rs.insertRow();

            // the driver does not detect it in any case
            assertFalse(rs.rowUpdated());
            assertFalse(rs.rowInserted());
            assertFalse(rs.rowDeleted());

        }
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        while (rs.next()) {
            int id = rs.getInt(1);
            String name = rs.getString(2);
            assertEquals(0, id % 2);
            if (id >= max) {
                assertEquals("Inserted " + id, rs.getString(2));
            } else {
                if (id % 4 == 0) {
                    assertEquals("Updated Hello" + id + "+", rs.getString(2));
                } else {
                    assertEquals("Updated Hello" + id, rs.getString(2));
                }
            }
            trace("id=" + id + " name=" + name);
        }
        c2.close();
        c1.close();

        // test scrollable result sets
        Connection conn = getConnection("updatableResultSet");
        for (int i = 0; i < 5; i++) {
            testScrollable(conn, i);
        }
        conn.close();
    }

    private void testScrollable(Connection conn, int rows) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE IF NOT EXISTS TEST" +
                "(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("DELETE FROM TEST");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?)");
        for (int i = 0; i < rows; i++) {
            prep.setInt(1, i);
            prep.setString(2, "Data " + i);
            prep.execute();
        }
        Statement regular = conn.createStatement();
        testScrollResultSet(regular, ResultSet.TYPE_FORWARD_ONLY, rows);
        Statement scroll = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        testScrollResultSet(scroll, ResultSet.TYPE_SCROLL_INSENSITIVE, rows);
    }

    private void testScrollResultSet(Statement stat, int type, int rows)
            throws SQLException {
        boolean error = false;
        if (type == ResultSet.TYPE_FORWARD_ONLY) {
            error = true;
        }
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        assertEquals(type, rs.getType());

        assertState(rs, rows > 0, false, false, false);
        for (int i = 0; i < rows; i++) {
            rs.next();
            assertState(rs, rows == 0, i == 0, i == rows - 1, rows == 0 || i == rows);
        }
        try {
            rs.beforeFirst();
            assertState(rs, rows > 0, false, false, false);
        } catch (SQLException e) {
            if (!error) {
                throw e;
            }
        }
        try {
            rs.afterLast();
            assertState(rs, false, false, false, rows > 0);
        } catch (SQLException e) {
            if (!error) {
                throw e;
            }
        }
        try {
            boolean valid = rs.first();
            assertEquals(rows > 0, valid);
            if (valid) {
                assertState(rs, false, true, rows == 1, rows == 0);
            }
        } catch (SQLException e) {
            if (!error) {
                throw e;
            }
        }
        try {
            boolean valid = rs.last();
            assertEquals(rows > 0, valid);
            if (valid) {
                assertState(rs, false, rows == 1, true, rows == 0);
            }
        } catch (SQLException e) {
            if (!error) {
                throw e;
            }
        }
    }

    private void assertState(ResultSet rs, boolean beforeFirst,
            boolean first, boolean last, boolean afterLast) throws SQLException {
        assertEquals(beforeFirst, rs.isBeforeFirst());
        assertEquals(first, rs.isFirst());
        assertEquals(last, rs.isLast());
        assertEquals(afterLast, rs.isAfterLast());
    }

}
