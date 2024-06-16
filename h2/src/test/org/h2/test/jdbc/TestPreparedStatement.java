/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.URL;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.UUID;

import org.h2.api.ErrorCode;
import org.h2.api.H2Type;
import org.h2.api.Interval;
import org.h2.api.IntervalQualifier;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.util.Task;

/**
 * Tests for the PreparedStatement implementation.
 */
public class TestPreparedStatement extends TestDb {

    private static final int LOB_SIZE = 4000, LOB_SIZE_BIG = 512 * 1024;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        deleteDb("preparedStatement");
        Connection conn = getConnection("preparedStatement");
        testUnwrap(conn);
        testUnsupportedOperations(conn);
        testChangeType(conn);
        testCallTablePrepared(conn);
        testValues(conn);
        testToString(conn);
        testExecuteUpdateCall(conn);
        testPrepareExecute(conn);
        testEnum(conn);
        testUUID(conn);
        testUUIDAsJavaObject(conn);
        testLobTempFiles(conn);
        testExecuteErrorTwice(conn);
        testTempView(conn);
        testInsertFunction(conn);
        testPrepareRecompile(conn);
        testMaxRowsChange(conn);
        testUnknownDataType(conn);
        testCancelReuse(conn);
        testCoalesce(conn);
        testPreparedStatementMetaData(conn);
        testBigDecimal(conn);
        testDate(conn);
        testDate8(conn);
        testTime8(conn);
        testOffsetTime8(conn);
        testDateTime8(conn);
        testOffsetDateTime8(conn);
        testZonedDateTime8(conn);
        testInstant8(conn);
        testInterval(conn);
        testInterval8(conn);
        testJson(conn);
        testArray(conn);
        testSetObject(conn);
        testSetObject2(conn);
        testPreparedSubquery(conn);
        testLikeIndex(conn);
        testCasewhen(conn);
        testSubquery(conn);
        testObject(conn);
        testDataTypes(conn);
        testGetMoreResults(conn);
        testBlob(conn);
        testClob(conn);
        testParameterMetaData(conn);
        testColumnMetaDataWithEquals(conn);
        testColumnMetaDataWithIn(conn);
        testMultipleStatements(conn);
        testParameterInSubquery(conn);
        testAfterRollback(conn);
        testUnnestWithArrayParameter(conn);
        conn.close();
        testPreparedStatementWithLiteralsNone();
        testPreparedStatementWithIndexedParameterAndLiteralsNone();
        testPreparedStatementWithAnyParameter();
        deleteDb("preparedStatement");
    }

    private void testUnwrap(Connection conn) throws SQLException {
        assertTrue(conn.isWrapperFor(Object.class));
        assertTrue(conn.isWrapperFor(Connection.class));
        assertTrue(conn.isWrapperFor(conn.getClass()));
        assertFalse(conn.isWrapperFor(String.class));
        assertTrue(conn == conn.unwrap(Object.class));
        assertTrue(conn == conn.unwrap(Connection.class));
        assertThrows(ErrorCode.INVALID_VALUE_2, conn).
                unwrap(String.class);
    }

    @SuppressWarnings("deprecation")
    private void testUnsupportedOperations(Connection conn) throws Exception {
        PreparedStatement prep = conn.prepareStatement("select ? from dual");
        assertThrows(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT, prep).
            addBatch("select 1");

        assertThrows(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT, prep).
            executeUpdate("create table test(id int)");
        assertThrows(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT, prep).
            executeUpdate("create table test(id int)", new int[0]);
        assertThrows(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT, prep).
            executeUpdate("create table test(id int)", new String[0]);
        assertThrows(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT, prep).
            executeUpdate("create table test(id int)", Statement.RETURN_GENERATED_KEYS);

        assertThrows(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT, prep).
            execute("create table test(id int)");
        assertThrows(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT, prep).
            execute("create table test(id int)", new int[0]);
        assertThrows(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT, prep).
            execute("create table test(id int)", new String[0]);
        assertThrows(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT, prep).
            execute("create table test(id int)", Statement.RETURN_GENERATED_KEYS);

        assertThrows(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT, prep).
            executeQuery("select * from dual");

        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, prep).
            setURL(1, new URL("http://www.acme.com"));
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, prep).
            setRowId(1, (RowId) null);
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, prep).
            setUnicodeStream(1, (InputStream) null, 0);

        ParameterMetaData meta = prep.getParameterMetaData();
        assertTrue(meta.toString(), meta.toString().endsWith("parameterCount=1"));
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, conn).
                createStruct("Integer", new Object[0]);
    }

    private static void testChangeType(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "select (? || ? || ?) from dual");
        prep.setString(1, "a");
        prep.setString(2, "b");
        prep.setString(3, "c");
        prep.executeQuery();
        prep.setInt(1, 1);
        prep.setString(2, "ab");
        prep.setInt(3, 45);
        prep.executeQuery();
    }

    private static void testCallTablePrepared(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("select * from table(x int = (1))");
        prep.executeQuery();
        prep.executeQuery();
    }

    private void testValues(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("values(?, ?)");
        prep.setInt(1, 1);
        prep.setString(2, "Hello");
        ResultSet rs = prep.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));

        prep = conn.prepareStatement("select * from values(?, ?), (2, 'World!')");
        prep.setInt(1, 1);
        prep.setString(2, "Hello");
        rs = prep.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals("World!", rs.getString(2));

        prep = conn.prepareStatement("values 1, 2");
        rs = prep.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs.next();
        assertEquals(2, rs.getInt(1));
    }

    private void testToString(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("call 1");
        assertTrue(prep.toString().endsWith(": call 1"));
        prep = conn.prepareStatement("call ?");
        assertTrue(prep.toString().endsWith(": call ?"));
        prep.setString(1, "Hello World");
        assertTrue(prep.toString().endsWith(": call ? {1: 'Hello World'}"));
    }

    private void testExecuteUpdateCall(Connection conn) throws SQLException {
        assertThrows(ErrorCode.DATA_CONVERSION_ERROR_1, conn.createStatement()).
                executeUpdate("CALL HASH('SHA256', STRINGTOUTF8('Password'), 1000)");
    }

    private void testPrepareExecute(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("prepare test(int, int) as select ?1*?2");
        ResultSet rs = stat.executeQuery("execute test(3, 2)");
        rs.next();
        assertEquals(6, rs.getInt(1));
        stat.execute("deallocate test");
    }

    private void testLobTempFiles(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, DATA CLOB)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?)");
        for (int i = 0; i < 5; i++) {
            prep.setInt(1, i);
            if (i % 2 == 0) {
                prep.setCharacterStream(2, new StringReader(getString(i)), -1);
            }
            prep.execute();
        }
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        int check = 0;
        for (int i = 0; i < 5; i++) {
            assertTrue(rs.next());
            if (i % 2 == 0) {
                check = i;
            }
            assertEquals(getString(check), rs.getString(2));
        }
        assertFalse(rs.next());
        stat.execute("DELETE FROM TEST");
        for (int i = 0; i < 3; i++) {
            prep.setInt(1, i);
            prep.setCharacterStream(2, new StringReader(getString(i)), -1);
            prep.addBatch();
        }
        prep.executeBatch();
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        for (int i = 0; i < 3; i++) {
            assertTrue(rs.next());
            assertEquals(getString(i), rs.getString(2));
        }
        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private static String getString(int i) {
        return new String(new char[100000]).replace('\0', (char) ('0' + i));
    }

    private void testExecuteErrorTwice(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "CREATE TABLE BAD AS SELECT A");
        assertThrows(ErrorCode.COLUMN_NOT_FOUND_1, prep).execute();
        assertThrows(ErrorCode.COLUMN_NOT_FOUND_1, prep).execute();
    }

    private void testTempView(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        stat.execute("CREATE TABLE TEST(FIELD INT PRIMARY KEY)");
        stat.execute("INSERT INTO TEST VALUES(1)");
        stat.execute("INSERT INTO TEST VALUES(2)");
        prep = conn.prepareStatement("select FIELD FROM "
                + "(select FIELD FROM (SELECT FIELD  FROM TEST "
                + "WHERE FIELD = ?) AS T2 "
                + "WHERE T2.FIELD = ?) AS T3 WHERE T3.FIELD = ?");
        prep.setInt(1, 1);
        prep.setInt(2, 1);
        prep.setInt(3, 1);
        ResultSet rs = prep.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        prep.setInt(1, 2);
        prep.setInt(2, 2);
        prep.setInt(3, 2);
        rs = prep.executeQuery();
        rs.next();
        assertEquals(2, rs.getInt(1));
        stat.execute("DROP TABLE TEST");
    }

    private void testInsertFunction(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        ResultSet rs;

        stat.execute("CREATE TABLE TEST(ID INT, H VARBINARY)");
        prep = conn.prepareStatement("INSERT INTO TEST " +
                "VALUES(?, HASH('SHA256', STRINGTOUTF8(?), 5))");
        prep.setInt(1, 1);
        prep.setString(2, "One");
        prep.execute();
        prep.setInt(1, 2);
        prep.setString(2, "Two");
        prep.execute();
        rs = stat.executeQuery("SELECT COUNT(DISTINCT H) FROM TEST");
        rs.next();
        assertEquals(2, rs.getInt(1));

        stat.execute("DROP TABLE TEST");
    }

    private void testPrepareRecompile(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        ResultSet rs;

        prep = conn.prepareStatement("SELECT COUNT(*) " +
                "FROM DUAL WHERE ? IS NULL");
        prep.setString(1, null);
        prep.executeQuery();
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("DROP TABLE TEST");
        prep.setString(1, null);
        prep.executeQuery();
        prep.setString(1, "X");
        rs = prep.executeQuery();
        rs.next();
        assertEquals(0, rs.getInt(1));

        stat.execute("CREATE TABLE t1 (c1 INT, c2 VARCHAR(10))");
        stat.execute("INSERT INTO t1 SELECT X, CONCAT('Test', X)  " +
                "FROM SYSTEM_RANGE(1, 5);");
        prep = conn.prepareStatement("SELECT c1, c2 FROM t1 WHERE c1 = ?");
        prep.setInt(1, 1);
        prep.executeQuery();
        stat.execute("CREATE TABLE t2 (x int PRIMARY KEY)");
        prep.setInt(1, 2);
        rs = prep.executeQuery();
        rs.next();
        assertEquals(2, rs.getInt(1));
        prep.setInt(1, 3);
        rs = prep.executeQuery();
        rs.next();
        assertEquals(3, rs.getInt(1));
        stat.execute("DROP TABLE t1, t2");

    }

    private void testMaxRowsChange(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "SELECT * FROM SYSTEM_RANGE(1, 100)");
        ResultSet rs;
        for (int j = 1; j < 20; j++) {
            prep.setMaxRows(j);
            rs = prep.executeQuery();
            for (int i = 0; i < j; i++) {
                assertTrue(rs.next());
            }
            assertFalse(rs.next());
        }
    }

    private void testUnknownDataType(Connection conn) throws SQLException {
        assertThrows(ErrorCode.UNKNOWN_DATA_TYPE_1, conn).
            prepareStatement("SELECT * FROM (SELECT ? FROM DUAL)");
        assertThrows(ErrorCode.UNKNOWN_DATA_TYPE_1, conn).
            prepareStatement("VALUES BITAND(?, ?)");
        PreparedStatement prep = conn.prepareStatement("SELECT -?");
        prep.setInt(1, 1);
        execute(prep);
        prep = conn.prepareStatement("SELECT ?-?");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        execute(prep);
    }

    private void testCancelReuse(Connection conn) throws Exception {
        conn.createStatement().execute(
                "CREATE ALIAS SLEEP FOR 'java.lang.Thread.sleep(long)'");
        // sleep for 10 seconds
        final PreparedStatement prep = conn.prepareStatement(
                "SELECT SLEEP(?) FROM SYSTEM_RANGE(1, 10000) LIMIT ?");
        prep.setInt(1, 1);
        prep.setInt(2, 10000);
        Task t = new Task() {
            @Override
            public void call() throws SQLException {
                TestPreparedStatement.this.execute(prep);
            }
        };
        t.execute();
        Thread.sleep(100);
        prep.cancel();
        SQLException e = (SQLException) t.getException();
        assertNotNull(e);
        assertEquals(ErrorCode.STATEMENT_WAS_CANCELED, e.getErrorCode());
        prep.setInt(1, 1);
        prep.setInt(2, 1);
        ResultSet rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertFalse(rs.next());
    }

    private static void testCoalesce(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.executeUpdate("create table test(tm timestamp)");
        stat.executeUpdate("insert into test values(current_timestamp)");
        PreparedStatement prep = conn.prepareStatement(
                "update test set tm = coalesce(?,tm)");
        prep.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis()));
        prep.executeUpdate();
        stat.executeUpdate("drop table test");
    }

    private void testPreparedStatementMetaData(Connection conn)
            throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "select * from table(x int = ?, name varchar = ?)");
        ResultSetMetaData meta = prep.getMetaData();
        assertEquals(2, meta.getColumnCount());
        assertEquals("INTEGER", meta.getColumnTypeName(1));
        assertEquals("CHARACTER VARYING", meta.getColumnTypeName(2));
        prep = conn.prepareStatement("call 1");
        meta = prep.getMetaData();
        assertEquals(1, meta.getColumnCount());
        assertEquals("INTEGER", meta.getColumnTypeName(1));
        prep = conn.prepareStatement("SELECT * FROM UNNEST(ARRAY[1, 2])");
        meta = prep.getMetaData();
        assertEquals(1, meta.getColumnCount());
        assertEquals("INTEGER", meta.getColumnTypeName(1));
    }

    private void testArray(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "select * from table(x int = ?) order by x");
        prep.setObject(1, new Object[] { new BigDecimal("1"), "2" });
        ResultSet rs = prep.executeQuery();
        rs.next();
        assertEquals("1", rs.getString(1));
        rs.next();
        assertEquals("2", rs.getString(1));
        assertFalse(rs.next());
    }

    private void testEnum(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE test_enum(size ENUM('small', 'medium', 'large'))");

        String[] badSizes = new String[]{"green", "smol", "0"};
        for (int i = 0; i < badSizes.length; i++) {
            PreparedStatement prep = conn.prepareStatement(
                    "INSERT INTO test_enum VALUES(?)");
            prep.setObject(1, badSizes[i]);
            assertThrows(ErrorCode.ENUM_VALUE_NOT_PERMITTED, prep).execute();
        }

        String[] goodSizes = new String[]{"small", "medium", "large"};
        for (int i = 0; i < goodSizes.length; i++) {
            PreparedStatement prep = conn.prepareStatement(
                    "INSERT INTO test_enum VALUES(?)");
            prep.setObject(1, goodSizes[i]);
            prep.execute();
            ResultSet rs = stat.executeQuery("SELECT * FROM test_enum");
            for (int j = 0; j <= i; j++) {
                rs.next();
            }
            assertEquals(goodSizes[i], rs.getString(1));
            assertEquals(i + 1, rs.getInt(1));
            Object o = rs.getObject(1);
            assertEquals(String.class, o.getClass());
        }

        for (int i = 0; i < goodSizes.length; i++) {
            PreparedStatement prep = conn.prepareStatement("SELECT * FROM test_enum WHERE size = ?");
            prep.setObject(1, goodSizes[i]);
            ResultSet rs = prep.executeQuery();
            rs.next();
            String s = rs.getString(1);
            assertTrue(s.equals(goodSizes[i]));
            assertFalse(rs.next());
        }

        for (int i = 0; i < badSizes.length; i++) {
            PreparedStatement prep = conn.prepareStatement("SELECT * FROM test_enum WHERE size = ?");
            prep.setObject(1, badSizes[i]);
            if (config.lazy && !config.networked) {
                ResultSet resultSet = prep.executeQuery();
                assertThrows(ErrorCode.ENUM_VALUE_NOT_PERMITTED, resultSet).next();
            } else {
                assertThrows(ErrorCode.ENUM_VALUE_NOT_PERMITTED, prep).executeQuery();
            }
        }

        stat.execute("DROP TABLE test_enum");
    }

    private void testUUID(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("create table test_uuid(id uuid primary key)");
        UUID uuid = new UUID(-2, -1);
        PreparedStatement prep = conn.prepareStatement(
                "insert into test_uuid values(?)");
        prep.setObject(1, uuid);
        prep.execute();
        ResultSet rs = stat.executeQuery("select * from test_uuid");
        rs.next();
        assertEquals("ffffffff-ffff-fffe-ffff-ffffffffffff", rs.getString(1));
        Object o = rs.getObject(1);
        assertEquals("java.util.UUID", o.getClass().getName());
        stat.execute("drop table test_uuid");
    }

    private void testUUIDAsJavaObject(Connection conn) throws SQLException {
        String uuidStr = "12345678-1234-4321-8765-123456789012";

        Statement stat = conn.createStatement();
        stat.execute("create table test_uuid(id uuid primary key)");
        UUID origUUID = UUID.fromString(uuidStr);
        PreparedStatement prep = conn.prepareStatement("insert into test_uuid values(?)");
        prep.setObject(1, origUUID, java.sql.Types.JAVA_OBJECT);
        prep.execute();

        prep = conn.prepareStatement("select * from test_uuid where id=?");
        prep.setObject(1, origUUID, java.sql.Types.JAVA_OBJECT);
        ResultSet rs = prep.executeQuery();
        rs.next();
        Object o = rs.getObject(1);
        assertTrue(o instanceof UUID);
        UUID selectedUUID = (UUID) o;
        assertTrue(selectedUUID.toString().equals(uuidStr));
        assertTrue(selectedUUID.equals(origUUID));
        stat.execute("drop table test_uuid");
    }

    private void testSetObject(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(C CHAR(1))");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?)");
        prep.setObject(1, 'x');
        prep.execute();
        stat.execute("DROP TABLE TEST");
        stat.execute("CREATE TABLE TEST(ID INT, DATA VARBINARY, JAVA OTHER)");
        prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?, ?)");
        prep.setInt(1, 1);
        prep.setObject(2, 11);
        prep.setObject(3, null);
        prep.execute();
        prep.setInt(1, 2);
        prep.setObject(2, 101, Types.JAVA_OBJECT);
        prep.setObject(3, 103, Types.JAVA_OBJECT);
        prep.execute();
        PreparedStatement p2 = conn.prepareStatement(
                "SELECT * FROM TEST ORDER BY ID");
        ResultSet rs = p2.executeQuery();
        rs.next();
        Object o = rs.getObject(2);
        assertTrue(o instanceof byte[]);
        assertNull(rs.getObject(3));
        rs.next();
        o = rs.getObject(2);
        assertTrue(o instanceof byte[]);
        o = rs.getObject(3);
        assertTrue(o instanceof Integer);
        assertEquals(103, ((Integer) o).intValue());
        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private void testSetObject2(Connection conn) throws SQLException {
        try (PreparedStatement prep = conn.prepareStatement("VALUES (?1, ?1 IS OF(INTEGER), ?1 IS OF(BIGINT))")) {
            for (int i = 1; i <= 6; i++) {
                testSetObject2SetObjectType(prep, i, (long) i);
                try (ResultSet rs = prep.executeQuery()) {
                    rs.next();
                    // Parameters are converted to VARCHAR by a query
                    assertEquals(Integer.toString(i), rs.getString(1));
                    // Use the type predicate to check a real data type
                    if (i == 1) {
                        assertFalse(rs.getBoolean(2));
                        assertTrue(rs.getBoolean(3));
                    } else {
                        assertTrue(rs.getBoolean(2));
                        assertFalse(rs.getBoolean(3));
                    }
                }
                testSetObject2SetObjectType(prep, i, null);
                try (ResultSet rs = prep.executeQuery()) {
                    rs.next();
                    assertNull(rs.getObject(1));
                }
            }
            prep.setObject(1, 1);
        }
    }

    private static void testSetObject2SetObjectType(PreparedStatement prep, int method, Object value)
            throws SQLException {
        switch (method) {
        case 1:
            prep.setObject(1, value);
            break;
        case 2:
            prep.setObject(1, value, Types.INTEGER);
            break;
        case 3:
            prep.setObject(1, value, JDBCType.INTEGER);
            break;
        case 4:
            prep.setObject(1, value, Types.INTEGER, 0);
            break;
        case 5:
            prep.setObject(1, value, JDBCType.INTEGER, 0);
            break;
        case 6:
            prep.setObject(1, value, H2Type.INTEGER, 0);
        }
    }

    private void testBigDecimal(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT ?, ?");
        BigDecimal bd = new BigDecimal("12300").setScale(-2, RoundingMode.UNNECESSARY);
        prep.setBigDecimal(1, bd);
        prep.setObject(2, bd);
        ResultSet rs = prep.executeQuery();
        rs.next();
        bd = rs.getBigDecimal(1);
        assertEquals(12300, bd.intValue());
        assertEquals(0, bd.scale());
        bd = rs.getBigDecimal(2);
        assertEquals(12300, bd.intValue());
        assertEquals(0, bd.scale());
    }

    private void testDate(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT ?");
        Timestamp ts = Timestamp.valueOf("2001-02-03 04:05:06");
        prep.setObject(1, new java.util.Date(ts.getTime()));
        ResultSet rs = prep.executeQuery();
        rs.next();
        Timestamp ts2 = rs.getTimestamp(1);
        assertEquals(ts.toString(), ts2.toString());
    }

    private void testDate8(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT ?");
        LocalDate localDate = LocalDate.parse("2001-02-03");
        prep.setObject(1, localDate);
        ResultSet rs = prep.executeQuery();
        rs.next();
        LocalDate localDate2 = rs.getObject(1, LocalDate.class);
        assertEquals(localDate, localDate2);
        rs.close();
        localDate = LocalDate.parse("-0509-01-01");
        prep.setObject(1, localDate);
        rs = prep.executeQuery();
        rs.next();
        localDate2 = rs.getObject(1, LocalDate.class);
        assertEquals(localDate, localDate2);
        rs.close();
        prep.setString(1, "1500-02-28");
        rs = prep.executeQuery();
        rs.next();
        localDate2 = rs.getObject(1, LocalDate.class);
        assertEquals(LocalDate.parse("1500-02-28"), localDate2);
        rs.close();
        prep.setString(1, "-0100-02-28");
        rs = prep.executeQuery();
        rs.next();
        localDate2 = rs.getObject(1, LocalDate.class);
        assertEquals(LocalDate.parse("-0100-02-28"), localDate2);
        rs.close();
        /*
         * Test dates during Julian to Gregorian transition.
         *
         * java.util.TimeZone doesn't support LMT, so perform this test with
         * fixed time zone offset
         */
        Statement stat = conn.createStatement();
        stat.execute("SET TIME ZONE '1'");
        TimeZone old = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+01"));
        try {
            localDate = LocalDate.parse("1582-10-05");
            prep.setObject(1, localDate);
            rs = prep.executeQuery();
            rs.next();
            localDate2 = rs.getObject(1, LocalDate.class);
            assertEquals(localDate, localDate2);
            assertEquals("1582-10-05", rs.getString(1));
            assertEquals(Date.valueOf("1582-09-25"), rs.getDate(1));
            GregorianCalendar gc = new GregorianCalendar();
            gc.setGregorianChange(new java.util.Date(Long.MIN_VALUE));
            gc.clear();
            gc.set(Calendar.YEAR, 1582);
            gc.set(Calendar.MONTH, 9);
            gc.set(Calendar.DAY_OF_MONTH, 5);
            Date expected = new Date(gc.getTimeInMillis());
            gc.clear();
            assertEquals(expected, rs.getDate(1, gc));
            rs.close();
        } finally {
            stat.execute("SET TIME ZONE LOCAL");
            TimeZone.setDefault(old);
        }
    }

    private void testTime8(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT ?");
        LocalTime localTime = LocalTime.parse("04:05:06");
        prep.setObject(1, localTime);
        ResultSet rs = prep.executeQuery();
        rs.next();
        LocalTime localTime2 = rs.getObject(1, LocalTime.class);
        assertEquals(localTime, localTime2);
        rs.close();
        localTime = LocalTime.parse("04:05:06.123456789");
        prep.setObject(1, localTime);
        rs = prep.executeQuery();
        rs.next();
        localTime2 = rs.getObject(1, LocalTime.class);
        assertEquals(localTime, localTime2);
        rs.close();
    }

    private void testOffsetTime8(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT ?");
        OffsetTime offsetTime = OffsetTime.parse("04:05:06+02:30");
        prep.setObject(1, offsetTime);
        ResultSet rs = prep.executeQuery();
        rs.next();
        OffsetTime offsetTime2 = rs.getObject(1, OffsetTime.class);
        assertEquals(offsetTime, offsetTime2);
        assertFalse(rs.next());
        rs.close();

        prep.setObject(1, offsetTime, Types.TIME_WITH_TIMEZONE);
        rs = prep.executeQuery();
        rs.next();
        offsetTime2 = rs.getObject(1, OffsetTime.class);
        assertEquals(offsetTime, offsetTime2);
        assertFalse(rs.next());
        rs.close();
    }

    private void testDateTime8(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT ?");
        LocalDateTime localDateTime = LocalDateTime.parse("2001-02-03T04:05:06");
        prep.setObject(1, localDateTime);
        ResultSet rs = prep.executeQuery();
        rs.next();
        LocalDateTime localDateTime2 = rs.getObject(1, LocalDateTime.class);
        assertEquals(localDateTime, localDateTime2);
        rs.close();
    }

    private void testOffsetDateTime8(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT ?");
        OffsetDateTime offsetDateTime = OffsetDateTime.parse("2001-02-03T04:05:06+02:30");
        prep.setObject(1, offsetDateTime);
        ResultSet rs = prep.executeQuery();
        rs.next();
        OffsetDateTime offsetDateTime2 = rs.getObject(1, OffsetDateTime.class);
        assertEquals(offsetDateTime, offsetDateTime2);
        assertFalse(rs.next());
        rs.close();

        prep.setObject(1, offsetDateTime, Types.TIMESTAMP_WITH_TIMEZONE);
        rs = prep.executeQuery();
        rs.next();
        offsetDateTime2 = rs.getObject(1, OffsetDateTime.class);
        assertEquals(offsetDateTime, offsetDateTime2);
        // Check default mapping
        rs.getObject(1);
        assertFalse(rs.next());
        rs.close();
    }

    private void testZonedDateTime8(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT ?");
        ZonedDateTime zonedDateTime = ZonedDateTime.parse("2001-02-03T04:05:06+02:30");
        prep.setObject(1, zonedDateTime);
        ResultSet rs = prep.executeQuery();
        rs.next();
        ZonedDateTime zonedDateTime2 = rs.getObject(1, ZonedDateTime.class);
        assertEquals(zonedDateTime, zonedDateTime2);
        assertFalse(rs.next());
        rs.close();

        prep.setObject(1, zonedDateTime, Types.TIMESTAMP_WITH_TIMEZONE);
        rs = prep.executeQuery();
        rs.next();
        zonedDateTime2 = rs.getObject(1, ZonedDateTime.class);
        assertEquals(zonedDateTime, zonedDateTime2);
        assertFalse(rs.next());
        rs.close();
    }

    private void testInstant8(Connection conn) throws Exception {
        PreparedStatement prep = conn.prepareStatement("SELECT ?");
        testInstant8Impl(prep, Instant.now());
        testInstant8Impl(prep, Instant.parse("2000-01-15T12:13:14.123456789Z"));
        testInstant8Impl(prep, Instant.parse("1500-09-10T23:22:11.123456789Z"));
    }

    private void testInstant8Impl(PreparedStatement prep, Instant instant) throws SQLException {
        prep.setObject(1, instant);
        ResultSet rs = prep.executeQuery();
        rs.next();
        Instant instant2 = rs.getObject(1, Instant.class);
        assertEquals(instant, instant2);
        Timestamp ts = rs.getTimestamp(1);
        assertEquals(instant, ts.toInstant());
        assertFalse(rs.next());
        rs.close();

        prep.setTimestamp(1, ts);
        rs = prep.executeQuery();
        rs.next();
        instant2 = rs.getObject(1, Instant.class);
        assertEquals(instant, instant2);
        assertFalse(rs.next());
        rs.close();
    }

    private void testInterval(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT ?");
        Interval interval = new Interval(IntervalQualifier.MINUTE, false, 100, 0);
        prep.setObject(1, interval);
        ResultSet rs = prep.executeQuery();
        rs.next();
        assertEquals("INTERVAL '100' MINUTE", rs.getString(1));
        assertEquals(interval, rs.getObject(1));
        assertEquals(interval, rs.getObject(1, Interval.class));
    }

    private void testInterval8(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT ?");
        testPeriod8(prep, 1, 2, "INTERVAL '1-2' YEAR TO MONTH");
        testPeriod8(prep, -1, -2, "INTERVAL '-1-2' YEAR TO MONTH");
        testPeriod8(prep, 1, -8, "INTERVAL '0-4' YEAR TO MONTH", 0, 4);
        testPeriod8(prep, -1, 8, "INTERVAL '-0-4' YEAR TO MONTH", 0, -4);
        testPeriod8(prep, 0, 0, "INTERVAL '0-0' YEAR TO MONTH");
        testPeriod8(prep, 100, 0, "INTERVAL '100' YEAR");
        testPeriod8(prep, -100, 0, "INTERVAL '-100' YEAR");
        testPeriod8(prep, 0, 100, "INTERVAL '100' MONTH");
        testPeriod8(prep, 0, -100, "INTERVAL '-100' MONTH");
        Period period = Period.of(0, 0, 1);
        assertThrows(ErrorCode.INVALID_VALUE_2, prep).setObject(1, period);
        Duration duration = Duration.ofSeconds(-4, 900_000_000);
        prep.setObject(1, duration);
        ResultSet rs = prep.executeQuery();
        rs.next();
        assertEquals("INTERVAL '-3.1' SECOND", rs.getString(1));
        assertEquals(duration, rs.getObject(1, Duration.class));
    }

    private void testPeriod8(PreparedStatement prep, int years, int months, String expectedString)
            throws SQLException {
        testPeriod8(prep, years, months, expectedString, years, months);
    }

    private void testPeriod8(PreparedStatement prep, int years, int months, String expectedString, int expYears,
            int expMonths) throws SQLException {
        Period period = Period.of(years, months, 0);
        Period expectedPeriod = Period.of(expYears, expMonths, 0);
        prep.setObject(1, period);
        ResultSet rs = prep.executeQuery();
        rs.next();
        assertEquals(expectedString, rs.getString(1));
        assertEquals(expectedPeriod, rs.getObject(1, Period.class));
    }

    private void testJson(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID BIGINT, J JSON)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES (?, ?)");
        prep.setInt(1, 1);
        prep.setString(2, "[1]");
        prep.executeUpdate();
        prep = conn.prepareStatement("INSERT INTO TEST VALUES (?, ? FORMAT JSON)");
        prep.setInt(1, 2);
        prep.setString(2, "[1]");
        prep.executeUpdate();
        prep.setInt(1, 3);
        prep.setString(2, null);
        prep.executeUpdate();
        prep = conn.prepareStatement("INSERT INTO TEST VALUES (?, ?)");
        prep.setInt(1, 4);
        prep.setObject(2, "[1]", H2Type.JSON);
        prep.executeUpdate();
        prep.setInt(1, 5);
        prep.setObject(2, null, H2Type.JSON);
        prep.executeUpdate();
        try (ResultSet rs = stat.executeQuery("SELECT J FROM TEST ORDER BY ID")) {
            assertTrue(rs.next());
            assertEquals("\"[1]\"", rs.getString(1));
            for (int i = 0; i < 2; i++) {
                assertTrue(rs.next());
                assertEquals("[1]", rs.getString(1));
                assertTrue(rs.next());
                assertEquals(null, rs.getString(1));
            }
            assertFalse(rs.next());
        }
        stat.execute("DROP TABLE TEST");
    }

    private void testPreparedSubquery(Connection conn) throws SQLException {
        Statement s = conn.createStatement();
        s.executeUpdate("CREATE TABLE TEST(ID IDENTITY, FLAG BIT)");
        s.executeUpdate("INSERT INTO TEST(ID, FLAG) VALUES(0, FALSE)");
        s.executeUpdate("INSERT INTO TEST(ID, FLAG) VALUES(1, FALSE)");
        PreparedStatement u = conn.prepareStatement(
                "SELECT ID, FLAG FROM TEST ORDER BY ID");
        PreparedStatement p = conn.prepareStatement(
                "UPDATE TEST SET FLAG=true WHERE ID=(SELECT ?)");
        p.clearParameters();
        p.setLong(1, 0);
        assertEquals(1, p.executeUpdate());
        p.clearParameters();
        p.setLong(1, 1);
        assertEquals(1, p.executeUpdate());
        ResultSet rs = u.executeQuery();
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertTrue(rs.getBoolean(2));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.getBoolean(2));

        p = conn.prepareStatement("SELECT * FROM TEST " +
                "WHERE EXISTS(SELECT * FROM TEST WHERE ID=?)");
        p.setInt(1, -1);
        rs = p.executeQuery();
        assertFalse(rs.next());
        p.setInt(1, 1);
        rs = p.executeQuery();
        assertTrue(rs.next());

        s.executeUpdate("DROP TABLE IF EXISTS TEST");
    }

    private void testParameterMetaData(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT ?, ?, ? FROM DUAL");
        ParameterMetaData pm = prep.getParameterMetaData();
        assertEquals("java.lang.String", pm.getParameterClassName(1));
        assertEquals("CHARACTER VARYING", pm.getParameterTypeName(1));
        assertEquals(3, pm.getParameterCount());
        assertEquals(ParameterMetaData.parameterModeIn, pm.getParameterMode(1));
        assertEquals(Types.VARCHAR, pm.getParameterType(1));
        assertEquals(0, pm.getPrecision(1));
        assertEquals(0, pm.getScale(1));
        assertEquals(ResultSetMetaData.columnNullableUnknown, pm.isNullable(1));
        assertEquals(pm.isSigned(1), true);
        assertThrows(ErrorCode.INVALID_VALUE_2, pm).getPrecision(0);
        assertThrows(ErrorCode.INVALID_VALUE_2, pm).getPrecision(4);
        prep.close();
        assertThrows(ErrorCode.OBJECT_CLOSED, pm).getPrecision(1);

        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST3(ID INT, " +
                "NAME VARCHAR(255), DATA1 DECIMAL(10,2), DATA2 NUMERIC(10,2))");
        PreparedStatement prep1 = conn.prepareStatement(
                "UPDATE TEST3 SET ID=?, NAME=?, DATA1=?, DATA2=?");
        PreparedStatement prep2 = conn.prepareStatement(
                "INSERT INTO TEST3 VALUES(?, ?, ?, ?)");
        checkParameter(prep1, 1, "java.lang.Integer", 4, "INTEGER", 32, 0);
        checkParameter(prep1, 2, "java.lang.String", 12, "CHARACTER VARYING", 255, 0);
        checkParameter(prep1, 3, "java.math.BigDecimal", Types.DECIMAL, "DECIMAL", 10, 2);
        checkParameter(prep1, 4, "java.math.BigDecimal", Types.NUMERIC, "NUMERIC", 10, 2);
        checkParameter(prep2, 1, "java.lang.Integer", 4, "INTEGER", 32, 0);
        checkParameter(prep2, 2, "java.lang.String", 12, "CHARACTER VARYING", 255, 0);
        checkParameter(prep2, 3, "java.math.BigDecimal", Types.DECIMAL, "DECIMAL", 10, 2);
        checkParameter(prep2, 4, "java.math.BigDecimal", Types.NUMERIC, "NUMERIC", 10, 2);
        PreparedStatement prep3 = conn.prepareStatement(
                "SELECT * FROM TEST3 WHERE ID=? AND NAME LIKE ? AND ?>DATA1 AND ?>DATA2");
        checkParameter(prep3, 1, "java.lang.Integer", 4, "INTEGER", 32, 0);
        checkParameter(prep3, 2, "java.lang.String", 12, "CHARACTER VARYING", 0, 0);
        checkParameter(prep3, 3, "java.math.BigDecimal", Types.DECIMAL, "DECIMAL", 10, 2);
        checkParameter(prep3, 4, "java.math.BigDecimal", Types.NUMERIC, "NUMERIC", 10, 2);
        stat.execute("DROP TABLE TEST3");
    }

    private void checkParameter(PreparedStatement prep, int index,
            String className, int type, String typeName, int precision,
            int scale) throws SQLException {
        ParameterMetaData meta = prep.getParameterMetaData();
        assertEquals(className, meta.getParameterClassName(index));
        assertEquals(type, meta.getParameterType(index));
        assertEquals(typeName, meta.getParameterTypeName(index));
        assertEquals(precision, meta.getPrecision(index));
        assertEquals(scale, meta.getScale(index));
    }

    private void testLikeIndex(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, V INT, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 2, 'Hello')");
        stat.execute("INSERT INTO TEST VALUES(2, 4, 'World')");
        stat.execute("create index idxname on test(name);");
        PreparedStatement prep, prepExe;

        prep = conn.prepareStatement(
                "EXPLAIN SELECT * FROM TEST WHERE NAME LIKE ?");
        assertEquals(1, prep.getParameterMetaData().getParameterCount());
        prepExe = conn.prepareStatement(
                "SELECT * FROM TEST WHERE NAME LIKE ?");
        prep.setString(1, "%orld");
        prepExe.setString(1, "%orld");
        ResultSet rs = prep.executeQuery();
        rs.next();
        String plan = rs.getString(1);
        assertContains(plan, ".tableScan");
        rs = prepExe.executeQuery();
        rs.next();
        assertEquals("World", rs.getString(3));
        assertFalse(rs.next());

        prep.setString(1, "H%");
        prepExe.setString(1, "H%");
        rs = prep.executeQuery();
        rs.next();
        String plan1 = rs.getString(1);
        assertContains(plan1, "IDXNAME");
        rs = prepExe.executeQuery();
        rs.next();
        assertEquals("Hello", rs.getString(3));
        assertFalse(rs.next());

        stat.execute("DROP TABLE IF EXISTS TEST");
    }

    private void testCasewhen(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("INSERT INTO TEST VALUES(1),(2),(3)");
        PreparedStatement prep;
        ResultSet rs;
        prep = conn.prepareStatement("EXPLAIN SELECT COUNT(*) FROM TEST " +
                "WHERE CASEWHEN(ID=1, ID, ID)=? GROUP BY ID");
        prep.setInt(1, 1);
        rs = prep.executeQuery();
        rs.next();
        String plan = rs.getString(1);
        trace(plan);
        rs.close();
        prep = conn.prepareStatement("EXPLAIN SELECT COUNT(*) FROM TEST " +
                "WHERE CASE ID WHEN 1 THEN ID WHEN 2 THEN ID " +
                "ELSE ID END=? GROUP BY ID");
        prep.setInt(1, 1);
        rs = prep.executeQuery();
        rs.next();
        plan = rs.getString(1);
        trace(plan);

        prep = conn.prepareStatement("SELECT COUNT(*) FROM TEST " +
                "WHERE CASEWHEN(ID=1, ID, ID)=? GROUP BY ID");
        prep.setInt(1, 1);
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        prep = conn.prepareStatement("SELECT COUNT(*) FROM TEST " +
                "WHERE CASE ID WHEN 1 THEN ID WHEN 2 THEN ID " +
                "ELSE ID END=? GROUP BY ID");
        prep.setInt(1, 1);
        rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        prep = conn.prepareStatement("SELECT * FROM TEST WHERE ? IS NULL");
        prep.setString(1, "Hello");
        rs = prep.executeQuery();
        assertFalse(rs.next());
        assertThrows(ErrorCode.UNKNOWN_DATA_TYPE_1, conn).
                prepareStatement("select ? from dual union select ? from dual");
        prep = conn.prepareStatement("select cast(? as varchar) " +
                "from dual union select ? from dual");
        assertEquals(2, prep.getParameterMetaData().getParameterCount());
        prep.setString(1, "a");
        prep.setString(2, "a");
        rs = prep.executeQuery();
        rs.next();
        assertEquals("a", rs.getString(1));
        assertEquals("a", rs.getString(1));
        assertFalse(rs.next());

        stat.execute("DROP TABLE TEST");
    }

    private void testSubquery(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("INSERT INTO TEST VALUES(1),(2),(3)");
        PreparedStatement prep = conn.prepareStatement("select x.id, ? from "
                + "(select * from test where id in(?, ?)) x where x.id*2 <>  ?");
        assertEquals(4, prep.getParameterMetaData().getParameterCount());
        prep.setInt(1, 0);
        prep.setInt(2, 1);
        prep.setInt(3, 2);
        prep.setInt(4, 4);
        ResultSet rs = prep.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals(0, rs.getInt(2));
        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

    private void testDataTypes(Connection conn) throws SQLException {
        conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        ResultSet rs;
        trace("Create tables");
        stat.execute("CREATE TABLE T_INT" +
                "(ID INT PRIMARY KEY,V INT)");
        stat.execute("CREATE TABLE T_VARCHAR" +
                "(ID INT PRIMARY KEY,V VARCHAR(255))");
        stat.execute("CREATE TABLE T_DECIMAL_0" +
                "(ID INT PRIMARY KEY,V DECIMAL(30,0))");
        stat.execute("CREATE TABLE T_DECIMAL_10" +
                "(ID INT PRIMARY KEY,V DECIMAL(20,10))");
        stat.execute("CREATE TABLE T_DATETIME" +
                "(ID INT PRIMARY KEY,V DATETIME)");
        stat.execute("CREATE TABLE T_BIGINT" +
                "(ID INT PRIMARY KEY,V DECIMAL(30,0))");
        prep = conn.prepareStatement("INSERT INTO T_INT VALUES(?,?)",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        prep.setInt(1, 1);
        prep.setInt(2, 0);
        prep.executeUpdate();
        prep.setInt(1, 2);
        prep.setInt(2, -1);
        prep.executeUpdate();
        prep.setInt(1, 3);
        prep.setInt(2, 3);
        prep.executeUpdate();
        prep.setInt(1, 4);
        prep.setNull(2, Types.INTEGER, "INTEGER");
        prep.setNull(2, Types.INTEGER);
        prep.executeUpdate();
        prep.setInt(1, 5);
        prep.setBigDecimal(2, new java.math.BigDecimal("0"));
        prep.executeUpdate();
        prep.setInt(1, 6);
        prep.setString(2, "-1");
        prep.executeUpdate();
        prep.setInt(1, 7);
        prep.setObject(2, 3);
        prep.executeUpdate();
        prep.setObject(1, "8");
        // should throw an exception
        prep.setObject(2, null);
        // some databases don't allow calling setObject with null (no data type)
        prep.executeUpdate();
        prep.setInt(1, 9);
        prep.setObject(2, -4, Types.VARCHAR);
        prep.executeUpdate();
        prep.setInt(1, 10);
        prep.setObject(2, "5", Types.INTEGER);
        prep.executeUpdate();
        prep.setInt(1, 11);
        prep.setObject(2, null, Types.INTEGER);
        prep.executeUpdate();
        prep.setInt(1, 12);
        prep.setBoolean(2, true);
        prep.executeUpdate();
        prep.setInt(1, 13);
        prep.setBoolean(2, false);
        prep.executeUpdate();
        prep.setInt(1, 14);
        prep.setByte(2, (byte) -20);
        prep.executeUpdate();
        prep.setInt(1, 15);
        prep.setByte(2, (byte) 100);
        prep.executeUpdate();
        prep.setInt(1, 16);
        prep.setShort(2, (short) 30000);
        prep.executeUpdate();
        prep.setInt(1, 17);
        prep.setShort(2, (short) (-30000));
        prep.executeUpdate();
        prep.setInt(1, 18);
        prep.setLong(2, Integer.MAX_VALUE);
        prep.executeUpdate();
        prep.setInt(1, 19);
        prep.setLong(2, Integer.MIN_VALUE);
        prep.executeUpdate();

        assertTrue(stat.execute("SELECT * FROM T_INT ORDER BY ID"));
        rs = stat.getResultSet();
        assertResultSetOrdered(rs, new String[][] { { "1", "0" },
                { "2", "-1" }, { "3", "3" }, { "4", null }, { "5", "0" },
                { "6", "-1" }, { "7", "3" }, { "8", null }, { "9", "-4" },
                { "10", "5" }, { "11", null }, { "12", "1" }, { "13", "0" },
                { "14", "-20" }, { "15", "100" }, { "16", "30000" },
                { "17", "-30000" }, { "18", "" + Integer.MAX_VALUE },
                { "19", "" + Integer.MIN_VALUE }, });

        prep = conn.prepareStatement("INSERT INTO T_DECIMAL_0 VALUES(?,?)");
        prep.setInt(1, 1);
        prep.setLong(2, Long.MAX_VALUE);
        prep.executeUpdate();
        prep.setInt(1, 2);
        prep.setLong(2, Long.MIN_VALUE);
        prep.executeUpdate();
        prep.setInt(1, 3);
        prep.setFloat(2, 10);
        prep.executeUpdate();
        prep.setInt(1, 4);
        prep.setFloat(2, -20);
        prep.executeUpdate();
        prep.setInt(1, 5);
        prep.setFloat(2, 30);
        prep.executeUpdate();
        prep.setInt(1, 6);
        prep.setFloat(2, -40);
        prep.executeUpdate();

        rs = stat.executeQuery("SELECT V FROM T_DECIMAL_0 ORDER BY ID");
        checkBigDecimal(rs, new String[] { "" + Long.MAX_VALUE,
                "" + Long.MIN_VALUE, "10", "-20", "30", "-40" });
        prep = conn.prepareStatement("INSERT INTO T_BIGINT VALUES(?,?)");
        prep.setInt(1, 1);
        prep.setObject(2, new BigInteger("" + Long.MAX_VALUE));
        prep.executeUpdate();
        prep.setInt(1, 2);
        prep.setObject(2, Long.MIN_VALUE);
        prep.executeUpdate();
        prep.setInt(1, 3);
        prep.setObject(2, 10);
        prep.executeUpdate();
        prep.setInt(1, 4);
        prep.setObject(2, -20);
        prep.executeUpdate();
        prep.setInt(1, 5);
        prep.setObject(2, 30);
        prep.executeUpdate();
        prep.setInt(1, 6);
        prep.setObject(2, -40);
        prep.executeUpdate();
        prep.setInt(1, 7);
        prep.setObject(2, new BigInteger("-60"));
        prep.executeUpdate();

        rs = stat.executeQuery("SELECT V FROM T_BIGINT ORDER BY ID");
        checkBigDecimal(rs, new String[] { "" + Long.MAX_VALUE,
                "" + Long.MIN_VALUE, "10", "-20", "30", "-40", "-60" });
    }

    private void testGetMoreResults(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        ResultSet rs;
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("INSERT INTO TEST VALUES(1)");

        prep = conn.prepareStatement("SELECT * FROM TEST");
        // just to check if it doesn't throw an exception - it may be null
        prep.getMetaData();
        assertTrue(prep.execute());
        rs = prep.getResultSet();
        assertFalse(prep.getMoreResults());
        assertEquals(-1, prep.getUpdateCount());
        // supposed to be closed now
        assertThrows(ErrorCode.OBJECT_CLOSED, rs).next();
        assertEquals(-1, prep.getUpdateCount());

        prep = conn.prepareStatement("UPDATE TEST SET ID = 2");
        assertFalse(prep.execute());
        assertEquals(1, prep.getUpdateCount());
        assertFalse(prep.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
        assertEquals(-1, prep.getUpdateCount());
        // supposed to be closed now
        assertThrows(ErrorCode.OBJECT_CLOSED, rs).next();
        assertEquals(-1, prep.getUpdateCount());

        prep = conn.prepareStatement("DELETE FROM TEST");
        prep.executeUpdate();
        assertFalse(prep.getMoreResults());
        assertEquals(-1, prep.getUpdateCount());
        stat.execute("DROP TABLE TEST");
    }

    private void testObject(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs;
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        PreparedStatement prep = conn.prepareStatement(
                "SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM TEST");
        prep.setObject(1, Boolean.TRUE);
        prep.setObject(2, "Abc");
        prep.setObject(3, new BigDecimal("10.2"));
        prep.setObject(4, (byte) 0xff);
        prep.setObject(5, Short.MAX_VALUE);
        prep.setObject(6, Integer.MIN_VALUE);
        prep.setObject(7, Long.MAX_VALUE);
        prep.setObject(8, Float.MAX_VALUE);
        prep.setObject(9, Double.MAX_VALUE);
        prep.setObject(10, java.sql.Date.valueOf("2001-02-03"));
        prep.setObject(11, java.sql.Time.valueOf("04:05:06"));
        prep.setObject(12, java.sql.Timestamp.valueOf(
                "2001-02-03 04:05:06.123456789"));
        prep.setObject(13, new java.util.Date(java.sql.Date.valueOf(
                "2001-02-03").getTime()));
        prep.setObject(14, new byte[] { 10, 20, 30 });
        prep.setObject(15, 'a', Types.JAVA_OBJECT);
        prep.setObject(16, "2001-01-02", Types.DATE);
        // converting to null seems strange...
        prep.setObject(17, "2001-01-02", Types.NULL);
        prep.setObject(18, "3.725", Types.DOUBLE);
        prep.setObject(19, "23:22:21", Types.TIME);
        prep.setObject(20, new java.math.BigInteger("12345"), Types.JAVA_OBJECT);
        prep.setArray(21, conn.createArrayOf("TINYINT", new Object[] {(byte) 1}));
        prep.setArray(22, conn.createArrayOf("SMALLINT", new Object[] {(short) -2}));
        rs = prep.executeQuery();
        rs.next();
        assertTrue(rs.getObject(1).equals(Boolean.TRUE));
        assertTrue(rs.getObject(2).equals("Abc"));
        assertTrue(rs.getObject(3).equals(new BigDecimal("10.2")));
        assertTrue(rs.getObject(4).equals(Integer.valueOf(-1)));
        assertTrue(rs.getObject(5).equals(Integer.valueOf(Short.MAX_VALUE)));
        assertTrue(rs.getObject(6).equals(Integer.MIN_VALUE));
        assertTrue(rs.getObject(7).equals(Long.MAX_VALUE));
        assertTrue(rs.getObject(8).equals(Float.MAX_VALUE));
        assertTrue(rs.getObject(9).equals(Double.MAX_VALUE));
        assertTrue(rs.getObject(10).equals(
                java.sql.Date.valueOf("2001-02-03")));
        assertEquals("04:05:06", rs.getObject(11).toString());
        assertTrue(rs.getObject(11).equals(
                java.sql.Time.valueOf("04:05:06")));
        assertTrue(rs.getObject(12).equals(
                java.sql.Timestamp.valueOf("2001-02-03 04:05:06.123456789")));
        assertTrue(rs.getObject(13).equals(
                java.sql.Timestamp.valueOf("2001-02-03 00:00:00")));
        assertEquals(new byte[] { 10, 20, 30 }, (byte[]) rs.getObject(14));
        assertTrue(rs.getObject(15).equals('a'));
        assertTrue(rs.getObject(16).equals(
                java.sql.Date.valueOf("2001-01-02")));
        assertTrue(rs.getObject(17) == null && rs.wasNull());
        assertTrue(rs.getObject(18).equals(3.725d));
        assertTrue(rs.getObject(19).equals(
                java.sql.Time.valueOf("23:22:21")));
        assertTrue(rs.getObject(20).equals(
                new java.math.BigInteger("12345")));
        Object[] a = (Object[]) ((Array) rs.getObject(21)).getArray();
        assertEquals(a[0], Integer.valueOf(1));
        a = (Object[]) ((Array) rs.getObject(22)).getArray();
        assertEquals(a[0], Integer.valueOf(-2));

        // } else if(x instanceof java.io.Reader) {
        // return session.createLob(Value.CLOB,
        // TypeConverter.getInputStream((java.io.Reader)x), 0);
        // } else if(x instanceof java.io.InputStream) {
        // return session.createLob(Value.BLOB, (java.io.InputStream)x, 0);
        // } else {
        // return ValueBytes.get(TypeConverter.serialize(x));

        stat.execute("DROP TABLE TEST");

    }

    private int getLength() {
        return getSize(LOB_SIZE, LOB_SIZE_BIG);
    }

    private void testBlob(Connection conn) throws SQLException {
        trace("testBlob");
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        ResultSet rs;
        stat.execute("CREATE TABLE T_BLOB(ID INT PRIMARY KEY,V1 BLOB,V2 BLOB)");
        trace("table created");
        prep = conn.prepareStatement("INSERT INTO T_BLOB VALUES(?,?,?)");

        prep.setInt(1, 1);
        prep.setBytes(2, null);
        prep.setNull(3, Types.BINARY);
        prep.executeUpdate();

        prep.setInt(1, 2);
        prep.setBinaryStream(2, null, 0);
        prep.setNull(3, Types.BLOB);
        prep.executeUpdate();

        int length = getLength();
        byte[] big1 = new byte[length];
        byte[] big2 = new byte[length];
        for (int i = 0; i < big1.length; i++) {
            big1[i] = (byte) ((i * 11) % 254);
            big2[i] = (byte) ((i * 17) % 251);
        }

        prep.setInt(1, 3);
        prep.setBytes(2, big1);
        prep.setBytes(3, big2);
        prep.executeUpdate();

        prep.setInt(1, 4);
        ByteArrayInputStream buffer;
        buffer = new ByteArrayInputStream(big2);
        prep.setBinaryStream(2, buffer, big2.length);
        buffer = new ByteArrayInputStream(big1);
        prep.setBinaryStream(3, buffer, big1.length);
        prep.executeUpdate();
        try {
            buffer.close();
            trace("buffer not closed");
        } catch (IOException e) {
            trace("buffer closed");
        }

        prep.setInt(1, 5);
        buffer = new ByteArrayInputStream(big2);
        prep.setObject(2, buffer, Types.BLOB, 0);
        buffer = new ByteArrayInputStream(big1);
        prep.setObject(3, buffer);
        prep.executeUpdate();

        rs = stat.executeQuery("SELECT ID, V1, V2 FROM T_BLOB ORDER BY ID");

        rs.next();
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.getBytes(2) == null && rs.wasNull());
        assertTrue(rs.getBytes(3) == null && rs.wasNull());

        rs.next();
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.getBytes(2) == null && rs.wasNull());
        assertTrue(rs.getBytes(3) == null && rs.wasNull());

        rs.next();
        assertEquals(3, rs.getInt(1));
        assertEquals(big1, rs.getBytes(2));
        assertEquals(big2, rs.getBytes(3));

        rs.next();
        assertEquals(4, rs.getInt(1));
        assertEquals(big2, rs.getBytes(2));
        assertEquals(big1, rs.getBytes(3));

        rs.next();
        assertEquals(5, rs.getInt(1));
        assertEquals(big2, rs.getBytes(2));
        assertEquals(big1, rs.getBytes(3));

        assertFalse(rs.next());
    }

    private void testClob(Connection conn) throws SQLException {
        trace("testClob");
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        ResultSet rs;
        stat.execute("CREATE TABLE T_CLOB(ID INT PRIMARY KEY,V1 CLOB,V2 CLOB)");
        StringBuilder asciiBuffer = new StringBuilder();
        int len = getLength();
        for (int i = 0; i < len; i++) {
            asciiBuffer.append((char) ('a' + (i % 20)));
        }
        String ascii1 = asciiBuffer.toString();
        String ascii2 = "Number2 " + ascii1;
        prep = conn.prepareStatement("INSERT INTO T_CLOB VALUES(?,?,?)");

        prep.setInt(1, 1);
        prep.setString(2, null);
        prep.setNull(3, Types.CLOB);
        prep.executeUpdate();

        prep.clearParameters();
        prep.setInt(1, 2);
        prep.setAsciiStream(2, null, 0);
        prep.setCharacterStream(3, null, 0);
        prep.executeUpdate();

        prep.clearParameters();
        prep.setInt(1, 3);
        prep.setCharacterStream(2,
                new StringReader(ascii1), ascii1.length());
        prep.setCharacterStream(3, null, 0);
        prep.setAsciiStream(3,
                new ByteArrayInputStream(ascii2.getBytes()), ascii2.length());
        prep.executeUpdate();

        prep.clearParameters();
        prep.setInt(1, 4);
        prep.setNull(2, Types.CLOB);
        prep.setString(2, ascii2);
        prep.setCharacterStream(3, null, 0);
        prep.setNull(3, Types.CLOB);
        prep.setString(3, ascii1);
        prep.executeUpdate();

        prep.clearParameters();
        prep.setInt(1, 5);
        prep.setObject(2, new StringReader(ascii1));
        prep.setObject(3, new StringReader(ascii2), Types.CLOB, 0);
        prep.executeUpdate();

        rs = stat.executeQuery("SELECT ID, V1, V2 FROM T_CLOB ORDER BY ID");

        rs.next();
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.getCharacterStream(2) == null && rs.wasNull());
        assertTrue(rs.getAsciiStream(3) == null && rs.wasNull());

        rs.next();
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.getString(2) == null && rs.wasNull());
        assertTrue(rs.getString(3) == null && rs.wasNull());

        rs.next();
        assertEquals(3, rs.getInt(1));
        assertEquals(ascii1, rs.getString(2));
        assertEquals(ascii2, rs.getString(3));

        rs.next();
        assertEquals(4, rs.getInt(1));
        assertEquals(ascii2, rs.getString(2));
        assertEquals(ascii1, rs.getString(3));

        rs.next();
        assertEquals(5, rs.getInt(1));
        assertEquals(ascii1, rs.getString(2));
        assertEquals(ascii2, rs.getString(3));

        assertFalse(rs.next());
        assertNull(prep.getWarnings());
        prep.clearWarnings();
        assertNull(prep.getWarnings());
        assertTrue(conn == prep.getConnection());
    }

    private void testPreparedStatementWithLiteralsNone() throws SQLException {
        // make sure that when the analyze table kicks in,
        // it works with ALLOW_LITERALS=NONE
        deleteDb("preparedStatement");
        Connection conn = getConnection(
                "preparedStatement;ANALYZE_AUTO=100");
        conn.createStatement().execute(
                "SET ALLOW_LITERALS NONE");
        conn.prepareStatement("CREATE TABLE test (id INT)").execute();
        PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO test (id) VALUES (?)");
        for (int i = 0; i < 200; i++) {
            ps.setInt(1, i);
            ps.executeUpdate();
        }
        conn.close();
        deleteDb("preparedStatement");
    }

    private void testPreparedStatementWithIndexedParameterAndLiteralsNone() throws SQLException {
        // make sure that when the analyze table kicks in,
        // it works with ALLOW_LITERALS=NONE
        deleteDb("preparedStatement");
        Connection conn = getConnection(
                "preparedStatement;ANALYZE_AUTO=100");
        conn.createStatement().execute(
                "SET ALLOW_LITERALS NONE");
        conn.prepareStatement("CREATE TABLE test (id INT)").execute();
        PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO test (id) VALUES (?1)");

        ps.setInt(1, 1);
        ps.executeUpdate();

        conn.close();
        deleteDb("preparedStatement");
    }

    private void testPreparedStatementWithAnyParameter() throws SQLException {
        deleteDb("preparedStatement");
        Connection conn = getConnection("preparedStatement");
        conn.prepareStatement("CREATE TABLE TEST(ID INT PRIMARY KEY, V INT UNIQUE)").execute();
        PreparedStatement ps = conn.prepareStatement("INSERT INTO TEST(ID, V) VALUES (?, ?)");
        for (int i = 0; i < 10_000; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i * 10);
            ps.executeUpdate();
        }
        Integer[] values = {-100, 10, 200, 3_000, 40_000, 500_000};
        int[] expected = {1, 20, 300, 4_000};
        // Ensure that other methods return the same results
        ps = conn.prepareStatement("SELECT ID FROM TEST WHERE V IN (SELECT * FROM TABLE(X INT=?)) ORDER BY ID");
        anyParameterCheck(ps, values, expected);
        ps = conn.prepareStatement("SELECT ID FROM TEST INNER JOIN TABLE(X INT=?) T ON TEST.V = T.X");
        anyParameterCheck(ps, values, expected);
        // Test expression = ANY(?)
        ps = conn.prepareStatement("SELECT ID FROM TEST WHERE V = ANY(?)");
        assertThrows(ErrorCode.PARAMETER_NOT_SET_1, ps).executeQuery();
        anyParameterCheck(ps, values, expected);
        anyParameterCheck(ps, 300, new int[] {30});
        anyParameterCheck(ps, -5, new int[0]);
        ps = conn.prepareStatement("SELECT V, CASE V WHEN = ANY(?) THEN 1 ELSE 2 END FROM"
                + " (VALUES DATE '2000-01-01', DATE '2010-01-01') T(V) ORDER BY V");
        ps.setObject(1, new LocalDate[] { LocalDate.of(2000, 1, 1), LocalDate.of(2030, 1, 1) });
        try (ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(LocalDate.of(2000, 1, 1), rs.getObject(1, LocalDate.class));
            assertEquals(1, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(LocalDate.of(2010, 1, 1), rs.getObject(1, LocalDate.class));
            assertEquals(2, rs.getInt(2));
            assertFalse(rs.next());
            assertEquals("CASE V WHEN = ANY(?1) THEN 1 ELSE 2 END", rs.getMetaData().getColumnLabel(2));
        }
        conn.close();
        deleteDb("preparedStatement");
    }

    private void anyParameterCheck(PreparedStatement ps, Object values, int[] expected) throws SQLException {
        ps.setObject(1, values);
        try (ResultSet rs = ps.executeQuery()) {
            for (int exp : expected) {
                assertTrue(rs.next());
                assertEquals(exp, rs.getInt(1));
            }
            assertFalse(rs.next());
        }
    }

    private void checkBigDecimal(ResultSet rs, String[] value) throws SQLException {
        for (String v : value) {
            assertTrue(rs.next());
            java.math.BigDecimal x = rs.getBigDecimal(1);
            trace("v=" + v + " x=" + x);
            if (v == null) {
                assertNull(x);
            } else {
                assertTrue(x.compareTo(new java.math.BigDecimal(v)) == 0);
            }
        }
        assertFalse(rs.next());
    }

    private void testColumnMetaDataWithEquals(Connection conn)
            throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE TEST( id INT, someColumn INT )");
        PreparedStatement ps = conn
                .prepareStatement("INSERT INTO TEST VALUES(?,?)");
        ps.setInt(1, 0);
        ps.setInt(2, 999);
        ps.execute();
        ps = conn.prepareStatement("SELECT * FROM TEST WHERE someColumn = ?");
        assertEquals(Types.INTEGER,
                ps.getParameterMetaData().getParameterType(1));
        stmt.execute("DROP TABLE TEST");
    }

    private void testColumnMetaDataWithIn(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE TEST( id INT, someColumn INT )");
        PreparedStatement ps = conn
                .prepareStatement("INSERT INTO TEST VALUES( ? , ? )");
        ps.setInt(1, 0);
        ps.setInt(2, 999);
        ps.execute();
        ps = conn
                .prepareStatement("SELECT * FROM TEST WHERE someColumn IN (?,?)");
        assertEquals(Types.INTEGER,
                ps.getParameterMetaData().getParameterType(1));
        stmt.execute("DROP TABLE TEST");
    }

    private void testMultipleStatements(Connection conn) throws SQLException {
        assertThrows(ErrorCode.CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS, conn).prepareStatement("SELECT ?; SELECT ?1");
        assertThrows(ErrorCode.CANNOT_MIX_INDEXED_AND_UNINDEXED_PARAMS, conn).prepareStatement("SELECT ?1; SELECT ?");
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE TEST (ID IDENTITY, V INT)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO TEST(V) VALUES ?; INSERT INTO TEST(V) VALUES ?");
        ps.setInt(1, 1);
        ps.setInt(2, 2);
        ps.executeUpdate();
        ps = conn.prepareStatement("INSERT INTO TEST(V) VALUES ?2; INSERT INTO TEST(V) VALUES ?1;");
        ps.setInt(1, 3);
        ps.setInt(2, 4);
        ps.executeUpdate();
        try (ResultSet rs = stmt.executeQuery("SELECT V FROM TEST ORDER BY ID")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        }
        stmt.execute("DROP TABLE TEST");
        ps = conn.prepareStatement("CREATE TABLE A (C1 INT);" //
                + "CREATE INDEX A_IDX ON A(C1);" //
                + "ALTER TABLE A ADD (C2 INT);" //
                + "CREATE TABLE B AS (SELECT C1 FROM A);");
        ps.executeUpdate();
        stmt.execute("DROP TABLE A, B");
    }

    private void testParameterInSubquery(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE T1(ID1 BIGINT PRIMARY KEY, S INT NOT NULL)");
        stat.execute("CREATE TABLE T2(ID1 BIGINT REFERENCES T1, ID2 BIGINT)");

        stat.executeUpdate("INSERT INTO T1(ID1, S) VALUES(1, 1), (2, 1)");
        stat.executeUpdate("INSERT INTO T2(ID1, ID2) VALUES(1, 1), (2, 2)");

        PreparedStatement query = conn.prepareStatement("SELECT ID2 FROM "
                + "(SELECT * FROM T1 WHERE ID1 IN (SELECT ID1 FROM T2 WHERE ID2 = ?) AND S = ?) T1 "
                + "JOIN T2 USING(ID1)");

        query.setLong(1, 2L);
        query.setInt(2, 1);
        ResultSet rs = query.executeQuery();
        rs.next();
        assertEquals(2L, rs.getLong(1));
        query.setLong(1, 1L);
        rs = query.executeQuery();
        rs.next();
        assertEquals(1L, rs.getLong(1));
        stat.execute("DROP TABLE T2, T1");
    }

    private void testAfterRollback(Connection conn) throws SQLException {
        try (Statement stat = conn.createStatement()) {
            try {
                stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
                conn.setAutoCommit(false);

                // insert something into test table
                stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");

                // execute 'SELECT count(*)' with prepared-statements
                PreparedStatement pstmt = conn.prepareStatement("SELECT count(*) FROM TEST");
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }

                // rollback the insert
                conn.rollback();

                // re-execute the pstmt.
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1));
                }
            } finally {
                // cleanup
                stat.execute("DROP TABLE IF EXISTS TEST");
                conn.setAutoCommit(true);
            }
        }
    }

    private void testUnnestWithArrayParameter(Connection conn) throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "SELECT * FROM ("
                + "SELECT * FROM UNNEST(CAST(? AS INTEGER ARRAY)) UNION SELECT * FROM UNNEST(CAST(? AS INTEGER ARRAY))"
                + ") ORDER BY 1");
        prep.setObject(1, new Integer[] {1, 2, 3});
        prep.setObject(2, new Integer[] {3, 4, 5});
        try (ResultSet rs = prep.executeQuery()) {
            for (int i = 1; i <= 5; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            assertFalse(rs.next());
        }
        prep = conn.prepareStatement(
                "SELECT ARRAY_AGG(V) FROM UNNEST(ARRAY[CAST(? AS INTEGER), CAST(? AS INTEGER)]) T(V)");
        prep.setInt(1, 1);
        prep.setInt(2, 2);
        ResultSet rs = prep.executeQuery();
        assertTrue(rs.next());
        assertEquals(new Integer[] { 1, 2 }, rs.getObject(1, Integer[].class));
    }

}
