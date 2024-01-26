/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.h2.api.ErrorCode;
import org.h2.server.pg.PgServer;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.tools.Server;
import org.h2.util.DateTimeUtils;

/**
 * Tests the PostgreSQL server protocol compliant implementation.
 */
public class TestPgServer extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.memory = true;
        test.testFromMain();
    }

    @Override
    public boolean isEnabled() {
        if (!config.memory) {
            return false;
        }
        return true;
    }

    @Override
    public void test() throws Exception {
        // testPgAdapter() starts server by itself without a wait so run it first
        testPgAdapter();
        testKeyAlias();
        testCancelQuery();
        testTextualAndBinaryTypes();
        testBinaryNumeric();
        testDateTime();
        testPrepareWithUnspecifiedType();
        testOtherPgClients();
        testArray();
    }

    private boolean getPgJdbcDriver() {
        try {
            Class.forName("org.postgresql.Driver");
            return true;
        } catch (ClassNotFoundException e) {
            println("PostgreSQL JDBC driver not found - PgServer not tested");
            return false;
        }
    }

    private Server createPgServer(String... args) throws SQLException {
        Server server = Server.createPgServer(args);
        int failures = 0;
        for (;;) {
            try {
                server.start();
                return server;
            } catch (SQLException e) {
                // the sleeps are too mitigate "port in use" exceptions on Jenkins
                if (e.getErrorCode() != ErrorCode.EXCEPTION_OPENING_PORT_2 || ++failures > 10) {
                    throw e;
                }
                println("Sleeping");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e2) {
                    throw new RuntimeException(e2);
                }
            }
        }
    }

    private void testPgAdapter() throws SQLException {
        deleteDb("pgserver");
        Server server = Server.createPgServer(
                "-ifNotExists", "-baseDir", getBaseDir(), "-pgPort", "5535", "-pgDaemon");
        assertEquals(5535, server.getPort());
        assertEquals("Not started", server.getStatus());
        server.start();
        assertStartsWith(server.getStatus(), "PG server running at pg://");
        try {
            if (getPgJdbcDriver()) {
                testPgClient();
                testPgClientSimple();
            }
        } finally {
            server.stop();
        }
    }

    private void testCancelQuery() throws Exception {
        if (!getPgJdbcDriver()) {
            return;
        }

        Server server = createPgServer(
                "-ifNotExists", "-pgPort", "5535", "-pgDaemon", "-key", "pgserver", "mem:pgserver");

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5535/pgserver", "sa", "sa");
            Statement stat = conn.createStatement();
            stat.execute("create alias sleep for 'java.lang.Thread.sleep(long)'");

            // create a table with 200 rows (cancel interval is 127)
            stat.execute("create table test(id int)");
            for (int i = 0; i < 200; i++) {
                stat.execute("insert into test (id) values (rand())");
            }

            Future<Boolean> future = executor.submit(() -> stat.execute("select id, sleep(5) from test"));

            // give it a little time to start and then cancel it
            Thread.sleep(100);
            stat.cancel();

            try {
                future.get();
                throw new IllegalStateException();
            } catch (ExecutionException e) {
                assertStartsWith(e.getCause().getMessage(),
                        "ERROR: canceling statement due to user request");
            } finally {
                conn.close();
            }
        } finally {
            server.stop();
            executor.shutdown();
        }
        deleteDb("pgserver");
    }

    private void testPgClient() throws SQLException {
        Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5535/pgserver", "sa", "sa");
        Statement stat = conn.createStatement();
        assertThrows(SQLException.class, stat).
                execute("select ***");
        stat.execute("create user test password 'test'");
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("create index idx_test_name on test(name, id)");
        stat.execute("grant all on test to test");
        int userId;
        try (ResultSet rs = stat.executeQuery("call db_object_id('USER', 'test')")) {
            rs.next();
            userId = rs.getInt(1);
        }
        int indexId;
        try (ResultSet rs = stat.executeQuery("call db_object_id('INDEX', 'public', 'idx_test_name')")) {
            rs.next();
            indexId = rs.getInt(1);
        }
        stat.close();
        conn.close();

        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5535/pgserver", "test", "test");
        stat = conn.createStatement();
        ResultSet rs;

        stat.execute("prepare test(int, int) as select ?1*?2");
        rs = stat.executeQuery("execute test(3, 2)");
        rs.next();
        assertEquals(6, rs.getInt(1));
        stat.execute("deallocate test");

        PreparedStatement prep;
        prep = conn.prepareStatement("select * from test where name = ?");
        prep.setNull(1, Types.VARCHAR);
        rs = prep.executeQuery();
        assertFalse(rs.next());

        prep = conn.prepareStatement("insert into test values(?, ?)");
        ParameterMetaData meta = prep.getParameterMetaData();
        assertEquals(2, meta.getParameterCount());
        prep.setInt(1, 1);
        prep.setString(2, "Hello");
        prep.execute();
        rs = stat.executeQuery("select *, null nul from test");
        rs.next();

        ResultSetMetaData rsMeta = rs.getMetaData();
        assertEquals(Types.INTEGER, rsMeta.getColumnType(1));
        assertEquals(Types.VARCHAR, rsMeta.getColumnType(2));
        assertEquals(Types.VARCHAR, rsMeta.getColumnType(3));
        assertEquals("test", rsMeta.getTableName(1));

        prep.close();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        prep = conn.prepareStatement(
                "select * from test " +
                "where id = ? and name = ?");
        prep.setInt(1, 1);
        prep.setString(2, "Hello");
        rs = prep.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        rs.close();
        DatabaseMetaData dbMeta = conn.getMetaData();
        rs = dbMeta.getTables(null, null, "TEST", null);
        assertFalse(rs.next());
        rs = dbMeta.getTables(null, null, "test", null);
        assertTrue(rs.next());
        assertEquals("test", rs.getString("TABLE_NAME"));
        assertFalse(rs.next());
        rs = dbMeta.getColumns(null, null, "test", null);
        rs.next();
        assertEquals("id", rs.getString("COLUMN_NAME"));
        rs.next();
        assertEquals("name", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        rs = dbMeta.getIndexInfo(null, null, "TEST", false, false);
        // index info is currently disabled
        // rs.next();
        // assertEquals("TEST", rs.getString("TABLE_NAME"));
        // rs.next();
        // assertEquals("TEST", rs.getString("TABLE_NAME"));
        assertFalse(rs.next());
        rs = stat.executeQuery(
                "select version(), pg_postmaster_start_time(), current_schema()");
        rs.next();
        String s = rs.getString(1);
        assertContains(s, "H2");
        assertContains(s, "PostgreSQL");
        s = rs.getString(2);
        s = rs.getString(3);
        assertEquals(s, "public");
        assertFalse(rs.next());

        conn.setAutoCommit(false);
        stat.execute("delete from test");
        conn.rollback();
        stat.execute("update test set name = 'Hallo'");
        conn.commit();
        rs = stat.executeQuery("select * from test order by id");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hallo", rs.getString(2));
        assertFalse(rs.next());

        rs = stat.executeQuery("select pg_get_userbyid(" + userId + ')');
        rs.next();
        assertEquals("test", rs.getString(1));
        rs.close();

        rs = stat.executeQuery("select currTid2('x', 1)");
        rs.next();
        assertEquals(1, rs.getInt(1));

        rs = stat.executeQuery("select has_table_privilege('TEST', 'READ')");
        rs.next();
        assertTrue(rs.getBoolean(1));

        rs = stat.executeQuery("select has_schema_privilege(1, 'READ')");
        rs.next();
        assertTrue(rs.getBoolean(1));

        rs = stat.executeQuery("select has_database_privilege(1, 'READ')");
        rs.next();
        assertTrue(rs.getBoolean(1));


        rs = stat.executeQuery("select pg_get_userbyid(1000000000)");
        rs.next();
        assertEquals("unknown (OID=1000000000)", rs.getString(1));

        rs = stat.executeQuery("select pg_encoding_to_char(0)");
        rs.next();
        assertEquals("SQL_ASCII", rs.getString(1));

        rs = stat.executeQuery("select pg_encoding_to_char(6)");
        rs.next();
        assertEquals("UTF8", rs.getString(1));

        rs = stat.executeQuery("select pg_encoding_to_char(8)");
        rs.next();
        assertEquals("LATIN1", rs.getString(1));

        rs = stat.executeQuery("select pg_encoding_to_char(20)");
        rs.next();
        assertEquals("UTF8", rs.getString(1));

        rs = stat.executeQuery("select pg_encoding_to_char(40)");
        rs.next();
        assertEquals("", rs.getString(1));

        rs = stat.executeQuery("select 0::regclass");
        rs.next();
        assertEquals(0, rs.getInt(1));

        rs = stat.executeQuery("select pg_get_indexdef(0, 0, false)");
        rs.next();
        assertNull(rs.getString(1));

        rs = stat.executeQuery("select pg_get_indexdef("+indexId+", 0, false)");
        rs.next();
        assertEquals("CREATE INDEX \"public\".\"idx_test_name\" ON \"public\".\"test\""
                + "(\"name\" NULLS LAST, \"id\" NULLS LAST)",
                rs.getString(1));
        rs = stat.executeQuery("select pg_get_indexdef("+indexId+", null, false)");
        rs.next();
        assertNull(rs.getString(1));
        rs = stat.executeQuery("select pg_get_indexdef("+indexId+", 1, false)");
        rs.next();
        assertEquals("name", rs.getString(1));
        rs = stat.executeQuery("select pg_get_indexdef("+indexId+", 2, false)");
        rs.next();
        assertEquals("id", rs.getString(1));

        rs = stat.executeQuery("select * from pg_type where oid = " + PgServer.PG_TYPE_VARCHAR_ARRAY);
        rs.next();
        assertEquals("_varchar", rs.getString("typname"));
        assertEquals("_varchar", rs.getObject("typname"));
        assertEquals("b", rs.getString("typtype"));
        assertEquals(",", rs.getString("typdelim"));
        assertEquals(PgServer.PG_TYPE_VARCHAR, rs.getInt("typelem"));

        stat.setMaxRows(10);
        rs = stat.executeQuery("select * from generate_series(0, 10)");
        assertNRows(rs, 10);
        stat.setMaxRows(0);

        stat.setFetchSize(2);
        rs = stat.executeQuery("select * from generate_series(0, 4)");
        assertNRows(rs, 5);
        rs = stat.executeQuery("select * from generate_series(0, 1)");
        assertNRows(rs, 2);
        stat.setFetchSize(0);

        conn.close();
    }

    private void assertNRows(ResultSet rs, int n) throws SQLException {
        for (int i = 0; i < n; i++) {
            assertTrue(rs.next());
            assertEquals(i, rs.getInt(1));
        }
        assertFalse(rs.next());
    }

    private void testPgClientSimple() throws SQLException {
        Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5535/pgserver?preferQueryMode=simple", "sa", "sa");
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());
        stat.setMaxRows(0);
        stat.execute("create table test2(int integer)");
        stat.execute("drop table test2");
        assertThrows(SQLException.class, stat).execute("drop table test2");
        conn.close();
    }

    private void testKeyAlias() throws SQLException {
        if (!getPgJdbcDriver()) {
            return;
        }
        Server server = createPgServer(
                "-ifNotExists", "-pgPort", "5535", "-pgDaemon", "-key", "pgserver", "mem:pgserver");
        try {
            Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5535/pgserver", "sa", "sa");
            Statement stat = conn.createStatement();

            // confirm that we've got the in memory implementation
            // by creating a table and checking flags
            stat.execute("create table test(id int primary key, name varchar)");
            ResultSet rs = stat.executeQuery(
                    "select storage_type from information_schema.tables " +
                    "where table_name = 'test'");
            assertTrue(rs.next());
            assertEquals("MEMORY", rs.getString(1));

            conn.close();
        } finally {
            server.stop();
        }
    }

    private static Set<Integer> supportedBinaryOids;

    static {
        try {
            supportedBinaryOids = getSupportedBinaryOids();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Set<Integer> getSupportedBinaryOids() throws ReflectiveOperationException {
        Field supportedBinaryOidsField = Class
                .forName("org.postgresql.jdbc.PgConnection")
                .getDeclaredField("SUPPORTED_BINARY_OIDS");
        supportedBinaryOidsField.setAccessible(true);
        return (Set<Integer>) supportedBinaryOidsField.get(null);
    }

    private void testTextualAndBinaryTypes() throws SQLException {
        testTextualAndBinaryTypes(false);
        testTextualAndBinaryTypes(true);
        // additional support of NUMERIC for Npgsql
        supportedBinaryOids.add(1700);
        testTextualAndBinaryTypes(true);
        supportedBinaryOids.remove(1700);
    }

    private void testTextualAndBinaryTypes(boolean binary) throws SQLException {
        if (!getPgJdbcDriver()) {
            return;
        }

        Server server = createPgServer(
                "-ifNotExists", "-pgPort", "5535", "-pgDaemon", "-key", "pgserver", "mem:pgserver");
        try {
            Properties props = new Properties();
            props.setProperty("user", "sa");
            props.setProperty("password", "sa");

            // force binary
            if (binary) {
                props.setProperty("prepareThreshold", "-1");
            }

            Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5535/pgserver", props);
            Statement stat = conn.createStatement();

            stat.execute(
                    "create table test(x1 varchar, x2 int, " +
                    "x3 smallint, x4 bigint, x5 double precision, x6 float, " +
                    "x7 real, x8 boolean, x9 char(3), x10 bytea, " +
                    "x11 date, x12 time, x13 timestamp, x14 numeric(25, 5)," +
                    "x15 time with time zone, x16 timestamp with time zone)");

            PreparedStatement ps = conn.prepareStatement(
                    "insert into test values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            ps.setString(1, "test");
            ps.setInt(2, 12345678);
            ps.setShort(3, (short) 12345);
            ps.setLong(4, 1234567890123L);
            ps.setDouble(5, 123.456);
            ps.setFloat(6, 123.456f);
            ps.setFloat(7, 123.456f);
            ps.setBoolean(8, true);
            ps.setByte(9, (byte) 0xfe);
            ps.setBytes(10, new byte[] { 'a', (byte) 0xfe, '\127', 0, 127, '\\' });
            ps.setDate(11, Date.valueOf("2015-01-31"));
            ps.setTime(12, Time.valueOf("20:11:15"));
            ps.setTimestamp(13, Timestamp.valueOf("2001-10-30 14:16:10.111"));
            ps.setBigDecimal(14, new BigDecimal("12345678901234567890.12345"));
            ps.setTime(15, Time.valueOf("20:11:15"));
            ps.setTimestamp(16, Timestamp.valueOf("2001-10-30 14:16:10.111"));
            ps.execute();
            for (int i = 1; i <= 16; i++) {
                ps.setNull(i, Types.NULL);
            }
            ps.execute();

            ResultSet rs = stat.executeQuery("select * from test");
            assertTrue(rs.next());
            assertEquals("test", rs.getString(1));
            assertEquals(12345678, rs.getInt(2));
            assertEquals((short) 12345, rs.getShort(3));
            assertEquals(1234567890123L, rs.getLong(4));
            assertEquals(123.456, rs.getDouble(5));
            assertEquals(123.456f, rs.getFloat(6));
            assertEquals(123.456f, rs.getFloat(7));
            assertEquals(true, rs.getBoolean(8));
            assertEquals((byte) 0xfe, rs.getByte(9));
            assertEquals(new byte[] { 'a', (byte) 0xfe, '\127', 0, 127, '\\' },
                    rs.getBytes(10));
            assertEquals(Date.valueOf("2015-01-31"), rs.getDate(11));
            assertEquals(Time.valueOf("20:11:15"), rs.getTime(12));
            assertEquals(Timestamp.valueOf("2001-10-30 14:16:10.111"), rs.getTimestamp(13));
            assertEquals(new BigDecimal("12345678901234567890.12345"), rs.getBigDecimal(14));
            assertEquals(Time.valueOf("20:11:15"), rs.getTime(15));
            assertEquals(Timestamp.valueOf("2001-10-30 14:16:10.111"), rs.getTimestamp(16));
            assertTrue(rs.next());
            for (int i = 1; i <= 16; i++) {
                assertNull(rs.getObject(i));
            }
            assertFalse(rs.next());

            conn.close();
        } finally {
            server.stop();
        }
    }

    private void testBinaryNumeric() throws SQLException {
        if (!getPgJdbcDriver()) {
            return;
        }
        Server server = createPgServer(
                "-ifNotExists", "-pgPort", "5535", "-pgDaemon", "-key", "pgserver", "mem:pgserver");
        supportedBinaryOids.add(1700);
        try {
            Properties props = new Properties();
            props.setProperty("user", "sa");
            props.setProperty("password", "sa");
            // force binary
            props.setProperty("prepareThreshold", "-1");

            Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5535/pgserver", props);
            Statement stat = conn.createStatement();

            try (ResultSet rs = stat.executeQuery("SELECT 1E-16383, 1E+1, 1E+89, 1E-16384")) {
                rs.next();
                assertEquals(new BigDecimal("1E-16383"), rs.getBigDecimal(1));
                assertEquals(new BigDecimal("10"), rs.getBigDecimal(2));
                assertEquals(new BigDecimal("10").pow(89), rs.getBigDecimal(3));
                // TODO `SELECT 1E+90, 1E+131071` fails due to PgJDBC issue 1935
                try {
                    rs.getBigDecimal(4);
                    fail();
                } catch (IllegalArgumentException e) {
                    // PgJDBC doesn't support scale greater than 16383
                }
            }
            try (ResultSet rs = stat.executeQuery("SELECT 1E-32768")) {
                fail();
            } catch (SQLException e) {
                assertEquals("22003", e.getSQLState());
            }
            try (ResultSet rs = stat.executeQuery("SELECT 1E+131072")) {
                fail();
            } catch (SQLException e) {
                assertEquals("22003", e.getSQLState());
            }

            conn.close();
        } finally {
            supportedBinaryOids.remove(1700);
            server.stop();
        }
    }

    private void testDateTime() throws SQLException {
        if (!getPgJdbcDriver()) {
            return;
        }
        TimeZone old = TimeZone.getDefault();
        /*
         * java.util.TimeZone doesn't support LMT, so perform this test with
         * fixed time zone offset
         */
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+01"));
        DateTimeUtils.resetCalendar();
        try {
            Server server = createPgServer(
                    "-ifNotExists", "-pgPort", "5535", "-pgDaemon", "-key", "pgserver", "mem:pgserver");
            try {
                Properties props = new Properties();
                props.setProperty("user", "sa");
                props.setProperty("password", "sa");
                // force binary
                props.setProperty("prepareThreshold", "-1");

                Connection conn = DriverManager.getConnection(
                        "jdbc:postgresql://localhost:5535/pgserver", props);
                Statement stat = conn.createStatement();

                stat.execute(
                        "create table test(x1 date, x2 time, x3 timestamp)");

                Date[] dates = { null, Date.valueOf("2017-02-20"),
                        Date.valueOf("1970-01-01"), Date.valueOf("1969-12-31"),
                        Date.valueOf("1940-01-10"), Date.valueOf("1950-11-10"),
                        Date.valueOf("1500-01-01")};
                Time[] times = { null, Time.valueOf("14:15:16"),
                        Time.valueOf("00:00:00"), Time.valueOf("23:59:59"),
                        Time.valueOf("00:10:59"), Time.valueOf("08:30:42"),
                        Time.valueOf("10:00:00")};
                Timestamp[] timestamps = { null, Timestamp.valueOf("2017-02-20 14:15:16.763"),
                        Timestamp.valueOf("1970-01-01 00:00:00"), Timestamp.valueOf("1969-12-31 23:59:59"),
                        Timestamp.valueOf("1940-01-10 00:10:59"), Timestamp.valueOf("1950-11-10 08:30:42.12"),
                        Timestamp.valueOf("1500-01-01 10:00:10")};
                int count = dates.length;

                PreparedStatement ps = conn.prepareStatement(
                        "insert into test values (?,?,?)");
                    for (int i = 0; i < count; i++) {
                    ps.setDate(1, dates[i]);
                    ps.setTime(2, times[i]);
                    ps.setTimestamp(3, timestamps[i]);
                    ps.execute();
                }

                ResultSet rs = stat.executeQuery("select * from test");
                for (int i = 0; i < count; i++) {
                    assertTrue(rs.next());
                    assertEquals(dates[i], rs.getDate(1));
                    assertEquals(times[i], rs.getTime(2));
                    assertEquals(timestamps[i], rs.getTimestamp(3));
                }
                assertFalse(rs.next());

                conn.close();
            } finally {
                server.stop();
            }
        } finally {
            TimeZone.setDefault(old);
            DateTimeUtils.resetCalendar();
        }
    }

    private void testPrepareWithUnspecifiedType() throws Exception {
        if (!getPgJdbcDriver()) {
            return;
        }

        Server server = createPgServer(
                "-ifNotExists", "-pgPort", "5535", "-pgDaemon", "-key", "pgserver", "mem:pgserver");
        try {
            Properties props = new Properties();

            props.setProperty("user", "sa");
            props.setProperty("password", "sa");
            // force server side prepare
            props.setProperty("prepareThreshold", "1");

            Connection conn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5535/pgserver", props);

            Statement stmt = conn.createStatement();
            stmt.executeUpdate("create table t1 (id integer, v timestamp)");
            stmt.close();

            PreparedStatement pstmt = conn.prepareStatement("insert into t1 values(100500, ?)");
            // assertTrue(((PGStatement) pstmt).isUseServerPrepare());
            assertEquals(Types.TIMESTAMP, pstmt.getParameterMetaData().getParameterType(1));

            Timestamp t = new Timestamp(System.currentTimeMillis());
            pstmt.setObject(1, t);
            assertEquals(1, pstmt.executeUpdate());
            pstmt.close();

            pstmt = conn.prepareStatement("SELECT * FROM t1 WHERE v = ?");
            assertEquals(Types.TIMESTAMP, pstmt.getParameterMetaData().getParameterType(1));

            pstmt.setObject(1, t);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(100500, rs.getInt(1));
            rs.close();
            pstmt.close();

            conn.close();
        } finally {
            server.stop();
        }
    }

    private void testOtherPgClients() throws SQLException {
        if (!getPgJdbcDriver()) {
            return;
        }

        Server server = createPgServer(
                "-ifNotExists", "-pgPort", "5535", "-pgDaemon", "-key", "pgserver", "mem:pgserver");
        try (
                Connection conn = DriverManager.getConnection(
                        "jdbc:postgresql://localhost:5535/pgserver", "sa", "sa");
                Statement stat = conn.createStatement();
        ) {
            stat.execute(
                    "create table test(id serial primary key, x1 integer)");

            // pgAdmin
            stat.execute("SET client_min_messages=notice");
            try (ResultSet rs = stat.executeQuery("SELECT set_config('bytea_output','escape',false) " +
                    "FROM pg_settings WHERE name = 'bytea_output'")) {
                assertFalse(rs.next());
            }
            stat.execute("SET client_encoding='UNICODE'");
            try (ResultSet rs = stat.executeQuery("SELECT version()")) {
                assertTrue(rs.next());
                assertNotNull(rs.getString("version"));
            }
            try (ResultSet rs = stat.executeQuery("SELECT " +
                    "db.oid as did, db.datname, db.datallowconn, " +
                    "pg_encoding_to_char(db.encoding) AS serverencoding, " +
                    "has_database_privilege(db.oid, 'CREATE') as cancreate, datlastsysoid " +
                    "FROM pg_database db WHERE db.datname = current_database()")) {
                assertTrue(rs.next());
                assertEquals("pgserver", rs.getString("datname"));
                assertFalse(rs.next());
            }
            try (ResultSet rs = stat.executeQuery("SELECT " +
                    "oid as id, rolname as name, rolsuper as is_superuser, " +
                    "CASE WHEN rolsuper THEN true ELSE rolcreaterole END as can_create_role, " +
                    "CASE WHEN rolsuper THEN true ELSE rolcreatedb END as can_create_db " +
                    "FROM pg_catalog.pg_roles WHERE rolname = current_user")) {
                assertTrue(rs.next());
                assertEquals("sa", rs.getString("name"));
                assertFalse(rs.next());
            }
            try (ResultSet rs = stat.executeQuery("SELECT " +
                    "db.oid as did, db.datname as name, ta.spcname as spcname, db.datallowconn, " +
                    "has_database_privilege(db.oid, 'CREATE') as cancreate, datdba as owner " +
                    "FROM pg_database db LEFT OUTER JOIN pg_tablespace ta ON db.dattablespace = ta.oid " +
                    "WHERE db.oid > 100000::OID")) {
                assertTrue(rs.next());
                assertEquals("pgserver", rs.getString("name"));
                assertFalse(rs.next());
            }
            try (ResultSet rs = stat.executeQuery("SELECT nsp.oid, nsp.nspname as name, " +
                    "has_schema_privilege(nsp.oid, 'CREATE') as can_create, " +
                    "has_schema_privilege(nsp.oid, 'USAGE') as has_usage " +
                    "FROM pg_namespace nsp WHERE nspname NOT LIKE 'pg\\_%' AND NOT (" +
                    "(nsp.nspname = 'pg_catalog' AND EXISTS (SELECT 1 FROM pg_class " +
                    "WHERE relname = 'pg_class' AND relnamespace = nsp.oid LIMIT 1)) OR " +
                    "(nsp.nspname = 'pgagent' AND EXISTS (SELECT 1 FROM pg_class " +
                    "WHERE relname = 'pga_job' AND relnamespace = nsp.oid LIMIT 1)) OR " +
                    "(nsp.nspname = 'information_schema' AND EXISTS (SELECT 1 FROM pg_class " +
                    "WHERE relname = 'tables' AND relnamespace = nsp.oid LIMIT 1))" +
                    ") ORDER BY nspname")) {
                assertTrue(rs.next());
                assertEquals("public", rs.getString("name"));
                assertFalse(rs.next());
            }
            try (ResultSet rs = stat.executeQuery("SELECT format_type(23, NULL)")) {
                assertTrue(rs.next());
                assertEquals("INTEGER", rs.getString(1));
                assertFalse(rs.next());
            }
            // pgAdmin sends `SET LOCAL join_collapse_limit=8`, but `LOCAL` is not supported yet
            stat.execute("SET join_collapse_limit=8");

            // HeidiSQL
            try (ResultSet rs = stat.executeQuery("SHOW ssl")) {
                assertTrue(rs.next());
                assertEquals("off", rs.getString(1));
            }
            stat.execute("SET search_path TO 'public', '$user'");
            try (ResultSet rs = stat.executeQuery("SELECT *, NULL AS data_length, " +
                    "pg_relation_size(QUOTE_IDENT(t.TABLE_SCHEMA) || '.' || QUOTE_IDENT(t.TABLE_NAME))::bigint " +
                    "AS index_length, " +
                    "c.reltuples, obj_description(c.oid) AS comment " +
                    "FROM \"information_schema\".\"tables\" AS t " +
                    "LEFT JOIN \"pg_namespace\" n ON t.table_schema = n.nspname " +
                    "LEFT JOIN \"pg_class\" c ON n.oid = c.relnamespace AND c.relname=t.table_name " +
                    "WHERE t.\"table_schema\"='public'")) {
                assertTrue(rs.next());
                assertEquals("test", rs.getString("table_name"));
                assertTrue(rs.getLong("index_length") >= 0L); // test pg_relation_size()
                assertNull(rs.getString("comment")); // test obj_description()
            }
            try (ResultSet rs = stat.executeQuery("SELECT \"p\".\"proname\", \"p\".\"proargtypes\" " +
                    "FROM \"pg_catalog\".\"pg_namespace\" AS \"n\" " +
                    "JOIN \"pg_catalog\".\"pg_proc\" AS \"p\" ON \"p\".\"pronamespace\" = \"n\".\"oid\" " +
                    "WHERE \"n\".\"nspname\"='public'")) {
                assertFalse(rs.next()); // "pg_proc" always empty
            }
            try (ResultSet rs = stat.executeQuery("SELECT DISTINCT a.attname AS column_name, " +
                    "a.attnum, a.atttypid, FORMAT_TYPE(a.atttypid, a.atttypmod) AS data_type, " +
                    "CASE a.attnotnull WHEN false THEN 'YES' ELSE 'NO' END AS IS_NULLABLE, " +
                    "com.description AS column_comment, pg_get_expr(def.adbin, def.adrelid) AS column_default, " +
                    "NULL AS character_maximum_length FROM pg_attribute AS a " +
                    "JOIN pg_class AS pgc ON pgc.oid = a.attrelid " +
                    "LEFT JOIN pg_description AS com ON (pgc.oid = com.objoid AND a.attnum = com.objsubid) " +
                    "LEFT JOIN pg_attrdef AS def ON (a.attrelid = def.adrelid AND a.attnum = def.adnum) " +
                    "WHERE a.attnum > 0 AND pgc.oid = a.attrelid AND pg_table_is_visible(pgc.oid) " +
                    "AND NOT a.attisdropped AND pgc.relname = 'test' ORDER BY a.attnum")) {
                assertTrue(rs.next());
                assertEquals("id", rs.getString("column_name"));
                assertTrue(rs.next());
                assertEquals("x1", rs.getString("column_name"));
                assertFalse(rs.next());
            }
            try (ResultSet rs = stat.executeQuery("SHOW ALL")) {
                ResultSetMetaData rsMeta = rs.getMetaData();
                assertEquals("name", rsMeta.getColumnName(1));
                assertEquals("setting", rsMeta.getColumnName(2));
            }

            // DBeaver
            try (ResultSet rs = stat.executeQuery("SELECT t.oid,t.*,c.relkind FROM pg_catalog.pg_type t " +
                    "LEFT OUTER JOIN pg_class c ON c.oid=t.typrelid WHERE typnamespace=-1000")) {
                // just no exception
            }
            stat.execute("SET search_path TO 'ab', 'c\"d', 'e''f'");
            try (ResultSet rs = stat.executeQuery("SHOW search_path")) {
                assertTrue(rs.next());
                assertEquals("pg_catalog, ab, \"c\"\"d\", \"e'f\"", rs.getString("search_path"));
            }
            stat.execute("SET search_path TO ab, \"c\"\"d\", \"e'f\"");
            try (ResultSet rs = stat.executeQuery("SHOW search_path")) {
                assertTrue(rs.next());
                assertEquals("pg_catalog, ab, \"c\"\"d\", \"e'f\"", rs.getString("search_path"));
            }
            int oid;
            try (ResultSet rs = stat.executeQuery("SELECT oid FROM pg_class WHERE relname = 'test'")) {
                rs.next();
                oid = rs.getInt("oid");
            }
            try (ResultSet rs = stat.executeQuery("SELECT i.*,i.indkey as keys," +
                    "c.relname,c.relnamespace,c.relam,c.reltablespace," +
                    "tc.relname as tabrelname,dsc.description," +
                    "pg_catalog.pg_get_expr(i.indpred, i.indrelid) as pred_expr," +
                    "pg_catalog.pg_get_expr(i.indexprs, i.indrelid, true) as expr," +
                    "pg_catalog.pg_relation_size(i.indexrelid) as index_rel_size," +
                    "pg_catalog.pg_stat_get_numscans(i.indexrelid) as index_num_scans " +
                    "FROM pg_catalog.pg_index i " +
                    "INNER JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid " +
                    "INNER JOIN pg_catalog.pg_class tc ON tc.oid=i.indrelid " +
                    "LEFT OUTER JOIN pg_catalog.pg_description dsc ON i.indexrelid=dsc.objoid " +
                    "WHERE i.indrelid=" + oid + " ORDER BY c.relname")) {
                // pg_index is empty
                assertFalse(rs.next());
            }
            try (ResultSet rs = stat.executeQuery("SELECT c.oid,c.*," +
                    "t.relname as tabrelname,rt.relnamespace as refnamespace,d.description " +
                    "FROM pg_catalog.pg_constraint c " +
                    "INNER JOIN pg_catalog.pg_class t ON t.oid=c.conrelid " +
                    "LEFT OUTER JOIN pg_catalog.pg_class rt ON rt.oid=c.confrelid " +
                    "LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid " +
                    "AND d.objsubid=0 AND d.classoid='pg_constraint'::regclass WHERE c.conrelid=" + oid)) {
                assertTrue(rs.next());
                assertEquals("test", rs.getString("tabrelname"));
                assertEquals("p", rs.getString("contype"));
                assertEquals(Short.valueOf((short) 1), ((Object[]) rs.getArray("conkey").getArray())[0]);
            }
        } finally {
            server.stop();
        }
    }

    private void testArray() throws Exception {
        if (!getPgJdbcDriver()) {
            return;
        }

        Server server = createPgServer(
                "-ifNotExists", "-pgPort", "5535", "-pgDaemon", "-key", "pgserver", "mem:pgserver");
        try (
                Connection conn = DriverManager.getConnection(
                        "jdbc:postgresql://localhost:5535/pgserver", "sa", "sa");
                Statement stat = conn.createStatement();
        ) {
            stat.execute("CREATE TABLE test (id int primary key, x1 varchar array)");
            stat.execute("INSERT INTO test (id, x1) VALUES (1, ARRAY['abc', 'd\\\"e', '{,}'])");
            try (ResultSet rs = stat.executeQuery(
                    "SELECT x1 FROM test WHERE id = 1")) {
                assertTrue(rs.next());
                Object[] arr = (Object[]) rs.getArray(1).getArray();
                assertEquals("abc", arr[0]);
                assertEquals("d\\\"e", arr[1]);
                assertEquals("{,}", arr[2]);
            }
            try (ResultSet rs = stat.executeQuery(
                    "SELECT data_type FROM information_schema.columns WHERE table_schema = 'pg_catalog' " +
                    "AND table_name = 'pg_database' AND column_name = 'datacl'")) {
                assertTrue(rs.next());
                assertEquals("array", rs.getString(1));
            }
            try (ResultSet rs = stat.executeQuery(
                    "SELECT data_type FROM information_schema.columns WHERE table_schema = 'pg_catalog' " +
                    "AND table_name = 'pg_tablespace' AND column_name = 'spcacl'")) {
                assertTrue(rs.next());
                assertEquals("array", rs.getString(1));
            }
        } finally {
            server.stop();
        }
    }

}
