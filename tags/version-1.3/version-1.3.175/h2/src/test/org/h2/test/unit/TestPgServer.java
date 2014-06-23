/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.h2.test.TestBase;
import org.h2.tools.Server;

/**
 * Tests the PostgreSQL server protocol compliant implementation.
 */
public class TestPgServer extends TestBase {

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
        testLowerCaseIdentifiers();
        testPgAdapter();
        testKeyAlias();
        testKeyAlias();
        testCancelQuery();
        testBinaryTypes();
    }

    private void testLowerCaseIdentifiers() throws SQLException {
        if (!getPgJdbcDriver()) {
            return;
        }
        deleteDb("test");
        Connection conn = getConnection("test;DATABASE_TO_UPPER=false", "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int, name varchar(255))");
        Server server = Server.createPgServer("-baseDir", getBaseDir(), "-pgPort", "5535", "-pgDaemon");
        server.start();
        try {
            Connection conn2;
            conn2 = DriverManager.getConnection("jdbc:postgresql://localhost:5535/test", "sa", "sa");
            stat = conn2.createStatement();
            stat.execute("select * from test");
            conn2.close();
        } finally {
            server.stop();
        }
        conn.close();
        deleteDb("test");
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

    private void testPgAdapter() throws SQLException {
        deleteDb("test");
        Server server = Server.createPgServer("-baseDir", getBaseDir(), "-pgPort", "5535", "-pgDaemon");
        assertEquals(5535, server.getPort());
        assertEquals("Not started", server.getStatus());
        server.start();
        assertStartsWith(server.getStatus(), "PG server running at pg://");
        try {
            if (getPgJdbcDriver()) {
                testPgClient();
            }
        } finally {
            server.stop();
        }
    }

    private void testCancelQuery() throws Exception {
        if (!getPgJdbcDriver()) {
            return;
        }

        Server server = Server.createPgServer("-pgPort", "5535", "-pgDaemon", "-key", "test", "mem:test");
        server.start();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5535/test", "sa", "sa");
            final Statement stat = conn.createStatement();
            stat.execute("create alias sleep for \"java.lang.Thread.sleep\"");

            // create a table with 200 rows (cancel interval is 127)
            stat.execute("create table test(id int)");
            for (int i = 0; i < 200; i++) {
                stat.execute("insert into test (id) values (rand())");
            }

            Future<Boolean> future = executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws SQLException {
                    return stat.execute("select id, sleep(5) from test");
                }
            });

            // give it a little time to start and then cancel it
            Thread.sleep(100);
            stat.cancel();

            try {
                future.get();
                throw new IllegalStateException();
            } catch (ExecutionException e) {
                assertStartsWith(e.getCause().getMessage(), "ERROR: canceling statement due to user request");
            } finally {
                conn.close();
            }
        } finally {
            server.stop();
            executor.shutdown();
        }
        deleteDb("test");
    }

    private void testPgClient() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5535/test", "sa", "sa");
        Statement stat = conn.createStatement();
        assertThrows(SQLException.class, stat).
                execute("select ***");
        stat.execute("create user test password 'test'");
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("create index idx_test_name on test(name, id)");
        stat.execute("grant all on test to test");
        stat.close();
        conn.close();

        conn = DriverManager.getConnection("jdbc:postgresql://localhost:5535/test", "test", "test");
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
        rs = stat.executeQuery("select * from test");
        rs.next();

        ResultSetMetaData rsMeta = rs.getMetaData();
        assertEquals(Types.INTEGER, rsMeta.getColumnType(1));
        assertEquals(Types.VARCHAR, rsMeta.getColumnType(2));

        prep.close();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());
        prep = conn.prepareStatement("select * from test where id = ? and name = ?");
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
        rs.next();
        assertEquals("TEST", rs.getString("TABLE_NAME"));
        assertFalse(rs.next());
        rs = dbMeta.getColumns(null, null, "TEST", null);
        rs.next();
        assertEquals("ID", rs.getString("COLUMN_NAME"));
        rs.next();
        assertEquals("NAME", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
        rs = dbMeta.getIndexInfo(null, null, "TEST", false, false);
        // index info is currently disabled
        // rs.next();
        // assertEquals("TEST", rs.getString("TABLE_NAME"));
        // rs.next();
        // assertEquals("TEST", rs.getString("TABLE_NAME"));
        assertFalse(rs.next());
        rs = stat.executeQuery("select version(), pg_postmaster_start_time(), current_schema()");
        rs.next();
        String s = rs.getString(1);
        assertTrue(s.indexOf("H2") >= 0);
        assertTrue(s.indexOf("PostgreSQL") >= 0);
        s = rs.getString(2);
        s = rs.getString(3);
        assertEquals(s, "PUBLIC");
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

        rs = stat.executeQuery("select id, name, pg_get_userbyid(id) from information_schema.users order by id");
        rs.next();
        assertEquals(rs.getString(2), rs.getString(3));
        assertFalse(rs.next());
        rs.close();

        rs = stat.executeQuery("select currTid2('x', 1)");
        rs.next();
        assertEquals(1, rs.getInt(1));

        rs = stat.executeQuery("select has_table_privilege('TEST', 'READ')");
        rs.next();
        assertTrue(rs.getBoolean(1));

        rs = stat.executeQuery("select has_database_privilege(1, 'READ')");
        rs.next();
        assertTrue(rs.getBoolean(1));


        rs = stat.executeQuery("select pg_get_userbyid(-1)");
        rs.next();
        assertEquals(null, rs.getString(1));

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

        rs = stat.executeQuery("select pg_get_oid('\"WRONG\"')");
        rs.next();
        assertEquals(0, rs.getInt(1));

        rs = stat.executeQuery("select pg_get_oid('TEST')");
        rs.next();
        assertTrue(rs.getInt(1) > 0);

        rs = stat.executeQuery("select pg_get_indexdef(0, 0, false)");
        rs.next();
        assertEquals("", rs.getString(1));

        rs = stat.executeQuery("select id from information_schema.indexes where index_name='IDX_TEST_NAME'");
        rs.next();
        int indexId = rs.getInt(1);

        rs = stat.executeQuery("select pg_get_indexdef("+indexId+", 0, false)");
        rs.next();
        assertEquals("CREATE INDEX PUBLIC.IDX_TEST_NAME ON PUBLIC.TEST(NAME, ID)", rs.getString(1));
        rs = stat.executeQuery("select pg_get_indexdef("+indexId+", null, false)");
        rs.next();
        assertEquals("CREATE INDEX PUBLIC.IDX_TEST_NAME ON PUBLIC.TEST(NAME, ID)", rs.getString(1));
        rs = stat.executeQuery("select pg_get_indexdef("+indexId+", 1, false)");
        rs.next();
        assertEquals("NAME", rs.getString(1));
        rs = stat.executeQuery("select pg_get_indexdef("+indexId+", 2, false)");
        rs.next();
        assertEquals("ID", rs.getString(1));

        conn.close();
    }

    private void testKeyAlias() throws SQLException {
        if (!getPgJdbcDriver()) {
            return;
        }
        Server server = Server.createPgServer("-pgPort", "5535", "-pgDaemon", "-key", "test", "mem:test");
        server.start();
        try {
            Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5535/test", "sa", "sa");
            Statement stat = conn.createStatement();

            // confirm that we've got the in memory implementation
            // by creating a table and checking flags
            stat.execute("create table test(id int primary key, name varchar)");
            ResultSet rs = stat.executeQuery("select storage_type from information_schema.tables where table_name = 'TEST'");
            assertTrue(rs.next());
            assertEquals("MEMORY", rs.getString(1));

            conn.close();
        } finally {
            server.stop();
        }
    }

    private void testBinaryTypes() throws SQLException {
        if (!getPgJdbcDriver()) {
            return;
        }

        Server server = Server.createPgServer("-pgPort", "5535", "-pgDaemon", "-key", "test", "mem:test");
        server.start();
        try {
            Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5535/test", "sa", "sa");
            Statement stat = conn.createStatement();

            stat.execute("create table test(x1 varchar, x2 int, x3 smallint, x4 bigint, x5 double, x6 float, " +
                "x7 real, x8 boolean, x9 char, x10 bytea)");

            PreparedStatement ps = conn.prepareStatement("insert into test values (?,?,?,?,?,?,?,?,?,?)");
            ps.setString(1, "test");
            ps.setInt(2, 12345678);
            ps.setShort(3, (short) 12345);
            ps.setLong(4, 1234567890123L);
            ps.setDouble(5, 123.456);
            ps.setFloat(6, 123.456f);
            ps.setDouble(7, 123.456);
            ps.setBoolean(8, true);
            ps.setByte(9, (byte) 0xfe);
            ps.setBytes(10, new byte[] { 'a', (byte) 0xfe, '\127' });
            ps.execute();

            ResultSet rs = stat.executeQuery("select * from test");
            assertTrue(rs.next());
            assertEquals("test", rs.getString(1));
            assertEquals(12345678, rs.getInt(2));
            assertEquals((short) 12345, rs.getShort(3));
            assertEquals(1234567890123L, rs.getLong(4));
            assertEquals(123.456, rs.getDouble(5));
            assertEquals(123.456f, rs.getFloat(6));
            assertEquals(123.456, rs.getDouble(7));
            assertEquals(true, rs.getBoolean(8));
            assertEquals((byte) 0xfe, rs.getByte(9));
            assertEquals(new byte[] { 'a', (byte) 0xfe, '\127' },
                    rs.getBytes(10));

            conn.close();
        } finally {
            server.stop();
        }
    }
}
