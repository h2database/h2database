/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.mvstore.db.MVTableEngine;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.util.Task;

/**
 * Tests the MVStore in a database.
 */
public class TestMVTableEngine extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        testEncryption();
        testReadOnly();
        testReuseDiskSpace();
        testDataTypes();
        testLocking();
        testSimple();
    }

    private void testEncryption() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        String dbName = "mvstore" +
                ";DEFAULT_TABLE_ENGINE=org.h2.mvstore.db.MVTableEngine";
        Connection conn;
        Statement stat;
        String url = getURL(dbName + ";CIPHER=AES", true);
        String user = "sa";
        String password = "123 123";
        conn = DriverManager.getConnection(url, user, password);
        stat = conn.createStatement();
        stat.execute("create table test(id int)");
        conn.close();
        conn = DriverManager.getConnection(url, user, password);
        stat = conn.createStatement();
        stat.execute("select * from test");
        conn.close();
        FileUtils.deleteRecursive(getBaseDir(), true);
    }

    private void testReadOnly() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        String dbName = "mvstore" +
                ";DEFAULT_TABLE_ENGINE=org.h2.mvstore.db.MVTableEngine";
        Connection conn;
        Statement stat;
        conn = getConnection(dbName);
        stat = conn.createStatement();
        stat.execute("create table test(id int)");
        conn.close();
        FileUtils.setReadOnly(getBaseDir() + "/mvstore.h2.db");
        conn = getConnection(dbName);
        for (MVTableEngine.Store s : MVTableEngine.getStores()) {
            assertTrue(s.getStore().isReadOnly());
        }
        conn.close();
        FileUtils.deleteRecursive(getBaseDir(), true);
    }

    private void testReuseDiskSpace() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        String dbName = "mvstore" +
                ";DEFAULT_TABLE_ENGINE=org.h2.mvstore.db.MVTableEngine";
        Connection conn;
        Statement stat;
        long maxSizeHalf = 0;
        for (int i = 0; i < 10; i++) {
            conn = getConnection(dbName);
            for (MVTableEngine.Store s : MVTableEngine.getStores()) {
                s.getStore().setRetentionTime(0);
            }
            stat = conn.createStatement();
            stat.execute("create table test(id int primary key, data varchar)");
            stat.execute("insert into test select x, space(1000) from system_range(1, 1000)");
            stat.execute("drop table test");
            conn.close();
            long size = FileUtils.size(getBaseDir() + "/mvstore"
                    + Constants.SUFFIX_MV_FILE);
            if (i < 6) {
                maxSizeHalf = Math.max(size, maxSizeHalf);
            } else if (size > maxSizeHalf) {
                fail("size: " + size + " max: " + maxSizeHalf);
            }
        }
    }

    private void testDataTypes() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        String dbName = "mvstore" +
                ";DEFAULT_TABLE_ENGINE=org.h2.mvstore.db.MVTableEngine";
        Connection conn = getConnection(dbName);
        Statement stat = conn.createStatement();

        stat.execute("create table test(id int primary key, " +
                "vc varchar," +
                "ch char(10)," +
                "bo boolean," +
                "by tinyint," +
                "sm smallint," +
                "bi bigint," +
                "de decimal," +
                "re real,"+
                "do double," +
                "ti time," +
                "da date," +
                "ts timestamp," +
                "bin binary," +
                "uu uuid," +
                "bl blob," +
                "cl clob)");
        stat.execute("insert into test values(1000, '', '', null, 0, 0, 0, "
                + "9, 2, 3, '10:00:00', '2001-01-01', "
                + "'2010-10-10 10:10:10', x'00', 0, x'b1', 'clob')");
        stat.execute("insert into test values(1, 'vc', 'ch', true, 8, 16, 64, "
                + "123.00, 64.0, 32.0, '10:00:00', '2001-01-01', "
                + "'2010-10-10 10:10:10', x'00', 0, x'b1', 'clob')");
        stat.execute("insert into test values(-1, 'quite a long string \u1234 \u00ff', 'ch', false, -8, -16, -64, "
                + "0, 0, 0, '10:00:00', '2001-01-01', "
                + "'2010-10-10 10:10:10', SECURE_RAND(100), 0, x'b1', 'clob')");
        stat.execute("insert into test values(-1000, space(1000), 'ch', false, -8, -16, -64, "
                + "1, 1, 1, '10:00:00', '2001-01-01', "
                + "'2010-10-10 10:10:10', SECURE_RAND(100), 0, x'b1', 'clob')");
        if (!config.memory) {
            conn.close();
            conn = getConnection(dbName);
            stat = conn.createStatement();
        }
        ResultSet rs;
        rs = stat.executeQuery("select * from test order by id desc");
        rs.next();
        assertEquals(1000, rs.getInt(1));
        assertEquals("", rs.getString(2));
        assertEquals("", rs.getString(3));
        assertFalse(rs.getBoolean(4));
        assertEquals(0, rs.getByte(5));
        assertEquals(0, rs.getShort(6));
        assertEquals(0, rs.getLong(7));
        assertEquals("9", rs.getBigDecimal(8).toString());
        assertEquals(2d, rs.getDouble(9));
        assertEquals(3d, rs.getFloat(10));
        assertEquals("10:00:00", rs.getString(11));
        assertEquals("2001-01-01", rs.getString(12));
        assertEquals("2010-10-10 10:10:10.0", rs.getString(13));
        assertEquals(1, rs.getBytes(14).length);
        assertEquals("00000000-0000-0000-0000-000000000000", rs.getString(15));
        assertEquals(1, rs.getBytes(16).length);
        assertEquals("clob", rs.getString(17));
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("vc", rs.getString(2));
        assertEquals("ch", rs.getString(3));
        assertTrue(rs.getBoolean(4));
        assertEquals(8, rs.getByte(5));
        assertEquals(16, rs.getShort(6));
        assertEquals(64, rs.getLong(7));
        assertEquals("123.00", rs.getBigDecimal(8).toString());
        assertEquals(64d, rs.getDouble(9));
        assertEquals(32d, rs.getFloat(10));
        assertEquals("10:00:00", rs.getString(11));
        assertEquals("2001-01-01", rs.getString(12));
        assertEquals("2010-10-10 10:10:10.0", rs.getString(13));
        assertEquals(1, rs.getBytes(14).length);
        assertEquals("00000000-0000-0000-0000-000000000000", rs.getString(15));
        assertEquals(1, rs.getBytes(16).length);
        assertEquals("clob", rs.getString(17));
        rs.next();
        assertEquals(-1, rs.getInt(1));
        assertEquals("quite a long string \u1234 \u00ff", rs.getString(2));
        assertEquals("ch", rs.getString(3));
        assertFalse(rs.getBoolean(4));
        assertEquals(-8, rs.getByte(5));
        assertEquals(-16, rs.getShort(6));
        assertEquals(-64, rs.getLong(7));
        assertEquals("0", rs.getBigDecimal(8).toString());
        assertEquals(0.0d, rs.getDouble(9));
        assertEquals(0.0d, rs.getFloat(10));
        assertEquals("10:00:00", rs.getString(11));
        assertEquals("2001-01-01", rs.getString(12));
        assertEquals("2010-10-10 10:10:10.0", rs.getString(13));
        assertEquals(100, rs.getBytes(14).length);
        assertEquals("00000000-0000-0000-0000-000000000000", rs.getString(15));
        assertEquals(1, rs.getBytes(16).length);
        assertEquals("clob", rs.getString(17));
        rs.next();
        assertEquals(-1000, rs.getInt(1));
        assertEquals(1000, rs.getString(2).length());
        assertEquals("ch", rs.getString(3));
        assertFalse(rs.getBoolean(4));
        assertEquals(-8, rs.getByte(5));
        assertEquals(-16, rs.getShort(6));
        assertEquals(-64, rs.getLong(7));
        assertEquals("1", rs.getBigDecimal(8).toString());
        assertEquals(1.0d, rs.getDouble(9));
        assertEquals(1.0d, rs.getFloat(10));
        assertEquals("10:00:00", rs.getString(11));
        assertEquals("2001-01-01", rs.getString(12));
        assertEquals("2010-10-10 10:10:10.0", rs.getString(13));
        assertEquals(100, rs.getBytes(14).length);
        assertEquals("00000000-0000-0000-0000-000000000000", rs.getString(15));
        assertEquals(1, rs.getBytes(16).length);
        assertEquals("clob", rs.getString(17));

        stat.execute("drop table test");

        stat.execute("create table test(id int, obj object, rs result_set, arr array, ig varchar_ignorecase)");
        PreparedStatement prep = conn.prepareStatement("insert into test values(?, ?, ?, ?, ?)");
        prep.setInt(1, 1);
        prep.setObject(2, new java.lang.AssertionError());
        prep.setObject(3, stat.executeQuery("select 1 from dual"));
        prep.setObject(4, new Object[]{1, 2});
        prep.setObject(5, "test");
        prep.execute();
        prep.setInt(1, 1);
        prep.setObject(2, new java.lang.AssertionError());
        prep.setObject(3, stat.executeQuery("select 1 from dual"));
        prep.setObject(4, new Object[]{new BigDecimal(new String(new char[1000]).replace((char) 0, '1'))});
        prep.setObject(5, "test");
        prep.execute();
        if (!config.memory) {
            conn.close();
            conn = getConnection(dbName);
            stat = conn.createStatement();
        }
        stat.execute("select * from test");

        rs = stat.executeQuery("script");
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertTrue(count < 10);

        stat.execute("drop table test");
        conn.close();
    }

    private void testLocking() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        String dbName = "mvstore" +
                ";DEFAULT_TABLE_ENGINE=org.h2.mvstore.db.MVTableEngine";
        Connection conn = getConnection(dbName);
        Statement stat = conn.createStatement();
        stat.execute("set lock_timeout 1000");

        stat.execute("create table a(id int primary key, name varchar)");
        stat.execute("create table b(id int primary key, name varchar)");

        Connection conn1 = getConnection(dbName);
        final Statement stat1 = conn1.createStatement();
        stat1.execute("set lock_timeout 1000");

        conn.setAutoCommit(false);
        conn1.setAutoCommit(false);
        stat.execute("insert into a values(1, 'Hello')");
        stat1.execute("insert into b values(1, 'Hello')");
        Task t = new Task() {
            @Override
            public void call() throws Exception {
                stat1.execute("insert into a values(2, 'World')");
            }
        };
        t.execute();
        try {
            stat.execute("insert into b values(2, 'World')");
            throw t.getException();
        } catch (SQLException e) {
            assertEquals(e.toString(), ErrorCode.DEADLOCK_1, e.getErrorCode());
        }

        conn1.close();
        conn.close();
    }

    private void testSimple() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        String dbName = "mvstore" +
                ";DEFAULT_TABLE_ENGINE=org.h2.mvstore.db.MVTableEngine";
        Connection conn = getConnection(dbName);
        Statement stat = conn.createStatement();
        // create table test(id int, name varchar)
        // engine "org.h2.mvstore.db.MVStoreTableEngine"
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test values(1, 'Hello'), (2, 'World')");
        ResultSet rs = stat.executeQuery("select *, _rowid_ from test");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertEquals(1, rs.getInt(3));

        if (!config.memory) {
            conn.close();
            conn = getConnection(dbName);
            stat = conn.createStatement();
        }

        rs = stat.executeQuery("select * from test order by id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("World", rs.getString(2));
        assertFalse(rs.next());

        stat.execute("create unique index idx_name on test(name)");
        rs = stat.executeQuery("select * from test where name = 'Hello' order by name");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());

        try {
            stat.execute("insert into test(id, name) values(10, 'Hello')");
            fail();
        } catch (SQLException e) {
            assertEquals(ErrorCode.DUPLICATE_KEY_1, e.getErrorCode());
        }

        rs = stat.executeQuery("select min(id), max(id), min(name), max(name) from test");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertEquals("Hello", rs.getString(3));
        assertEquals("World", rs.getString(4));
        assertFalse(rs.next());

        stat.execute("delete from test where id = 2");
        rs = stat.executeQuery("select * from test order by id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());

        stat.execute("alter table test add column firstName varchar");
        rs = stat.executeQuery("select * from test where name = 'Hello'");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());

        if (!config.memory) {
            conn.close();
            conn = getConnection(dbName);
            stat = conn.createStatement();
        }

        rs = stat.executeQuery("select * from test order by id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertFalse(rs.next());

        stat.execute("truncate table test");
        rs = stat.executeQuery("select * from test order by id");
        assertFalse(rs.next());

        rs = stat.executeQuery("select count(*) from test");
        rs.next();
        assertEquals(0, rs.getInt(1));
        stat.execute("insert into test(id) select x from system_range(1, 3000)");
        rs = stat.executeQuery("select count(*) from test");
        rs.next();
        assertEquals(3000, rs.getInt(1));
        try {
            stat.execute("insert into test(id) values(1)");
            fail();
        } catch (SQLException e) {
            assertEquals(ErrorCode.DUPLICATE_KEY_1, e.getErrorCode());
        }
    }

}
