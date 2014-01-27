/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.store;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;

import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.jdbc.JdbcConnection;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.db.TransactionStore;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Recover;
import org.h2.tools.Restore;
import org.h2.util.JdbcUtils;
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

    @Override
    public void test() throws Exception {
        testGarbageCollectionForLOB();
        testSpatial();
        testCount();
        testMinMaxWithNull();
        testTimeout();
        testExplainAnalyze();
        testTransactionLogUsuallyNotStored();
        testShrinkDatabaseFile();
        testTwoPhaseCommit();
        testRecover();
        testSeparateKey();
        testRollback();
        testRollbackAfterCrash();
        testReferentialIntegrity();
        testWriteDelay();
        testAutoCommit();
        testReopen();
        testBlob();
        testExclusiveLock();
        testEncryption();
        testReadOnly();
        testReuseDiskSpace();
        testDataTypes();
        testLocking();
        testSimple();
    }

    private void testGarbageCollectionForLOB() throws SQLException {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;
        String url = "mvstore;MV_STORE=TRUE";
        url = getURL(url, true);
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id int, data blob)");
        stat.execute("insert into test select x, repeat('0', 10000) from system_range(1, 10)");
        stat.execute("drop table test");
        stat.equals("call @temp := cast(repeat('0', 10000) as blob)");
        stat.execute("create table test2(id int, data blob)");
        PreparedStatement prep = conn.prepareStatement("insert into test2 values(?, ?)");
        prep.setInt(1, 1);
        assertThrows(ErrorCode.IO_EXCEPTION_1, prep).
            setBinaryStream(1, createFailingStream(new IOException()));
        prep.setInt(1, 2);
        assertThrows(ErrorCode.IO_EXCEPTION_1, prep).
            setBinaryStream(1, createFailingStream(new IllegalStateException()));
        conn.close();
        MVStore s = MVStore.open(getBaseDir()+ "/mvstore.mv.db");
        assertTrue(s.hasMap("lobData"));
        MVMap<Long, byte[]> lobData = s.openMap("lobData");
        assertEquals(0, lobData.sizeAsLong());
        assertTrue(s.hasMap("lobMap"));
        MVMap<Long, byte[]> lobMap = s.openMap("lobMap");
        assertEquals(0, lobMap.sizeAsLong());
        assertTrue(s.hasMap("lobRef"));
        MVMap<Long, byte[]> lobRef = s.openMap("lobRef");
        assertEquals(0, lobRef.sizeAsLong());
        s.close();
    }

    private void testSpatial() throws SQLException {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;
        String url = "mvstore;MV_STORE=TRUE";
        url = getURL(url, true);
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("call rand(1)");
        stat.execute("create table coordinates as select rand()*50 x, " +
                "rand()*50 y from system_range(1, 5000)");
        stat.execute("create table test(id identity, data geometry)");
        stat.execute("create spatial index on test(data)");
        stat.execute("insert into test(data) select 'polygon(('||" +
                "(1+x)||' '||(1+y)||', '||(2+x)||' '||(2+y)||', "+
                "'||(3+x)||' '||(1+y)||', '||(1+x)||' '||(1+y)||'))' from coordinates;");
        conn.close();
    }

    private void testCount() throws Exception {
        if (config.memory) {
            return;
        }

        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Connection conn2;
        Statement stat;
        Statement stat2;
        String url = "mvstore;MV_STORE=TRUE;MVCC=TRUE";
        url = getURL(url, true);
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("create table test2(id int)");
        stat.execute("insert into test select x from system_range(1, 10000)");
        conn.close();

        ResultSet rs;
        String plan;

        conn2 = getConnection(url);
        stat2 = conn2.createStatement();
        rs = stat2.executeQuery("explain analyze select count(*) from test");
        rs.next();
        plan = rs.getString(1);
        assertTrue(plan, plan.indexOf("reads:") < 0);

        conn = getConnection(url);
        stat = conn.createStatement();
        conn.setAutoCommit(false);
        stat.execute("insert into test select x from system_range(1, 1000)");
        rs = stat.executeQuery("select count(*) from test");
        rs.next();
        assertEquals(11000, rs.getInt(1));

        // not yet committed
        rs = stat2.executeQuery("explain analyze select count(*) from test");
        rs.next();
        plan = rs.getString(1);
        // transaction log is small, so no need to read the table
        assertTrue(plan, plan.indexOf("reads:") < 0);
        rs = stat2.executeQuery("select count(*) from test");
        rs.next();
        assertEquals(10000, rs.getInt(1));

        stat.execute("insert into test2 select x from system_range(1, 11000)");
        rs = stat2.executeQuery("explain analyze select count(*) from test");
        rs.next();
        plan = rs.getString(1);
        // transaction log is larger than the table, so read the table
        assertTrue(plan, plan.indexOf("reads:") >= 0);
        rs = stat2.executeQuery("select count(*) from test");
        rs.next();
        assertEquals(10000, rs.getInt(1));

        conn2.close();
        conn.close();
    }

    private void testMinMaxWithNull() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Connection conn2;
        Statement stat;
        Statement stat2;
        String url = "mvstore;MV_STORE=TRUE;MVCC=TRUE";
        url = getURL(url, true);
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(data int)");
        stat.execute("create index on test(data)");
        stat.execute("insert into test values(null), (2)");
        conn2 = getConnection(url);
        stat2 = conn2.createStatement();
        conn.setAutoCommit(false);
        conn2.setAutoCommit(false);
        stat.execute("insert into test values(1)");
        ResultSet rs;
        rs = stat.executeQuery("select min(data) from test");
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs = stat2.executeQuery("select min(data) from test");
        rs.next();
        // not yet committed
        assertEquals(2, rs.getInt(1));
        conn2.close();
        conn.close();
    }

    private void testTimeout() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Connection conn2;
        Statement stat;
        Statement stat2;
        String url = "mvstore;MV_STORE=TRUE;MVCC=TRUE";
        url = getURL(url, true);
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id identity, name varchar)");
        conn2 = getConnection(url);
        stat2 = conn2.createStatement();
        conn.setAutoCommit(false);
        conn2.setAutoCommit(false);
        stat.execute("insert into test values(1, 'Hello')");
        assertThrows(ErrorCode.LOCK_TIMEOUT_1, stat2).
                execute("insert into test values(1, 'Hello')");
        conn2.close();
        conn.close();
    }

    private void testExplainAnalyze() throws Exception {
        if (config.memory) {
            return;
        }
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;
        String url = "mvstore;MV_STORE=TRUE";
        url = getURL(url, true);
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id identity, name varchar) as " +
                "select x, space(1000) from system_range(1, 1000)");
        ResultSet rs;
        conn.close();
        conn = getConnection(url);
        stat = conn.createStatement();
        rs = stat.executeQuery("explain analyze select * from test");
        rs.next();
        String plan = rs.getString(1);
        // expect about 249 reads
        assertTrue(plan, plan.indexOf("reads: 2") >= 0);
        conn.close();
    }

    private void testTransactionLogUsuallyNotStored() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;
        String url = "mvstore;MV_STORE=TRUE";
        url = getURL(url, true);
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id identity, name varchar)");
        conn.setAutoCommit(false);
        PreparedStatement prep = conn.prepareStatement(
                "insert into test(name) values(space(10000))");
        for (int j = 0; j < 100; j++) {
            for (int i = 0; i < 100; i++) {
                prep.execute();
            }
            conn.commit();
        }
        stat.execute("shutdown immediately");
        JdbcUtils.closeSilently(conn);

        String file = getBaseDir() + "/mvstore" + Constants.SUFFIX_MV_FILE;

        MVStore store = MVStore.open(file);
        TransactionStore t = new TransactionStore(store);
        assertEquals(0, t.getOpenTransactions().size());
        store.close();
    }

    private void testShrinkDatabaseFile() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        String dbName = "mvstore" +
                ";MV_STORE=TRUE";
        Connection conn;
        Statement stat;
        long maxSize = 0;
        // by default, the database does not shrink for 45 seconds
        int retentionTime = 45000;
        for (int i = 0; i < 20; i++) {
            // the first 10 times, keep the default retention time
            // then switch to 0, at which point the database file
            // should stop to grow
            conn = getConnection(dbName);
            stat = conn.createStatement();
            if (i == 10) {
                stat.execute("set retention_time 0");
                retentionTime = 0;
            }
            ResultSet rs = stat.executeQuery(
                    "select value from information_schema.settings " +
                    "where name='RETENTION_TIME'");
            assertTrue(rs.next());
            assertEquals(retentionTime, rs.getInt(1));
            stat.execute("create table test(id int primary key, data varchar)");
            stat.execute("insert into test select x, space(100) from system_range(1, 1000)");
            // this table is kept
            if (i < 10) {
                stat.execute("create table test" + i + "(id int primary key, data varchar) " +
                        "as select x, space(10) from system_range(1, 100)");
            }
            // force writing the chunk
            stat.execute("checkpoint");
            // drop the table - but the chunk is still used
            stat.execute("drop table test");
            stat.execute("checkpoint");
            stat.execute("shutdown immediately");
            try {
                conn.close();
            } catch (Exception e) {
                // ignore
            }
            String fileName = getBaseDir() + "/mvstore"
                    + Constants.SUFFIX_MV_FILE;
            long size = FileUtils.size(fileName);
            if (i < 10) {
                maxSize = (int) (Math.max(size, maxSize) * 1.2);
            } else if (size > maxSize) {
                fail(i + " size: " + size + " max: " + maxSize);
            }
        }
        long sizeOld = FileUtils.size(getBaseDir() + "/mvstore"
                + Constants.SUFFIX_MV_FILE);
        conn = getConnection(dbName);
        stat = conn.createStatement();
        stat.execute("shutdown compact");
        conn.close();
        long sizeNew = FileUtils.size(getBaseDir() + "/mvstore"
                + Constants.SUFFIX_MV_FILE);
        assertTrue("new: " + sizeNew + " old: " + sizeOld, sizeNew < sizeOld);
    }

    private void testTwoPhaseCommit() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;
        String url = "mvstore;MV_STORE=TRUE";
        url = getURL(url, true);
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("set write_delay 0");
        conn.setAutoCommit(false);
        stat.execute("insert into test values(1, 'Hello')");
        stat.execute("prepare commit test_tx");
        stat.execute("shutdown immediately");
        JdbcUtils.closeSilently(conn);

        conn = getConnection(url);
        stat = conn.createStatement();
        ResultSet rs;
        rs = stat.executeQuery("select * from information_schema.in_doubt");
        assertTrue(rs.next());
        stat.execute("commit transaction test_tx");
        rs = stat.executeQuery("select * from test");
        assertTrue(rs.next());
        conn.close();
    }

    private void testRecover() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;
        String url = "mvstore;MV_STORE=TRUE";
        url = getURL(url, true);
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test values(1, 'Hello')");
        stat.execute("create table test2(name varchar)");
        stat.execute("insert into test2 values('Hello World')");
        conn.close();

        Recover.execute(getBaseDir(), "mvstore");
        DeleteDbFiles.execute(getBaseDir(), "mvstore", true);
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("runscript from '" + getBaseDir() + "/mvstore.h2.sql'");
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        rs = stat.executeQuery("select * from test2");
        assertTrue(rs.next());
        assertEquals("Hello World", rs.getString(1));
        conn.close();
    }

    private void testRollback() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;
        String url = "mvstore;MV_STORE=TRUE";
        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id identity)");
        conn.setAutoCommit(false);
        stat.execute("insert into test values(1)");
        stat.execute("delete from test");
        conn.rollback();
        conn.close();
    }

    private void testSeparateKey() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;

        String url = "mvstore;MV_STORE=TRUE";

        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table a(id int)");
        stat.execute("insert into a values(1)");
        stat.execute("insert into a values(1)");

        stat.execute("create table test(id int not null) as select 100");
        stat.execute("create primary key on test(id)");
        ResultSet rs = stat.executeQuery("select * from test where id = 100");
        assertTrue(rs.next());
        conn.close();

        conn = getConnection(url);
        stat = conn.createStatement();
        rs = stat.executeQuery("select * from test where id = 100");
        assertTrue(rs.next());
        conn.close();
    }

    private void testRollbackAfterCrash() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;

        String url = "mvstore;MV_STORE=TRUE";
        String url2 = "mvstore2;MV_STORE=TRUE";

        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("insert into test values(0)");
        stat.execute("set write_delay 0");
        conn.setAutoCommit(false);
        stat.execute("insert into test values(1)");
        stat.execute("shutdown immediately");
        JdbcUtils.closeSilently(conn);

        conn = getConnection(url);
        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select row_count_estimate " +
                "from information_schema.tables where table_name='TEST'");
        rs.next();
        assertEquals(1, rs.getLong(1));
        stat.execute("drop table test");

        stat.execute("create table test(id int primary key, data clob)");
        stat.execute("insert into test values(1, space(10000))");
        conn.setAutoCommit(false);
        stat.execute("delete from test");
        stat.execute("checkpoint");
        stat.execute("shutdown immediately");
        JdbcUtils.closeSilently(conn);

        conn = getConnection(url);
        stat = conn.createStatement();
        rs = stat.executeQuery("select * from test");
        assertTrue(rs.next());
        stat.execute("drop all objects delete files");
        conn.close();

        conn = getConnection(url);
        stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("create index idx_name on test(name, id)");
        stat.execute("insert into test select x, x || space(200 * x) from system_range(1, 10)");
        conn.setAutoCommit(false);
        stat.execute("delete from test where id > 5");
        stat.execute("backup to '" + getBaseDir() + "/backup.zip'");
        conn.rollback();
        Restore.execute(getBaseDir() + "/backup.zip", getBaseDir(), "mvstore2");
        Connection conn2;
        conn2 = getConnection(url2);
        conn.close();
        conn2.close();

    }

    private void testReferentialIntegrity() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;

        conn = getConnection("mvstore;MV_STORE=TRUE");

        stat = conn.createStatement();
        stat.execute("create table test(id int, parent int " +
                "references test(id) on delete cascade)");
        stat.execute("insert into test values(0, 0)");
        stat.execute("delete from test");
        stat.execute("drop table test");

        stat.execute("create table parent(id int, name varchar)");
        stat.execute("create table child(id int, parentid int, " +
        "foreign key(parentid) references parent(id))");
        stat.execute("insert into parent values(1, 'mary'), (2, 'john')");
        stat.execute("insert into child values(10, 1), (11, 1), (20, 2), (21, 2)");
        stat.execute("update parent set name = 'marc' where id = 1");
        stat.execute("merge into parent key(id) values(1, 'marcy')");
        stat.execute("drop table parent, child");

        stat.execute("create table test(id identity, parent bigint, " +
                "foreign key(parent) references(id))");
        stat.execute("insert into test values(0, 0), (1, NULL), " +
                "(2, 1), (3, 3), (4, 3)");
        stat.execute("drop table test");

        stat.execute("create table parent(id int)");
        stat.execute("create table child(pid int)");
        stat.execute("insert into parent values(1)");
        stat.execute("insert into child values(2)");
        try {
            stat.execute("alter table child add constraint cp " +
                    "foreign key(pid) references parent(id)");
            fail();
        } catch (SQLException e) {
            assertEquals(ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1, e.getErrorCode());
        }
        stat.execute("update child set pid=1");
        stat.execute("drop table child, parent");

        stat.execute("create table parent(id int)");
        stat.execute("create table child(pid int)");
        stat.execute("insert into parent values(1)");
        stat.execute("insert into child values(2)");
        try {
            stat.execute("alter table child add constraint cp " +
                        "foreign key(pid) references parent(id)");
            fail();
        } catch (SQLException e) {
            assertEquals(ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1, e.getErrorCode());
        }
        stat.execute("drop table child, parent");

        stat.execute("create table test(id identity, parent bigint, " +
                "foreign key(parent) references(id))");
        stat.execute("insert into test values(0, 0), (1, NULL), " +
                "(2, 1), (3, 3), (4, 3)");
        stat.execute("drop table test");

        stat.execute("create table parent(id int, x int)");
        stat.execute("insert into parent values(1, 2)");
        stat.execute("create table child(id int references parent(id)) as select 1");

        conn.close();
    }

    private void testWriteDelay() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;
        ResultSet rs;
        conn = getConnection("mvstore;MV_STORE=TRUE");
        stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("set write_delay 0");
        stat.execute("insert into test values(1)");
        stat.execute("shutdown immediately");
        try {
            conn.close();
        } catch (Exception e) {
            // ignore
        }
        conn = getConnection("mvstore;MV_STORE=TRUE");
        stat = conn.createStatement();
        rs = stat.executeQuery("select * from test");
        assertTrue(rs.next());
        conn.close();
    }

    private void testAutoCommit() throws SQLException {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;
        ResultSet rs;
        conn = getConnection("mvstore;MV_STORE=TRUE");
        for (int i = 0; i < 2; i++) {
            stat = conn.createStatement();
            stat.execute("create table test(id int primary key, name varchar)");
            stat.execute("create index on test(name)");
            conn.setAutoCommit(false);
            stat.execute("insert into test values(1, 'Hello')");
            stat.execute("insert into test values(2, 'World')");
            rs = stat.executeQuery("select count(*) from test");
            rs.next();
            assertEquals(2, rs.getInt(1));
            conn.rollback();
            rs = stat.executeQuery("select count(*) from test");
            rs.next();
            assertEquals(0, rs.getInt(1));

            stat.execute("insert into test values(1, 'Hello')");
            Savepoint sp = conn.setSavepoint();
            stat.execute("insert into test values(2, 'World')");
            conn.rollback(sp);
            rs = stat.executeQuery("select count(*) from test");
            rs.next();
            assertEquals(1, rs.getInt(1));
            stat.execute("drop table test");
        }

        conn.close();
    }

    private void testReopen() throws SQLException {
        FileUtils.deleteRecursive(getBaseDir(), true);
        Connection conn;
        Statement stat;
        conn = getConnection("mvstore;MV_STORE=TRUE");
        stat = conn.createStatement();
        stat.execute("create table test(id int, name varchar)");
        conn.close();
        conn = getConnection("mvstore;MV_STORE=TRUE");
        stat = conn.createStatement();
        stat.execute("drop table test");
        conn.close();
    }

    private void testBlob() throws SQLException, IOException {
        FileUtils.deleteRecursive(getBaseDir(), true);
        String dbName = "mvstore;MV_STORE=TRUE";
        Connection conn;
        Statement stat;
        conn = getConnection(dbName);
        stat = conn.createStatement();
        stat.execute("create table test(id int, name blob)");
        PreparedStatement prep = conn.prepareStatement("insert into test values(1, ?)");
        prep.setBinaryStream(1,  new ByteArrayInputStream(new byte[129]));
        prep.execute();
        conn.close();
        conn = getConnection(dbName);
        stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from test");
        while (rs.next()) {
            InputStream in = rs.getBinaryStream(2);
            int len = 0;
            while (in.read() >= 0) {
                len++;
            }
            assertEquals(129, len);
        }
        conn.close();
        FileUtils.deleteRecursive(getBaseDir(), true);
    }

    private void testEncryption() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        String dbName = "mvstore;MV_STORE=TRUE";
        Connection conn;
        Statement stat;
        String url = getURL(dbName + ";CIPHER=AES", true);
        String user = "sa";
        String password = "123 123";
        conn = DriverManager.getConnection(url, user, password);
        stat = conn.createStatement();
        stat.execute("create table test(id int primary key)");
        conn.close();
        conn = DriverManager.getConnection(url, user, password);
        stat = conn.createStatement();
        stat.execute("select * from test");
        stat.execute("drop table test");
        conn.close();
        FileUtils.deleteRecursive(getBaseDir(), true);
    }

    private void testExclusiveLock() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        String dbName = "mvstore;MV_STORE=TRUE";
        Connection conn, conn2;
        Statement stat, stat2;
        conn = getConnection(dbName);
        stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("insert into test values(1)");
        conn.setAutoCommit(false);
        // stat.execute("update test set id = 2");
        stat.executeQuery("select * from test for update");
        conn2 = getConnection(dbName);
        stat2 = conn2.createStatement();
        ResultSet rs2 = stat2.executeQuery("select * from information_schema.locks");
        assertTrue(rs2.next());
        assertEquals("TEST", rs2.getString("table_name"));
        assertEquals("WRITE", rs2.getString("lock_type"));
        conn2.close();
        conn.close();
        FileUtils.deleteRecursive(getBaseDir(), true);
    }

    private void testReadOnly() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        String dbName = "mvstore;MV_STORE=TRUE";
        Connection conn;
        Statement stat;
        conn = getConnection(dbName);
        stat = conn.createStatement();
        stat.execute("create table test(id int)");
        conn.close();
        FileUtils.setReadOnly(getBaseDir() + "/mvstore" + Constants.SUFFIX_MV_FILE);
        conn = getConnection(dbName);
        Database db = (Database) ((JdbcConnection) conn).getSession().getDataHandler();
        assertTrue(db.getMvStore().getStore().getFileStore().isReadOnly());
        conn.close();
        FileUtils.deleteRecursive(getBaseDir(), true);
    }

    private void testReuseDiskSpace() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        String dbName = "mvstore;MV_STORE=TRUE";
        Connection conn;
        Statement stat;
        long maxSize = 0;
        for (int i = 0; i < 20; i++) {
            conn = getConnection(dbName);
            Database db = (Database) ((JdbcConnection) conn).getSession().getDataHandler();
            db.getMvStore().getStore().setRetentionTime(0);
            stat = conn.createStatement();
            stat.execute("create table test(id int primary key, data varchar)");
            stat.execute("insert into test select x, space(1000) from system_range(1, 1000)");
            stat.execute("drop table test");
            conn.close();
            long size = FileUtils.size(getBaseDir() + "/mvstore"
                    + Constants.SUFFIX_MV_FILE);
            if (i < 10) {
                maxSize = (int) (Math.max(size, maxSize) * 1.1);
            } else if (size > maxSize) {
                fail(i + " size: " + size + " max: " + maxSize);
            }
        }
    }

    private void testDataTypes() throws Exception {
        FileUtils.deleteRecursive(getBaseDir(), true);
        String dbName = "mvstore;MV_STORE=TRUE";
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
        String dbName = "mvstore;MV_STORE=TRUE";
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
        String dbName = "mvstore;MV_STORE=TRUE";
        Connection conn = getConnection(dbName);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("insert into test values(1, 'Hello'), (2, 'World')");
        ResultSet rs = stat.executeQuery("select *, _rowid_ from test");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        assertEquals(1, rs.getInt(3));

        stat.execute("update test set name = 'Hello' where id = 1");

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
            assertEquals(e.toString(), ErrorCode.DUPLICATE_KEY_1, e.getErrorCode());
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
        stat.execute("delete from test");
        stat.execute("insert into test(id, name) values(-1, 'Hello')");
        rs = stat.executeQuery("select count(*) from test where id = -1");
        rs.next();
        assertEquals(1, rs.getInt(1));
        rs = stat.executeQuery("select count(*) from test where name = 'Hello'");
        rs.next();
        assertEquals(1, rs.getInt(1));
        conn.close();
    }

}
