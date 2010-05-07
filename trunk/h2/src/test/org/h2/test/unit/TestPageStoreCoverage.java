/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.engine.Constants;
import org.h2.store.fs.FileObject;
import org.h2.store.fs.FileSystem;
import org.h2.test.TestBase;
import org.h2.tools.Restore;
import org.h2.util.IOUtils;

/**
 * Test the page store.
 */
public class TestPageStoreCoverage extends TestBase {

    private static final String URL = "pageStore;PAGE_SIZE=64;CACHE_SIZE=16;MAX_LOG_SIZE=1";

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        // TODO mvcc, 2-phase commit
        if (config.memory) {
            return;
        }
        deleteDb("pageStore");
        testMoveRoot();
        testBasic();
        testReadOnly();
        testIncompleteCreate();
        testBackupRestore();
        testTrim();
        testLongTransaction();
        testRecoverTemp();
        deleteDb("pageStore");
    }

    private void testMoveRoot() throws SQLException {
        Connection conn;
        conn = getConnection(URL);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key) as select x from system_range(1, 100)");
        for (int i = 0; i < 10; i++) {
            stat.execute("create table test" + i + "(id int primary key) as select x from system_range(1, 2)");
        }
        stat.execute("drop table test");
        conn.close();

        conn = getConnection(URL);
        stat = conn.createStatement();
        for (int i = 0; i < 10; i++) {
            ResultSet rs = stat.executeQuery("select * from test" + i);
            while (rs.next()) {
                // ignore
            }
        }
        stat.execute("drop all objects delete files");
        conn.close();
    }

    private void testRecoverTemp() throws SQLException {
        Connection conn;
        conn = getConnection(URL);
        Statement stat = conn.createStatement();
        stat.execute("create cached temporary table test(id identity, name varchar)");
        stat.execute("create index idx_test_name on test(name)");
        stat.execute("create index idx_test_name2 on test(name, id)");
        stat.execute("create table test2(id identity, name varchar)");
        stat.execute("create index idx_test2_name on test2(name desc)");
        stat.execute("create index idx_test2_name2 on test2(name, id)");
        stat.execute("insert into test2 select null, space(10) from system_range(1, 10)");
        stat.execute("create table test3(id identity, name varchar)");
        stat.execute("checkpoint");
        conn.setAutoCommit(false);
        stat.execute("create table test4(id identity, name varchar)");
        stat.execute("create index idx_test4_name2 on test(name, id)");
        stat.execute("insert into test select null, space(10) from system_range(1, 10)");
        stat.execute("insert into test3 select null, space(10) from system_range(1, 10)");
        stat.execute("insert into test4 select null, space(10) from system_range(1, 10)");
        stat.execute("truncate table test2");
        stat.execute("drop index idx_test_name");
        stat.execute("drop index idx_test2_name");
        stat.execute("drop table test2");
        stat.execute("insert into test select null, space(10) from system_range(1, 10)");
        stat.execute("shutdown immediately");
        try {
            conn.close();
            fail();
        } catch (SQLException e) {
            // ignore
        }
        conn = getConnection(URL);
        stat = conn.createStatement();
        stat.execute("drop all objects");
        // re-allocate index root pages
        for (int i = 0; i < 10; i++) {
            stat.execute("create table test" + i + "(id identity, name varchar)");
        }
        stat.execute("checkpoint");
        for (int i = 0; i < 10; i++) {
            stat.execute("drop table test" + i);
        }
        for (int i = 0; i < 10; i++) {
            stat.execute("create table test" + i + "(id identity, name varchar)");
        }
        stat.execute("shutdown immediately");
        try {
            conn.close();
            fail();
        } catch (SQLException e) {
            // ignore
        }
        conn = getConnection(URL);
        conn.createStatement().execute("drop all objects");
        conn.close();
    }

    private void testLongTransaction() throws SQLException {
        Connection conn;
        conn = getConnection(URL);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id identity, name varchar)");
        conn.setAutoCommit(false);
        stat.execute("insert into test select null, space(10) from system_range(1, 10)");
        Connection conn2;
        conn2 = getConnection(URL);
        Statement stat2 = conn2.createStatement();
        stat2.execute("checkpoint");
        // large transaction
        stat2.execute("create table test2(id identity, name varchar)");
        stat2.execute("create index idx_test2_name on test2(name)");
        stat2.execute("insert into test2 select null, x || space(10000) from system_range(1, 100)");
        stat2.execute("drop table test2");
        conn2.close();
        stat.execute("drop table test");
        conn.close();
    }

    private void testTrim() throws SQLException {
        Connection conn;
        conn = getConnection(URL);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("create index idx_name on test(name, id)");
        stat.execute("insert into test select x, x || space(10) from system_range(1, 20)");
        stat.execute("create table test2(id int primary key, name varchar)");
        stat.execute("create index idx_test2_name on test2(name, id)");
        stat.execute("insert into test2 select x, x || space(10) from system_range(1, 20)");
        stat.execute("create table test3(id int primary key, name varchar)");
        stat.execute("create index idx_test3_name on test3(name, id)");
        stat.execute("insert into test3 select x, x || space(3) from system_range(1, 3)");
        stat.execute("delete from test");
        stat.execute("checkpoint");
        stat.execute("checkpoint sync");
        stat.execute("shutdown compact");
        conn.close();
        conn = getConnection(URL);
        conn.createStatement().execute("drop all objects");
        conn.close();
    }

    private void testBasic() throws Exception {
        Connection conn;
        conn = getConnection(URL);
        conn.close();
        conn = getConnection(URL);
        conn.close();

    }

    private void testReadOnly() throws Exception {
        Connection conn;
        conn = getConnection(URL);
        conn.createStatement().execute("shutdown compact");
        conn.close();
        conn = getConnection(URL + ";access_mode_data=r");
        conn.close();
    }

    private void testBackupRestore() throws Exception {
        Connection conn;
        conn = getConnection(URL);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("create index idx_name on test(name, id)");
        stat.execute("insert into test select x, x || space(200 * x) from system_range(1, 10)");
        conn.setAutoCommit(false);
        stat.execute("delete from test where id > 5");
        stat.execute("backup to '" + getBaseDir() + "/backup.zip'");
        conn.rollback();
        Restore.execute(getBaseDir() + "/backup.zip", getBaseDir(), "pageStore2", true);
        Connection conn2;
        conn2 = getConnection("pageStore2");
        Statement stat2 = conn2.createStatement();
        assertEqualDatabases(stat, stat2);
        conn.createStatement().execute("drop table test");
        conn2.close();
        conn.close();
        IOUtils.delete(getBaseDir() + "/backup.zip");
        deleteDb("pageStore2");
    }

    private void testIncompleteCreate() throws Exception {
        deleteDb("pageStore");
        Connection conn;
        String fileName = getBaseDir() + "/pageStore" + Constants.SUFFIX_PAGE_FILE;
        conn = getConnection("pageStore");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists INFORMATION_SCHEMA.LOB_DATA");
        stat.execute("drop table if exists INFORMATION_SCHEMA.LOB_MAP");
        conn.close();
        FileObject f = FileSystem.getInstance(fileName).openFileObject(fileName, "rw");
        // create a new database
        conn = getConnection("pageStore");
        conn.close();
        f = FileSystem.getInstance(fileName).openFileObject(fileName, "rw");
        f.setFileLength(16);
        // create a new database
        conn = getConnection("pageStore");
        deleteDb("pageStore");
    }

}
