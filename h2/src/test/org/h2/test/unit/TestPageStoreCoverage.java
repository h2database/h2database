/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.tools.Restore;

/**
 * Test the page store.
 */
public class TestPageStoreCoverage extends TestDb {

    private static final String URL = "pageStoreCoverage;" +
            "PAGE_SIZE=64;CACHE_SIZE=16;MAX_LOG_SIZE=1";

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public boolean isEnabled() {
        if (config.memory) {
            return false;
        }
        return true;
    }

    @Override
    public void test() throws Exception {
        // TODO mvcc, 2-phase commit
        deleteDb("pageStoreCoverage");
        testMoveRoot();
        testBasic();
        testReadOnly();
        testBackupRestore();
        testTrim();
        testLongTransaction();
        testRecoverTemp();
        deleteDb("pageStoreCoverage");
    }

    private void testMoveRoot() throws SQLException {
        Connection conn;

        conn = getConnection(URL);
        Statement stat = conn.createStatement();
        stat.execute("create memory table test(id int primary key) " +
                "as select x from system_range(1, 20)");
        for (int i = 0; i < 10; i++) {
            stat.execute("create memory table test" + i +
                    "(id int primary key) as select x from system_range(1, 2)");
        }
        stat.execute("drop table test");
        conn.close();

        conn = getConnection(URL);
        stat = conn.createStatement();
        stat.execute("drop all objects delete files");
        conn.close();

        conn = getConnection(URL);
        stat = conn.createStatement();
        stat.execute("create table test(id int primary key) " +
                "as select x from system_range(1, 100)");
        for (int i = 0; i < 10; i++) {
            stat.execute("create table test" + i + "(id int primary key) " +
                    "as select x from system_range(1, 2)");
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
        try (Connection conn = getConnection(URL)) {
            Statement stat = conn.createStatement();
            stat.execute("create cached temporary table test(id identity, name varchar)");
            stat.execute("create index idx_test_name on test(name)");
            stat.execute("create index idx_test_name2 on test(name, id)");
            stat.execute("create table test2(id identity, name varchar)");
            stat.execute("create index idx_test2_name on test2(name desc)");
            stat.execute("create index idx_test2_name2 on test2(name, id)");
            stat.execute("insert into test2(name) " +
                    "select space(10) from system_range(1, 10)");
            stat.execute("create table test3(id identity, name varchar)");
            stat.execute("checkpoint");
            conn.setAutoCommit(false);
            stat.execute("create table test4(id identity, name varchar)");
            stat.execute("create index idx_test4_name2 on test(name, id)");
            stat.execute("insert into test(name) " +
                    "select space(10) from system_range(1, 10)");
            stat.execute("insert into test3(name) " +
                    "select space(10) from system_range(1, 10)");
            stat.execute("insert into test4(name) " +
                    "select space(10) from system_range(1, 10)");
            stat.execute("truncate table test2");
            stat.execute("drop index idx_test_name");
            stat.execute("drop index idx_test2_name");
            stat.execute("drop table test2");
            stat.execute("insert into test(name) " +
                    "select space(10) from system_range(1, 10)");
            stat.execute("shutdown immediately");
        }
        try (Connection conn = getConnection(URL)) {
            Statement stat = conn.createStatement();
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
        }
        try (Connection conn = getConnection(URL)) {
            conn.createStatement().execute("drop all objects");
        }
    }

    private void testLongTransaction() throws SQLException {
        Connection conn;
        conn = getConnection(URL);
        Statement stat = conn.createStatement();
        stat.execute("create table test(id identity, name varchar)");
        conn.setAutoCommit(false);
        stat.execute("insert into test(name) " +
                "select space(10) from system_range(1, 10)");
        Connection conn2;
        conn2 = getConnection(URL);
        Statement stat2 = conn2.createStatement();
        stat2.execute("checkpoint");
        // large transaction
        stat2.execute("create table test2(id identity, name varchar)");
        stat2.execute("create index idx_test2_name on test2(name)");
        stat2.execute("insert into test2(name) " +
                "select x || space(10000) from system_range(1, 100)");
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
        stat.execute("insert into test " +
                "select x, x || space(10) from system_range(1, 20)");
        stat.execute("create table test2(id int primary key, name varchar)");
        stat.execute("create index idx_test2_name on test2(name, id)");
        stat.execute("insert into test2 " +
                "select x, x || space(10) from system_range(1, 20)");
        stat.execute("create table test3(id int primary key, name varchar)");
        stat.execute("create index idx_test3_name on test3(name, id)");
        stat.execute("insert into test3 " +
                "select x, x || space(3) from system_range(1, 3)");
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
        stat.execute(
                "create table test(id int primary key, name varchar)");
        stat.execute(
                "create index idx_name on test(name, id)");
        stat.execute(
                "insert into test select x, x || space(200 * x) from system_range(1, 10)");
        conn.setAutoCommit(false);
        stat.execute("delete from test where id > 5");
        stat.execute("backup to '" + getBaseDir() + "/backup.zip'");
        conn.rollback();
        Restore.execute(getBaseDir() + "/backup.zip", getBaseDir(),
                "pageStore2");
        Connection conn2;
        conn2 = getConnection("pageStore2");
        Statement stat2 = conn2.createStatement();
        assertEqualDatabases(stat, stat2);
        conn.createStatement().execute("drop table test");
        conn2.close();
        conn.close();
        FileUtils.delete(getBaseDir() + "/backup.zip");
        deleteDb("pageStore2");
    }

}
