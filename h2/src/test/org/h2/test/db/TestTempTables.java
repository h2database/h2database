/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.store.fs.FileUtils;
import org.h2.test.TestBase;

/**
 * Temporary table tests.
 */
public class TestTempTables extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        deleteDb("tempTables");
        testTempFileResultSet();
        testTempTableResultSet();
        testTransactionalTemp();
        testDeleteGlobalTempTableWhenClosing();
        Connection c1 = getConnection("tempTables");
        testAlter(c1);
        Connection c2 = getConnection("tempTables");
        testConstraints(c1, c2);
        testTables(c1, c2);
        testIndexes(c1, c2);
        c1.close();
        c2.close();
        deleteDb("tempTables");
    }

    private void testTempFileResultSet() throws SQLException {
        deleteDb("tempTables");
        Connection conn = getConnection("tempTables;MAX_MEMORY_ROWS=10");
        ResultSet rs1, rs2;
        Statement stat1 = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        Statement stat2 = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        rs1 = stat1.executeQuery("select * from system_range(1, 20)");
        rs2 = stat2.executeQuery("select * from system_range(1, 20)");
        for (int i = 0; i < 20; i++) {
            rs1.next();
            rs2.next();
            rs1.getInt(1);
            rs2.getInt(1);
        }
        rs2.close();
        // verify the temp table is not deleted yet
        rs1.beforeFirst();
        for (int i = 0; i < 20; i++) {
            rs1.next();
            rs1.getInt(1);
        }
        rs1.close();

        rs1 = stat1.executeQuery(
                "select * from system_range(1, 20) order by x desc");
        rs2 = stat2.executeQuery(
                "select * from system_range(1, 20) order by x desc");
        for (int i = 0; i < 20; i++) {
            rs1.next();
            rs2.next();
            rs1.getInt(1);
            rs2.getInt(1);
        }
        rs1.close();
        // verify the temp table is not deleted yet
        rs2.beforeFirst();
        for (int i = 0; i < 20; i++) {
            rs2.next();
            rs2.getInt(1);
        }
        rs2.close();

        conn.close();
    }

    private void testTempTableResultSet() throws SQLException {
        deleteDb("tempTables");
        Connection conn = getConnection(
                "tempTables;MAX_MEMORY_ROWS_DISTINCT=10");
        Statement stat1 = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        Statement stat2 = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs1, rs2;
        rs1 = stat1.executeQuery("select distinct * from system_range(1, 20)");
        // this will re-use the same temp table
        rs2 = stat2.executeQuery("select distinct * from system_range(1, 20)");
        for (int i = 0; i < 20; i++) {
            rs1.next();
            rs2.next();
            rs1.getInt(1);
            rs2.getInt(1);
        }
        rs2.close();
        // verify the temp table is not deleted yet
        rs1.beforeFirst();
        for (int i = 0; i < 20; i++) {
            rs1.next();
            rs1.getInt(1);
        }
        rs1.close();

        rs1 = stat1.executeQuery("select distinct * from system_range(1, 20)");
        rs2 = stat2.executeQuery("select distinct * from system_range(1, 20)");
        for (int i = 0; i < 20; i++) {
            rs1.next();
            rs2.next();
            rs1.getInt(1);
            rs2.getInt(1);
        }
        rs1.close();
        // verify the temp table is not deleted yet
        rs2.beforeFirst();
        for (int i = 0; i < 20; i++) {
            rs2.next();
            rs2.getInt(1);
        }
        rs2.close();

        conn.close();
    }

    private void testTransactionalTemp() throws SQLException {
        deleteDb("tempTables");
        Connection conn = getConnection("tempTables");
        conn.setAutoCommit(false);
        Statement stat = conn.createStatement();
        ResultSet rs;
        stat.execute("create table test(id int primary key)");
        stat.execute("insert into test values(1)");
        stat.execute("commit");
        stat.execute("insert into test values(2)");
        stat.execute("create local temporary table temp(" +
                "id int primary key, name varchar, constraint x index(name)) transactional");
        stat.execute("insert into temp values(3, 'test')");
        stat.execute("rollback");
        rs = stat.executeQuery("select * from test");
        assertTrue(rs.next());
        assertFalse(rs.next());
        stat.execute("drop table test");
        stat.execute("drop table temp");
        conn.close();
    }

    private void testDeleteGlobalTempTableWhenClosing() throws SQLException {
        if (config.memory) {
            return;
        }
        if (config.mvStore) {
            return;
        }
        deleteDb("tempTables");
        Connection conn = getConnection("tempTables");
        Statement stat = conn.createStatement();
        stat.execute("create global temporary table test(id int, data varchar)");
        stat.execute("insert into test " +
                    "select x, space(1000) from system_range(1, 1000)");
        stat.execute("shutdown compact");
        try {
            conn.close();
        } catch (SQLException e) {
            // expected
        }
        String dbName = getBaseDir() + "/tempTables" + Constants.SUFFIX_PAGE_FILE;
        long before = FileUtils.size(dbName);
        assertTrue(before > 0);
        conn = getConnection("tempTables");
        conn.close();
        long after = FileUtils.size(dbName);
        assertEquals(after, before);
    }

    private void testAlter(Connection conn) throws SQLException {
        Statement stat;
        stat = conn.createStatement();
        stat.execute("create temporary table test(id varchar)");
        stat.execute("create index idx1 on test(id)");
        stat.execute("drop index idx1");
        stat.execute("create index idx1 on test(id)");
        stat.execute("insert into test select x from system_range(1, 10)");
        assertThrows(ErrorCode.FEATURE_NOT_SUPPORTED_1, stat).
                execute("alter table test add column x int");
        stat.execute("drop table test");
    }

    private static void testConstraints(Connection conn1, Connection conn2)
            throws SQLException {
        Statement s1 = conn1.createStatement(), s2 = conn2.createStatement();
        s1.execute("create local temporary table test(id int unique)");
        s2.execute("create local temporary table test(id int unique)");
        s1.execute("alter table test add constraint a unique(id)");
        s2.execute("alter table test add constraint a unique(id)");
        s1.execute("drop table test");
        s2.execute("drop table test");
    }

    private static void testIndexes(Connection conn1, Connection conn2)
            throws SQLException {
        conn1.createStatement().executeUpdate(
                "create local temporary table test(id int)");
        conn1.createStatement().executeUpdate(
                "create index idx_id on test(id)");
        conn2.createStatement().executeUpdate(
                "create local temporary table test(id int)");
        conn2.createStatement().executeUpdate(
                "create index idx_id on test(id)");
        conn2.createStatement().executeUpdate("drop index idx_id");
        conn2.createStatement().executeUpdate("drop table test");
        conn2.createStatement().executeUpdate("create table test(id int)");
        conn2.createStatement().executeUpdate("create index idx_id on test(id)");
        conn1.createStatement().executeUpdate("drop table test");
        conn1.createStatement().executeUpdate("drop table test");
    }

    private void testTables(Connection c1, Connection c2) throws SQLException {
        Statement s1 = c1.createStatement();
        Statement s2 = c2.createStatement();
        s1.execute("CREATE LOCAL TEMPORARY TABLE LT(A INT)");
        s1.execute("CREATE GLOBAL TEMPORARY TABLE GT1(ID INT)");
        s2.execute("CREATE GLOBAL TEMPORARY TABLE GT2(ID INT)");
        s2.execute("CREATE LOCAL TEMPORARY TABLE LT(B INT)");
        s2.execute("SELECT B FROM LT");
        s1.execute("SELECT A FROM LT");
        s1.execute("SELECT * FROM GT1");
        s2.execute("SELECT * FROM GT1");
        s1.execute("SELECT * FROM GT2");
        s2.execute("SELECT * FROM GT2");
        s2.execute("DROP TABLE GT1");
        s2.execute("DROP TABLE GT2");
        s2.execute("DROP TABLE LT");
        s1.execute("DROP TABLE LT");

        // temp tables: 'on commit' syntax is currently not documented, because
        // not tested well
        // and hopefully nobody is using it, as it looks like functional sugar
        // (this features are here for compatibility only)
        ResultSet rs;
        c1.setAutoCommit(false);
        s1.execute("create local temporary table test_temp(id int) " +
                "on commit delete rows");
        s1.execute("insert into test_temp values(1)");
        rs = s1.executeQuery("select * from test_temp");
        assertResultRowCount(1, rs);
        c1.commit();
        rs = s1.executeQuery("select * from test_temp");
        assertResultRowCount(0, rs);
        s1.execute("drop table test_temp");

        s1.execute("create local temporary table test_temp(id int) on commit drop");
        s1.execute("insert into test_temp values(1)");
        rs = s1.executeQuery("select * from test_temp");
        assertResultRowCount(1, rs);
        c1.commit();
        // test_temp should have been dropped automatically
        assertThrows(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, s1).
                executeQuery("select * from test_temp");
    }

}
