/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.constant.ErrorCode;
import org.h2.test.TestBase;

/**
 * Test ALTER statements.
 */
public class TestAlter extends TestBase {

    private Connection conn;
    private Statement stat;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        deleteDb("alter");
        conn = getConnection("alter");
        stat = conn.createStatement();
        testAlterTableAlterColumn();
        testAlterTableDropIdentityColumn();
        conn.close();
        deleteDb("alter");
    }

    private void testAlterTableDropIdentityColumn() throws SQLException {
        stat.execute("create table test(id int auto_increment, name varchar)");
        stat.execute("alter table test drop column id");
        ResultSet rs = stat.executeQuery("select * from INFORMATION_SCHEMA.SEQUENCES");
        assertFalse(rs.next());
        stat.execute("drop table test");

        stat.execute("create table test(id int auto_increment, name varchar)");
        stat.execute("alter table test drop column name");
        rs = stat.executeQuery("select * from INFORMATION_SCHEMA.SEQUENCES");
        assertTrue(rs.next());
        stat.execute("drop table test");
    }

    private void testAlterTableAlterColumn() throws SQLException {
        stat.execute("create table t(x varchar) as select 'x'");
        try {
            stat.execute("alter table t alter column x int");
        } catch (SQLException e) {
            assertEquals(ErrorCode.DATA_CONVERSION_ERROR_1, e.getErrorCode());
        }
        stat.execute("drop table t");
        stat.execute("create table t(id identity, x varchar) as select null, 'x'");
        try {
            stat.execute("alter table t alter column x int");
        } catch (SQLException e) {
            assertEquals(ErrorCode.DATA_CONVERSION_ERROR_1, e.getErrorCode());
        }
        stat.execute("drop table t");
    }

}
