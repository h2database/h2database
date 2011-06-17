/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
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
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {
        deleteDb("alter");
        conn = getConnection("alter");
        stat = conn.createStatement();
        testAlterTableAlterColumn();
        conn.close();
        deleteDb("alter");
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
