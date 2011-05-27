/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.test.TestBase;


/**
 * Tests Statement.cancel
 */
public class TestCancel extends TestBase {

    class CancelThread extends Thread {
        private Statement cancel;
        private int wait;

        CancelThread(Statement cancel, int wait) {
            this.cancel = cancel;
            this.wait = wait;
        }

        public void run() {
            try {
                Thread.sleep(wait);
                cancel.cancel();
                Thread.yield();
            } catch (SQLException e) {
                // ignore errors on closed statements
            } catch (Exception e) {
                TestBase.logError("sleep", e);
            }
        }
    }

    public void test() throws Exception {
        testReset();
        testMaxQueryTimeout();
        testQueryTimeout();
        testJdbcQueryTimeout();
        testCancelStatement();
    }
    
    private void testReset() throws Exception {
        deleteDb("cancel");
        Connection conn = getConnection("cancel");
        Statement stat = conn.createStatement();
        stat.execute("set query_timeout 1");
        try {
            stat.execute("select count(*) from system_range(1, 1000000), system_range(1, 1000000)");
            error();
        } catch (SQLException e) {
            checkNotGeneralException(e);
        }
        stat.execute("set query_timeout 0");
        stat.execute("select count(*) from system_range(1, 1000), system_range(1, 1000)");
        conn.close();
    }

    private void testJdbcQueryTimeout() throws Exception {
        deleteDb("cancel");
        Connection conn = getConnection("cancel");
        Statement stat = conn.createStatement();
        check(0, stat.getQueryTimeout());
        stat.setQueryTimeout(1);
        check(1, stat.getQueryTimeout());
        Statement s2 = conn.createStatement();
        check(1, s2.getQueryTimeout());
        ResultSet rs = s2.executeQuery("SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME = 'QUERY_TIMEOUT'");
        rs.next();
        check(1000, rs.getInt(1));
        try {
            stat.executeQuery("SELECT MAX(RAND()) FROM SYSTEM_RANGE(1, 100000000)");
            error();
        } catch (SQLException e) {
            check(ErrorCode.STATEMENT_WAS_CANCELLED, e.getErrorCode());
        }
        stat.setQueryTimeout(0);
        stat.execute("SET QUERY_TIMEOUT 1100");
        check(2, stat.getQueryTimeout());
        conn.close();
    }

    private void testQueryTimeout() throws Exception {
        deleteDb("cancel");
        Connection conn = getConnection("cancel");
        Statement stat = conn.createStatement();
        stat.execute("SET QUERY_TIMEOUT 10");
        try {
            stat.executeQuery("SELECT MAX(RAND()) FROM SYSTEM_RANGE(1, 100000000)");
            error();
        } catch (SQLException e) {
            check(ErrorCode.STATEMENT_WAS_CANCELLED, e.getErrorCode());
        }
        conn.close();
    }

    private void testMaxQueryTimeout() throws Exception {
        deleteDb("cancel");
        int oldMax = SysProperties.getMaxQueryTimeout();
        try {
            System.setProperty(SysProperties.H2_MAX_QUERY_TIMEOUT, "" + 10);
            Connection conn = getConnection("cancel");
            Statement stat = conn.createStatement();
            try {
                stat.executeQuery("SELECT MAX(RAND()) FROM SYSTEM_RANGE(1, 100000000)");
                error();
            } catch (SQLException e) {
                check(ErrorCode.STATEMENT_WAS_CANCELLED, e.getErrorCode());
            }
            conn.close();
        } finally {
            System.setProperty("h2.maxQueryTimeout", "" + oldMax);
        }
    }

    private void testCancelStatement() throws Exception {
        deleteDb("cancel");
        Connection conn = getConnection("cancel");
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE  MEMORY TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        trace("insert");
        int len = getSize(1, 1000);
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            // prep.setString(2, "Test Value "+i);
            prep.setString(2, "hi");
            prep.execute();
        }
        trace("inserted");
        // TODO test insert.. select
        for (int i = 1;;) {
            Statement query = conn.createStatement();
            CancelThread cancel = new CancelThread(query, i);
            cancel.start();
            Thread.yield();
            int j = 0;
            try {
                ResultSet rs = query.executeQuery("SELECT * FROM TEST");
                while (rs.next()) {
                    j++;
                }
                trace("record count: " + j);
            } catch (SQLException e) {
                checkNotGeneralException(e);
                // ignore cancelled statements
                trace("record count: " + j);
            }
            if (j == 0) {
                i += 10;
            } else if (j == len) {
                break;
            }
        }
        conn.close();
    }

}
