/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html). 
 * Initial Developer: H2 Group 
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;

/**
 * @author Thomas
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
