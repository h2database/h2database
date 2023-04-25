/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.util.Task;

/**
 * Test concurrent access to JDBC objects.
 */
public class TestConcurrentJdbc extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        String url = "jdbc:h2:mem:";
        for (int i = 0; i < 50; i++) {
            final int x = i % 4;
            final Connection conn = DriverManager.getConnection(url);
            final Statement stat = conn.createStatement();
            stat.execute("create table test(id int primary key)");
            String sql = "";
            switch (x % 6) {
            case 0:
                sql = "select 1";
                break;
            case 1:
            case 2:
                sql = "delete from test";
                break;
            }
            final PreparedStatement prep = conn.prepareStatement(sql);
            final CountDownLatch executedUpdate = new CountDownLatch(1);
            Task t = new Task() {
                @Override
                public void call() throws SQLException {
                    while (!conn.isClosed()) {
                        executedUpdate.countDown();
                        switch (x % 6) {
                        case 0:
                            prep.executeQuery();
                            break;
                        case 1:
                            prep.execute();
                            break;
                        case 2:
                            prep.executeUpdate();
                            break;
                        case 3:
                            stat.executeQuery("select 1");
                            break;
                        case 4:
                            stat.execute("select 1");
                            break;
                        case 5:
                            stat.execute("delete from test");
                            break;
                        }
                    }
                }
            };
            t.execute();
            //Wait until the concurrent task has started
            try {
                executedUpdate.await();
            } catch (InterruptedException e) {
                // ignore
            }
            conn.close();
            SQLException e = (SQLException) t.getException();
            if (e != null) {
                if (ErrorCode.OBJECT_CLOSED != e.getErrorCode() &&
                        ErrorCode.DATABASE_IS_CLOSED != e.getErrorCode() &&
                        ErrorCode.STATEMENT_WAS_CANCELED != e.getErrorCode() &&
                        ErrorCode.DATABASE_CALLED_AT_SHUTDOWN != e.getErrorCode()) {
                    throw e;
                }
            }
        }
    }
}
