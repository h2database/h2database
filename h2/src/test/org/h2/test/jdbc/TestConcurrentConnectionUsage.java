/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.util.Task;

/**
 * Test concurrent usage of the same connection.
 */
public class TestConcurrentConnectionUsage extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws SQLException {
        testAutoCommit();
        testSessionUsage(false);
        testSessionUsage(true);
    }

    private void testAutoCommit() throws SQLException {
        deleteDb(getTestName());
        final Connection conn = getConnection(getTestName());
        final PreparedStatement p1 = conn.prepareStatement("select 1 from dual");
        Task t = new Task() {
            @Override
            public void call() throws Exception {
                while (!stop) {
                    p1.executeQuery();
                    conn.setAutoCommit(true);
                    conn.setAutoCommit(false);
                }
            }
        }.execute();
        PreparedStatement prep = conn.prepareStatement("select ? from dual");
        for (int i = 0; i < 10; i++) {
            prep.setBinaryStream(1, new ByteArrayInputStream(new byte[1024]));
            prep.executeQuery();
        }
        t.get();
        conn.close();
    }

    
    private void testSessionUsage(boolean useLegacy) throws SQLException {
        deleteDb(getTestName());
        try (Connection infoConn = getConnection(getTestName() + ";OLD_INFORMATION_SCHEMA=" + useLegacy)) {
            Statement stmt = infoConn.createStatement();
            stmt.execute("CREATE TABLE t(id INT PRIMARY KEY)");

            int threadCount = 8;
            Task[] tasks = new Task[threadCount];
            for (int i = 0; i < threadCount; i++) {
                tasks[i] = new Task() {
                    @Override
                    public void call() throws Exception {
                        try (Connection txConn = getConnection(getTestName())) {
                            Statement st = txConn.createStatement();
                            while (!stop) {
                                st.executeUpdate("MERGE INTO t(id) KEY (id) VALUES (0)");
                            }
                        }
                    }
                }.execute();
            }

            for (int i = 0; i < 1_000; i++) {
                stmt.executeQuery("SELECT ISOLATION_LEVEL FROM INFORMATION_SCHEMA.SESSIONS").close();
            }

            for (Task task : tasks) {
                task.get();
            }
        }
    }
}
