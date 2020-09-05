/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.server;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Tests H2 TCP server protocol with `lazy_query_execution`.
 */
public class TestLazyResultRemote extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase test = TestBase.createCaller().init();
        test.config.networked = true;
        test.testFromMain();
    }

    @Override
    public boolean isEnabled() {
        return config.networked;
    }

    @Override
    public void test() throws Exception {
        boolean originalLazy = config.lazy;
        deleteDb("test");
        for (int lazy = 0; lazy <= 1; lazy ++) {
            config.lazy = lazy != 0;
            for (int fetchSize = 2; fetchSize <= 4; fetchSize ++) {
                try (
                    Connection conn = getConnection("test");
                    Statement stmt = conn.createStatement();
                ) {
                    stmt.setFetchSize(fetchSize);
                    stmt.execute("DROP TABLE IF EXISTS test");
                    stmt.execute("CREATE TABLE test (id INT PRIMARY KEY, x1 INT)");
                    stmt.execute("INSERT INTO test (id, x1) VALUES (1, 2), (2, 3), (3, 4)");
                    // fetchSize = 2: COMMAND_EXECUTE_QUERY(2) + RESULT_FETCH_ROWS(1)
                    // fetchSize = 3: COMMAND_EXECUTE_QUERY(3) + RESULT_FETCH_ROWS(0)
                    // fetchSize = 4: COMMAND_EXECUTE_QUERY(3)
                    try (ResultSet rs = stmt.executeQuery("SELECT id, x1 FROM test")) {
                        int rowCount = 0;
                        while (rs.next()) {
                            rowCount ++;
                        }
                        assertEquals(3, rowCount);
                    }
                    stmt.execute("INSERT INTO test (id, x1) VALUES (4, 5), (5, 6), (6, 7)");
                    // fetchSize = 2: COMMAND_EXECUTE_QUERY(2) + RESULT_FETCH_ROWS(2 + 2 + 0)
                    // fetchSize = 3: COMMAND_EXECUTE_QUERY(3) + RESULT_FETCH_ROWS(3 + 0)
                    // fetchSize = 4: COMMAND_EXECUTE_QUERY(4) + RESULT_FETCH_ROWS(2)
                    try (ResultSet rs = stmt.executeQuery("SELECT id, x1 FROM test")) {
                        int rowCount = 0;
                        while (rs.next()) {
                            rowCount ++;
                        }
                        assertEquals(6, rowCount);
                    }
                }
            }
        }
        deleteDb("test");
        config.lazy = originalLazy;
    }

}
