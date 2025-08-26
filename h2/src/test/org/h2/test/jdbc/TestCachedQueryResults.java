/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import org.h2.test.TestBase;
import org.h2.test.TestDb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Class TestCachedQueryResults.
 * <UL>
 * <LI> 8/16/25 5:02 PM initial creation
 * </UL>
 *
 * @author <a href="mailto:andrei.tokar@gmail.com">Andrei Tokar</a>
 */
public class TestCachedQueryResults extends TestDb
{
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
        String name = "TestCachedQueryResults";
        deleteDb(name);
        Set<Integer> concurrentSet = ConcurrentHashMap.newKeySet();

        try (Connection mainConn = getConnection(name)) {
            String createTableQuery = "CREATE TABLE IF NOT EXISTS Counter (id INT PRIMARY KEY, counter INT) "
                    + "AS SELECT * FROM (Values (1,0))";

            try (Statement stmt = mainConn.createStatement()) {
                stmt.execute(createTableQuery);
            }

            int threadsCount = 5;
            int taskCount = 100_000;

            ExecutorService executorService = Executors.newFixedThreadPool(threadsCount);
            Callable<Object> callable = () -> {
                try (Connection conn = getConnection(name)) {
                    conn.setAutoCommit(false);

                    String selectQuery = "SELECT counter FROM Counter WHERE id = 1";
                    String lockRowQuery = "SELECT counter FROM Counter WHERE id = 1 FOR UPDATE WAIT 0.5";


                    PreparedStatement selectCounterStmt = conn.prepareStatement(selectQuery);
                    int countBefore = queryCounter(selectCounterStmt); // Select counter before lock

                    PreparedStatement lockStmt = conn.prepareStatement(lockRowQuery);
                    int countAtLock = queryCounter(lockStmt);     // Lock row and select

                    int countAfter = queryCounter(selectCounterStmt);  // select after lock
                    if (countAfter != countAtLock) {
                        println(countAfter + " != " + countAtLock + " " + Thread.currentThread().getName());
                    }
                    if (!concurrentSet.add(countAfter)) {
                        // lost update warning, if concurrentSet already contains current value
                        println("LOST UPDATE! value: " + countAfter);
                    }

                    String updateCounterQuery = "UPDATE Counter SET counter = ? WHERE id = 1";
                    PreparedStatement updateStmt = conn.prepareStatement(updateCounterQuery);
                    updateStmt.setInt(1, countAfter + 1); // Update counter++
                    updateStmt.executeUpdate();

                    conn.commit();

                    selectCounterStmt.close();
                    lockStmt.close();
                    return 0;
                } catch (SQLException e) {
                    println(e.getMessage());
                    return -1;
                }
            };
            ArrayList<Callable<Object>> callables = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                callables.add(callable);
            }

            executorService.invokeAll(callables);

            executorService.shutdownNow();
            deleteDb(name);
            assertEquals(taskCount, concurrentSet.size());
        }
    }

    private static int queryCounter(PreparedStatement stmt) throws SQLException {
        int value = -1;
        try (ResultSet rs1 = stmt.executeQuery()) {
            if (rs1.next()) {
                value = rs1.getInt(1);
            }
        }
        return value;
    }
}
