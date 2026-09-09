/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.h2.api.ErrorCode;
import org.h2.api.TableEngine;
import org.h2.command.Command;
import org.h2.command.CommandInterface;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.SessionLocal;
import org.h2.mvstore.db.MVTable;
import org.h2.table.Table;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Tests statistics updates with query timeouts and statement cancellation.
 */
public class TestAnalyze extends TestDb {

    private static volatile ScanGate scanGate;

    /**
     * Run just this test.
     *
     * @param args ignored
     */
    public static void main(String... args) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        testTimeout();
        testCancel();
        testAutomaticCancel();
    }

    private void testTimeout() throws Exception {
        for (boolean automatic : new boolean[] { false, true }) {
            for (boolean autoCommit : new boolean[] { false, true }) {
                try (Connection conn = getConnection("mem:analyzeTimeout;ANALYZE_AUTO="
                        + (automatic ? 4 : 0)); Statement stat = conn.createStatement()) {
                    stat.execute("CREATE TABLE TEST(ID INT)");
                    try (Statement validation = conn.createStatement()) {
                        validation.setQueryTimeout(300);
                        assertResult("1", validation, "SELECT 1");
                    }
                    conn.setAutoCommit(autoCommit);
                    stat.execute("INSERT INTO TEST SELECT X FROM SYSTEM_RANGE(1, 8)");
                    if (!autoCommit) {
                        assertSelectivity(stat, 50);
                        conn.commit();
                    }
                    if (!automatic) {
                        assertSelectivity(stat, 50);
                        stat.execute("ANALYZE TABLE TEST SAMPLE_SIZE 0");
                        stat.execute("CREATE TABLE EMPTY(ID INT)");
                        stat.execute("ANALYZE SAMPLE_SIZE 0");
                    }
                    assertSelectivity(stat, 100);
                    assertResult("8", stat, "SELECT COUNT(*) FROM TEST");
                }
            }
        }
        try (Connection conn = getConnection("mem:analyzeRollback;ANALYZE_AUTO=4");
                Statement stat = conn.createStatement()) {
            stat.execute("CREATE TABLE TEST(ID INT)");
            stat.setQueryTimeout(300);
            conn.setAutoCommit(false);
            stat.execute("INSERT INTO TEST SELECT X FROM SYSTEM_RANGE(1, 8)");
            conn.rollback();
            assertResult("0", stat, "SELECT COUNT(*) FROM TEST");
            try (Connection observer = getConnection("mem:analyzeRollback");
                    Statement check = observer.createStatement()) {
                stat.execute("INSERT INTO TEST VALUES(9)");
                conn.close();
                assertResult("0", check, "SELECT COUNT(*) FROM TEST");
            }
        }
    }

    private void testCancel() throws Exception {
        try (Connection conn = getConnection("mem:analyzeCancel;ANALYZE_AUTO=0");
                Statement stat = conn.createStatement()) {
            stat.execute("CREATE TABLE TEST(ID INT) ENGINE \"" + GatedTableEngine.class.getName() + '"');
            stat.execute("INSERT INTO TEST SELECT X FROM SYSTEM_RANGE(1, 8)");
            ScanGate gate = new ScanGate();
            FutureTask<Integer> analyze = new FutureTask<>(() -> {
                try {
                    stat.execute("ANALYZE TABLE TEST SAMPLE_SIZE 0");
                    return 0;
                } catch (SQLException e) {
                    return e.getErrorCode();
                }
            });
            Thread worker = new Thread(analyze, "analyze-cancel");
            worker.setDaemon(true);
            scanGate = gate;
            try {
                worker.start();
                assertTrue("ANALYZE did not reach the table", gate.entered.await(10, TimeUnit.SECONDS));
                stat.cancel();
                // TCP cancellation is one-way: wait for delivery before releasing the scan.
                Field canceled = Command.class.getDeclaredField("cancel");
                canceled.setAccessible(true);
                Command command = gate.session.getCurrentCommand();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!canceled.getBoolean(command)) {
                    assertTrue("Cancellation did not reach the server", System.nanoTime() - deadline < 0);
                    Thread.yield();
                }
            } finally {
                gate.resume.countDown();
                scanGate = null;
                worker.join(10_000);
            }
            assertFalse("ANALYZE did not finish after cancellation", worker.isAlive());
            assertEquals(ErrorCode.STATEMENT_WAS_CANCELED, analyze.get(1, TimeUnit.SECONDS).intValue());
            assertSelectivity(stat, 50);
            assertResult("8", stat, "SELECT COUNT(*) FROM TEST");
            stat.execute("ANALYZE TABLE TEST SAMPLE_SIZE 0");
            assertSelectivity(stat, 100);
        }
    }

    private void assertSelectivity(Statement stat, int expected) throws SQLException {
        try (ResultSet rs = stat.executeQuery("SELECT SELECTIVITY FROM INFORMATION_SCHEMA.COLUMNS "
                + "WHERE TABLE_SCHEMA='PUBLIC' AND TABLE_NAME='TEST' AND COLUMN_NAME='ID'")) {
            assertTrue(rs.next());
            assertEquals(expected, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    private void testAutomaticCancel() throws Exception {
        testAutomaticCancel(false);
        testAutomaticCancel(true);
    }

    private void testAutomaticCancel(boolean delete) throws Exception {
        try (Connection conn = getConnection("mem:analyzeAutomaticCancel;ANALYZE_AUTO=4");
                Statement stat = conn.createStatement()) {
            stat.execute("CREATE TABLE TEST(ID INT) ENGINE \"" + GatedTableEngine.class.getName() + '"');
            if (delete) {
                stat.execute("INSERT INTO TEST SELECT X FROM SYSTEM_RANGE(1, 8)");
                assertSelectivity(stat, 100);
            }
            ScanGate gate = new ScanGate();
            FutureTask<Integer> insert = new FutureTask<>(() ->
                    stat.executeUpdate(delete ? "DELETE FROM TEST"
                            : "INSERT INTO TEST SELECT X FROM SYSTEM_RANGE(1, 8)"));
            Thread worker = new Thread(insert, "analyze-automatic-cancel");
            worker.setDaemon(true);
            scanGate = gate;
            try {
                worker.start();
                assertTrue("Automatic analysis did not start", gate.entered.await(10, TimeUnit.SECONDS));
                gate.session.cancel();
            } finally {
                gate.resume.countDown();
                scanGate = null;
                worker.join(10_000);
            }
            assertFalse("Automatic analysis did not finish", worker.isAlive());
            assertEquals(8, insert.get(1, TimeUnit.SECONDS).intValue());
            stat.setQueryTimeout(0);
            assertSelectivity(stat, delete ? 100 : 50);
            assertResult(delete ? "0" : "8", stat, "SELECT COUNT(*) FROM TEST");
            stat.execute("ANALYZE TABLE TEST SAMPLE_SIZE 0");
            // Empty-table statistics are exposed as the default selectivity.
            assertSelectivity(stat, delete ? 50 : 100);
        }
    }

    private static final class ScanGate {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch resume = new CountDownLatch(1);
        SessionLocal session;
    }

    /**
     * Holds ANALYZE at the start of its table scan so another thread can cancel
     * the actual JDBC statement without relying on a large or slow data set.
     */
    public static final class GatedTableEngine implements TableEngine {

        @Override
        public Table createTable(CreateTableData data) {
            return new MVTable(data, data.session.getDatabase().getStore()) {
                @Override
                public boolean lock(SessionLocal session, int lockType) {
                    boolean result = super.lock(session, lockType);
                    ScanGate gate = scanGate;
                    Command command = session.getCurrentCommand();
                    if (gate != null && lockType == Table.READ_LOCK
                            && (command == null || command.getCommandType() == CommandInterface.ANALYZE)) {
                        gate.session = session;
                        gate.entered.countDown();
                        try {
                            if (!gate.resume.await(10, TimeUnit.SECONDS)) {
                                throw new AssertionError("Cancellation did not release the scan");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }
                    return result;
                }
            };
        }
    }
}
