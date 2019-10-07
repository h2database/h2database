/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.h2.api.ErrorCode;
import org.h2.test.TestAll;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.util.IOUtils;
import org.h2.util.Task;

/**
 * Multi-threaded tests.
 */
public class TestMultiThread extends TestDb implements Runnable {

    private boolean stop;
    private TestMultiThread parent;
    private Random random;

    public TestMultiThread() {
        // nothing to do
    }

    private TestMultiThread(TestAll config, TestMultiThread parent) {
        this.config = config;
        this.parent = parent;
        random = new Random();
    }

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testConcurrentSchemaChange();
        testConcurrentLobAdd();
        testConcurrentView();
        testConcurrentAlter();
        testConcurrentAnalyze();
        testConcurrentInsertUpdateSelect();
        testViews();
        testConcurrentInsert();
        testConcurrentUpdate();
        testConcurrentUpdate2();
        testCheckConstraint();
    }

    private void testConcurrentSchemaChange() throws Exception {
        String db = getTestName();
        deleteDb(db);
        final String url = getURL(db + ";LOCK_TIMEOUT=10000", true);
        try (Connection conn = getConnection(url)) {
            Task[] tasks = new Task[2];
            for (int i = 0; i < tasks.length; i++) {
                final int x = i;
                Task t = new Task() {
                    @Override
                    public void call() throws Exception {
                        try (Connection c2 = getConnection(url)) {
                            Statement stat = c2.createStatement();
                            for (int i = 0; !stop; i++) {
                                stat.execute("create table test" + x + "_" + i);
                                c2.getMetaData().getTables(null, null, null, null);
                                stat.execute("drop table test" + x + "_" + i);
                            }
                        }
                    }
                };
                tasks[i] = t;
                t.execute();
            }
            Thread.sleep(1000);
            for (Task t : tasks) {
                t.get();
            }
        }
    }

    private void testConcurrentLobAdd() throws Exception {
        String db = getTestName();
        deleteDb(db);
        final String url = getURL(db, true);
        try (Connection conn = getConnection(url)) {
            Statement stat = conn.createStatement();
            stat.execute("create table test(id identity, data clob)");
            Task[] tasks = new Task[2];
            for (int i = 0; i < tasks.length; i++) {
                Task t = new Task() {
                    @Override
                    public void call() throws Exception {
                        try (Connection c2 = getConnection(url)) {
                            PreparedStatement p2 = c2
                                    .prepareStatement("insert into test(data) values(?)");
                            while (!stop) {
                                p2.setCharacterStream(1, new StringReader(new String(
                                        new char[10 * 1024])));
                                p2.execute();
                            }
                        }
                    }
                };
                tasks[i] = t;
                t.execute();
            }
            Thread.sleep(500);
            for (Task t : tasks) {
                t.get();
            }
        }
    }

    private void testConcurrentView() throws Exception {
        if (config.mvStore) {
            return;
        }
        String db = getTestName();
        deleteDb(db);
        final String url = getURL(db, true);
        final Random r = new Random();
        try (Connection conn = getConnection(url)) {
            Statement stat = conn.createStatement();
            StringBuilder buff = new StringBuilder();
            buff.append("create table test(id int");
            final int len = 3;
            for (int i = 0; i < len; i++) {
                buff.append(", x" + i + " int");
            }
            buff.append(")");
            stat.execute(buff.toString());
            stat.execute("create view test_view as select * from test");
            stat.execute("insert into test(id) select x from system_range(1, 2)");
            Task t = new Task() {
                @Override
                public void call() throws Exception {
                    Connection c2 = getConnection(url);
                    while (!stop) {
                        c2.prepareStatement("select * from test_view where x" +
                                r.nextInt(len) + "=1");
                    }
                    c2.close();
                }
            };
            t.execute();
            for (int i = 0; i < 1000; i++) {
                conn.prepareStatement("select * from test_view where x" +
                        r.nextInt(len) + "=1");
            }
            t.get();
        }
    }

    private void testConcurrentAlter() throws Exception {
        deleteDb(getTestName());
        try (final Connection conn = getConnection(getTestName())) {
            Statement stat = conn.createStatement();
            Task t = new Task() {
                @Override
                public void call() throws Exception {
                    while (!stop) {
                        conn.prepareStatement("select * from test");
                    }
                }
            };
            stat.execute("create table test(id int)");
            t.execute();
            for (int i = 0; i < 200; i++) {
                stat.execute("alter table test add column x int");
                stat.execute("alter table test drop column x");
            }
            t.get();
        }
    }

    private void testConcurrentAnalyze() throws Exception {
        if (config.mvStore) {
            return;
        }
        deleteDb(getTestName());
        final String url = getURL("concurrentAnalyze", true);
        try (Connection conn = getConnection(url)) {
            Statement stat = conn.createStatement();
            stat.execute("create table test(id bigint primary key) " +
                    "as select x from system_range(1, 1000)");
            Task t = new Task() {
                @Override
                public void call() throws SQLException {
                    try (Connection conn2 = getConnection(url)) {
                        for (int i = 0; i < 1000; i++) {
                            conn2.createStatement().execute("analyze");
                        }
                    }
                }
            };
            t.execute();
            Thread.yield();
            for (int i = 0; i < 1000; i++) {
                conn.createStatement().execute("analyze");
            }
            t.get();
            stat.execute("drop table test");
        }
    }

    private void testConcurrentInsertUpdateSelect() throws Exception {
        try (Connection conn = getConnection()) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
            int len = getSize(10, 200);
            Thread[] threads = new Thread[len];
            for (int i = 0; i < len; i++) {
                threads[i] = new Thread(new TestMultiThread(config, this));
            }
            for (int i = 0; i < len; i++) {
                threads[i].start();
            }
            int sleep = getSize(400, 10000);
            Thread.sleep(sleep);
            this.stop = true;
            for (int i = 0; i < len; i++) {
                threads[i].join();
            }
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM TEST");
            rs.next();
            trace("max id=" + rs.getInt(1));
        }
    }

    private Connection getConnection() throws SQLException {
        return getConnection("jdbc:h2:mem:" + getTestName());
    }

    @Override
    public void run() {
        try (Connection conn = getConnection()) {
            Statement stmt = conn.createStatement();
            while (!parent.stop) {
                stmt.execute("SELECT COUNT(*) FROM TEST");
                stmt.execute("INSERT INTO TEST VALUES(NULL, 'Hi')");
                PreparedStatement prep = conn.prepareStatement(
                        "UPDATE TEST SET NAME='Hello' WHERE ID=?");
                prep.setInt(1, random.nextInt(10000));
                prep.execute();
                prep = conn.prepareStatement("SELECT * FROM TEST WHERE ID=?");
                prep.setInt(1, random.nextInt(10000));
                ResultSet rs = prep.executeQuery();
                while (rs.next()) {
                    rs.getString("NAME");
                }
            }
        } catch (Exception e) {
            logError("multi", e);
        }
    }

    private void testViews() throws Exception {
        // is not supported
        deleteDb("lockMode");
        final String url = getURL("lockMode", true);

        // create some common tables and views
        ExecutorService executor = Executors.newFixedThreadPool(8);
        Connection conn = getConnection(url);
        try {
            Statement stat = conn.createStatement();
            stat.execute(
                    "CREATE TABLE INVOICE(INVOICE_ID INT PRIMARY KEY, AMOUNT DECIMAL)");
            stat.execute("CREATE VIEW INVOICE_VIEW as SELECT * FROM INVOICE");

            stat.execute(
                    "CREATE TABLE INVOICE_DETAIL(DETAIL_ID INT PRIMARY KEY, " +
                    "INVOICE_ID INT, DESCRIPTION VARCHAR)");
            stat.execute(
                    "CREATE VIEW INVOICE_DETAIL_VIEW as SELECT * FROM INVOICE_DETAIL");

            stat.close();

            // create views that reference the common views in different threads
            ArrayList<Future<Void>> jobs = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                final int j = i;
                jobs.add(executor.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        try (Connection conn2 = getConnection(url)) {
                            Statement stat2 = conn2.createStatement();

                            stat2.execute("CREATE VIEW INVOICE_VIEW" + j
                                    + " as SELECT * FROM INVOICE_VIEW");

                            // the following query intermittently results in a
                            // NullPointerException
                            stat2.execute("CREATE VIEW INVOICE_DETAIL_VIEW" + j
                                    + " as SELECT DTL.* FROM INVOICE_VIEW" + j
                                    + " INV JOIN INVOICE_DETAIL_VIEW DTL "
                                    + "ON INV.INVOICE_ID = DTL.INVOICE_ID"
                                    + " WHERE DESCRIPTION='TEST'");

                            ResultSet rs = stat2
                                    .executeQuery("SELECT * FROM INVOICE_VIEW" + j);
                            rs.next();
                            rs.close();

                            rs = stat2.executeQuery(
                                    "SELECT * FROM INVOICE_DETAIL_VIEW" + j);
                            rs.next();
                            rs.close();

                            stat2.close();
                        }
                        return null;
                    }
                }));
            }
            // check for exceptions
            for (Future<Void> job : jobs) {
                try {
                    job.get();
                } catch (ExecutionException ex) {
                    // ignore timeout exceptions, happens periodically when the
                    // machine is really busy and it's not the thing we are
                    // trying to test
                    if (!(ex.getCause() instanceof SQLException)
                            || ((SQLException) ex.getCause()).getErrorCode() != ErrorCode.LOCK_TIMEOUT_1) {
                        throw ex;
                    }
                }
            }
        } finally {
            IOUtils.closeSilently(conn);
            executor.shutdown();
            executor.awaitTermination(20, TimeUnit.SECONDS);
        }

        deleteDb("lockMode");
    }

    private void testConcurrentInsert() throws Exception {
        deleteDb("lockMode");

        final String url = getURL("lockMode;LOCK_TIMEOUT=10000", true);
        int threadCount = 25;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        Connection conn = getConnection(url);
        try {
            conn.createStatement().execute(
                    "CREATE TABLE IF NOT EXISTS TRAN (ID NUMBER(18,0) not null PRIMARY KEY)");

            final ArrayList<Callable<Void>> callables = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                final long initialTransactionId = i * 1000000L;
                callables.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        try (Connection taskConn = getConnection(url)) {
                            taskConn.setAutoCommit(false);
                            PreparedStatement insertTranStmt = taskConn
                                    .prepareStatement("INSERT INTO tran (id) VALUES(?)");
                            // to guarantee uniqueness
                            long tranId = initialTransactionId;
                            for (int j = 0; j < 1000; j++) {
                                insertTranStmt.setLong(1, tranId++);
                                insertTranStmt.execute();
                                taskConn.commit();
                            }
                        }
                        return null;
                    }
                });
            }

            final ArrayList<Future<Void>> jobs = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                jobs.add(executor.submit(callables.get(i)));
            }
            // check for exceptions
            for (Future<Void> job : jobs) {
                job.get(5, TimeUnit.MINUTES);
            }
        } finally {
            IOUtils.closeSilently(conn);
            executor.shutdown();
            executor.awaitTermination(20, TimeUnit.SECONDS);
        }

        deleteDb("lockMode");
    }

    private void testConcurrentUpdate() throws Exception {
        deleteDb("lockMode");

        final int objectCount = 10000;
        final String url = getURL("lockMode;LOCK_TIMEOUT=10000", true);
        int threadCount = 25;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        Connection conn = getConnection(url);
        try {
            conn.createStatement().execute(
                    "CREATE TABLE IF NOT EXISTS ACCOUNT" +
                    "(ID NUMBER(18,0) not null PRIMARY KEY, BALANCE NUMBER null)");
            final PreparedStatement mergeAcctStmt = conn
                    .prepareStatement("MERGE INTO Account(id, balance) key (id) VALUES (?, ?)");
            for (int i = 0; i < objectCount; i++) {
                mergeAcctStmt.setLong(1, i);
                mergeAcctStmt.setBigDecimal(2, BigDecimal.ZERO);
                mergeAcctStmt.execute();
            }

            final ArrayList<Callable<Void>> callables = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                callables.add(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        try (Connection taskConn = getConnection(url)) {
                            taskConn.setAutoCommit(false);
                            final PreparedStatement updateAcctStmt = taskConn
                                    .prepareStatement("UPDATE account SET balance = ? WHERE id = ?");
                            for (int j = 0; j < 1000; j++) {
                                updateAcctStmt.setDouble(1, Math.random());
                                updateAcctStmt.setLong(2, (int) (Math.random() * objectCount));
                                updateAcctStmt.execute();
                                taskConn.commit();
                            }
                        }
                        return null;
                    }
                });
            }

            final ArrayList<Future<Void>> jobs = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                jobs.add(executor.submit(callables.get(i)));
            }
            // check for exceptions
            for (Future<Void> job : jobs) {
                job.get(5, TimeUnit.MINUTES);
            }
        } finally {
            IOUtils.closeSilently(conn);
            executor.shutdown();
            executor.awaitTermination(20, TimeUnit.SECONDS);
        }

        deleteDb("lockMode");
    }

    private final class ConcurrentUpdate2 extends Thread {
        private final String column;

        Throwable exception;

        ConcurrentUpdate2(String column) {
            this.column = column;
        }

        @Override
        public void run() {
            try (Connection c = getConnection("concurrentUpdate2;LOCK_TIMEOUT=10000")) {
                PreparedStatement ps = c.prepareStatement("UPDATE TEST SET V = ? WHERE " + column + " = ?");
                for (int test = 0; test < 1000; test++) {
                    for (int i = 0; i < 16; i++) {
                        ps.setInt(1, test);
                        ps.setInt(2, i);
                        assertEquals(16, ps.executeUpdate());
                    }
                }
            } catch (Throwable e) {
                exception = e;
            }
        }
    }

    private void testConcurrentUpdate2() throws Exception {
        deleteDb("concurrentUpdate2");
        try (Connection c = getConnection("concurrentUpdate2")) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE TEST(A INT, B INT, V INT, PRIMARY KEY(A, B))");
            PreparedStatement ps = c.prepareStatement("INSERT INTO TEST VALUES (?, ?, ?)");
            for (int i = 0; i < 16; i++) {
                for (int j = 0; j < 16; j++) {
                    ps.setInt(1, i);
                    ps.setInt(2, j);
                    ps.setInt(3, 0);
                    ps.executeUpdate();
                }
            }
            ConcurrentUpdate2 a = new ConcurrentUpdate2("A");
            ConcurrentUpdate2 b = new ConcurrentUpdate2("B");
            a.start();
            b.start();
            a.join();
            b.join();
            Throwable e = a.exception;
            if (e == null) {
                e = b.exception;
            }
            if (e != null) {
                if (e instanceof Exception) {
                    throw (Exception) e;
                }
                throw (Error) e;
            }
        } finally {
            deleteDb("concurrentUpdate2");
        }
    }

    private void testCheckConstraint() throws Exception {
        deleteDb("checkConstraint");
        try (Connection c = getConnection("checkConstraint")) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, A INT, B INT)");
            PreparedStatement ps = c.prepareStatement("INSERT INTO TEST VALUES (?, ?, ?)");
            s.execute("ALTER TABLE TEST ADD CONSTRAINT CHECK_A_B CHECK A = B");
            final int numRows = 10;
            for (int i = 0; i < numRows; i++) {
                ps.setInt(1, i);
                ps.setInt(2, 0);
                ps.setInt(3, 0);
                ps.executeUpdate();
            }
            int numThreads = 4;
            Thread[] threads = new Thread[numThreads];
            final AtomicBoolean error = new AtomicBoolean();
            for (int i = 0; i < numThreads; i++) {
                threads[i] = new Thread() {
                    @Override
                    public void run() {
                        try (Connection c = getConnection("checkConstraint")) {
                            PreparedStatement ps = c.prepareStatement("UPDATE TEST SET A = ?, B = ? WHERE ID = ?");
                            Random r = new Random();
                            for (int i = 0; i < 1_000; i++) {
                                int v = r.nextInt(1_000);
                                ps.setInt(1, v);
                                ps.setInt(2, v);
                                ps.setInt(3, r.nextInt(numRows));
                                ps.executeUpdate();
                            }
                        } catch (SQLException e) {
                            error.set(true);
                            synchronized (TestMultiThread.this) {
                                logError("Error in CHECK constraint", e);
                            }
                        }
                    }
                };
            }
            for (int i = 0; i < numThreads; i++) {
                threads[i].start();
            }
            for (int i = 0; i < numThreads; i++) {
                threads[i].join();
            }
            assertFalse(error.get());
        } finally {
            deleteDb("checkConstraint");
        }
    }

}
