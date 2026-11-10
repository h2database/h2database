/*
 * Copyright 2004-2026 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.h2.engine.SessionLocal;
import org.h2.jdbc.JdbcConnection;
import org.h2.mvstore.tx.Transaction;
import org.h2.store.fs.FileBaseDefault;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathWrapper;
import org.h2.store.fs.FileUtils;
import org.h2.table.Table;
import org.h2.test.TestBase;

/** Checks version protection at real file reads during post-commit analysis. */
public class TestAutomaticAnalyzeSnapshot extends TestBase {
    private static SessionLocal session;
    private static boolean armed;
    private static int reads;
    private static int unprotected;
    private static Object analysisTransaction;
    private static boolean failScan;
    private static boolean injected;
    private static boolean released;
    private static final Field TRANSACTION;
    private static final Field COUNTER;
    static {
        // Observe the existing transaction without creating one from the test.
        try {
            TRANSACTION = SessionLocal.class.getDeclaredField("transaction");
            COUNTER = Transaction.class.getDeclaredField("txCounter");
            TRANSACTION.setAccessible(true);
            COUNTER.setAccessible(true);
        } catch (Exception error) {
            throw new ExceptionInInitializerError(error);
        }
    }

    /** Transparent file path whose channels retain actual filesystem and locking behavior. */
    public static final class SnapshotPath extends FilePathWrapper {
        /** Give this isolated test its own registered filesystem prefix. */
        @Override
        public String getScheme() {
            return "analyzesnapshot";
        }
        /** Wrap real files solely to inspect snapshot ownership on actual analysis reads. */
        @Override
        public FileChannel open(String mode) throws IOException {
            return new InspectChannel(super.open(mode));
        }
    }

    /** Delegates every operation to the original file channel and only observes selected reads. */
    private static final class InspectChannel extends FileBaseDefault {
        private final FileChannel channel;
        /** Keep the original file as the sole source of bytes and lock ownership. */
        InspectChannel(FileChannel channel) { this.channel = channel; }
        /** Count real reads performed by the committing thread's automatic table scan. */
        @Override public int read(ByteBuffer dst, long pos) throws IOException {
            int result = channel.read(dst, pos);
            // Startup and unrelated reads must not be mistaken for automatic analysis.
            if (armed) {
                // Observe only automatic analysis in the committing thread.
                for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
                    // A scan inside Analyze must own a version for as long as its cursor is in use.
                    if (frame.getClassName().equals("org.h2.command.ddl.Analyze")
                            && frame.getMethodName().equals("analyzeTable")) {
                        reads++;
                        try {
                            Object transaction = TRANSACTION.get(session);
                            analysisTransaction = transaction;
                            // A missing transaction or counter leaves the scan unprotected.
                            if (transaction == null || COUNTER.get(transaction) == null) {
                                unprotected++;
                            }
                        } catch (IllegalAccessException error) {
                            throw new IOException(error);
                        }
                        // Fail once at a real scan read to verify exceptional snapshot cleanup.
                        if (failScan && !injected) {
                            injected = true;
                            throw new IllegalStateException("injected analysis read failure");
                        }
                        break;
                    }
                }
            }
            return result;
        }
        /** Preserve actual writes to the independent fixture. */
        @Override
        public int write(ByteBuffer src, long pos) throws IOException {
            return channel.write(src, pos);
        }
        /** Expose the real file extent to MVStore. */
        @Override
        public long size() throws IOException {
            return channel.size();
        }
        /** Keep truncation delegated so normal close follows the product's own flow. */
        @Override
        protected void implTruncate(long size) throws IOException {
            channel.truncate(size);
        }
        /** Retain all existing durability barriers. */
        @Override
        public void force(boolean metadata) throws IOException {
            channel.force(metadata);
        }
        /** Preserve native file locking. */
        @Override public FileLock tryLock(long pos, long size, boolean shared) throws IOException {
            return channel.tryLock(pos, size, shared);
        }
        /** Close the original file when the database releases the wrapper. */
        @Override
        protected void implCloseChannel() throws IOException {
            channel.close();
        }
    }

    /** Runs the test with the native test framework. */
    public static void main(String... args) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    /** Verifies version ownership and cleanup under every isolation level and a failed scan. */
    @Override
    public void test() throws Exception {
        Path parent = Path.of(getBaseDir());
        Files.createDirectories(parent);
        Path root = Files.createTempDirectory(parent, "automatic-analyze-snapshot-");
        // Unregister the test filesystem and remove only the generated fixture files.
        try {
            for (String isolation : new String[] { "READ COMMITTED", "READ UNCOMMITTED",
                    "REPEATABLE READ", "SNAPSHOT", "SERIALIZABLE" }) {
                checkSnapshot(root.resolve(isolation.replace(' ', '_')), isolation, false);
            }
            checkSnapshot(root.resolve("error"), "READ COMMITTED", true);
        } finally {
            FilePath.unregister(new SnapshotPath());
            FileUtils.deleteRecursive(root.toString(), false);
        }
    }

    /** Observes cleanup before connection close can release a missing statement registration. */
    private void checkSnapshot(Path root, String isolation, boolean injectFailure) throws Exception {
        long start = System.nanoTime();
        session = null;
        armed = false;
        reads = unprotected = 0;
        analysisTransaction = null;
        injected = released = false;
        failScan = injectFailure;
        Files.createDirectory(root);
        FilePath.register(new SnapshotPath());
        String url = "jdbc:h2:analyzesnapshot:" + root.resolve("database").toString().replace('\\', '/')
                + ";CACHE_SIZE=0;ANALYZE_AUTO=0";
        // Seed and close a real file so the subsequent analysis must load actual saved pages.
        try (Connection connection = DriverManager.getConnection(url, "sa", "");
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE SAMPLE(ID INT PRIMARY KEY, VAL VARCHAR)");
            statement.execute("INSERT INTO SAMPLE SELECT X, SPACE(1024) FROM SYSTEM_RANGE(1,32)");
        }
        // Analyze after the committing transaction has ended.
        try (Connection connection = DriverManager.getConnection(url, "sa", "")) {
            try (Statement settings = connection.createStatement()) {
                settings.execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL " + isolation);
            }
            session = (SessionLocal) ((JdbcConnection) connection).getSession();
            Table table = session.getDatabase().getSchema("PUBLIC").findTableOrView(session, "SAMPLE");
            session.lock();
            // Use the same session lock held by ordinary JDBC command execution.
            try {
                session.getTransaction();
                session.markTableForAnalyze(table);
                armed = true;
                // Observe release before rollback or closing the connection can release it for us.
                try {
                    session.commit(false);
                    if (failScan) {
                        throw new AssertionError("scan failure was not injected");
                    }
                } catch (RuntimeException error) {
                    if (!failScan || !injected) {
                        throw error;
                    }
                }
                released = analysisTransaction != null && COUNTER.get(analysisTransaction) == null;
                // Stronger isolation retains transaction snapshots until rollback after an error.
                if (failScan && !isolation.equals("READ COMMITTED") && !isolation.equals("READ UNCOMMITTED")) {
                    session.rollback();
                    released = COUNTER.get(analysisTransaction) == null;
                }
            } finally {
                armed = false;
                session.unlock();
            }
        }
        System.out.println("event=automatic_analyze_snapshot stage=commit outcome="
                + (reads > 0 && unprotected == 0 && released ? "pass" : "fail")
                + " costMs=" + (System.nanoTime() - start) / 1_000_000
                + " isolation=" + isolation + " failScan=" + failScan
                + " actualFileReads=" + reads + " unprotectedReads=" + unprotected
                + " snapshotReleased=" + released);
        // Zero reads must fail too; an empty observation proves nothing.
        if (reads == 0 || unprotected != 0 || !released) {
            throw new AssertionError("automatic analysis reads without a registered version: " + unprotected);
        }
    }
}
