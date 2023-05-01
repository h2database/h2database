/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Objects;
import java.util.SimpleTimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.h2.engine.SessionLocal;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.mvstore.MVStoreException;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FileUtils;
import org.h2.test.utils.ResultVerifier;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * The base class for all tests.
 */
public abstract class TestBase {

    /**
     * The base directory.
     */
    public static final String BASE_TEST_DIR = "./data";

    /**
     * An id used to create unique file names.
     */
    protected static int uniqueId;

    /**
     * The temporary directory.
     */
    private static final String TEMP_DIR = "./data/temp";

    /**
     * The base directory to write test databases.
     */
    private static String baseDir = getTestDir("");

    /**
     * The maximum size of byte array.
     */
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    /**
     * The test configuration.
     */
    public TestAll config;

    /**
     * The time when the test was started.
     */
    protected long start;

    private final LinkedList<byte[]> memory = new LinkedList<>();

    private static final DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HH:mm:ss");

    /**
     * Get the test directory for this test.
     *
     * @param name the directory name suffix
     * @return the test directory
     */
    public static String getTestDir(String name) {
        return BASE_TEST_DIR + "/test" + name;
    }

    /**
     * Initialize the test configuration using the default settings.
     *
     * @return itself
     */
    public TestBase init() throws Exception {
        return init(new TestAll());
    }

    /**
     * Initialize the test configuration.
     *
     * @param conf the configuration
     * @return itself
     */
    public TestBase init(TestAll conf) throws Exception {
        baseDir = getTestDir("");
        FileUtils.createDirectories(baseDir);
        System.setProperty("java.io.tmpdir", TEMP_DIR);
        this.config = conf;
        return this;
    }

    /**
     * This method is initializes the test, runs the test by calling the test()
     * method, and prints status information. It also catches exceptions so that
     * the tests can continue.
     *
     * @param conf the test configuration
     */
    public void runTest(TestAll conf) {
        if (conf.abbaLockingDetector != null) {
            conf.abbaLockingDetector.reset();
        }
        try {
            init(conf);
            if (!isEnabled()) {
                conf.executedTests.putIfAbsent(getClass(), false);
                return;
            }
            conf.executedTests.put(getClass(), true);
            start = System.nanoTime();
            test();
            println("");
        } catch (Throwable e) {
            println("FAIL " + e.toString());
            logError("FAIL ("+conf+") " + e.toString(), e);
            if (config.stopOnError) {
                throw new AssertionError("ERROR");
            }
            TestAll.atLeastOneTestFailed = true;
            if (e instanceof OutOfMemoryError) {
                throw (OutOfMemoryError) e;
            }
        }
    }

    /**
     * Get the password to use to login for the given user password. The file
     * password is added if required.
     *
     * @param userPassword the password of this user
     * @return the login password
     */
    protected String getPassword(String userPassword) {
        return config == null || config.cipher == null ?
                userPassword : getFilePassword() + " " + userPassword;
    }

    /**
     * Get the file password (only required if file encryption is used).
     *
     * @return the file password
     */
    protected String getFilePassword() {
        return "filePassword";
    }

    /**
     * Get the login password. This is usually the user password. If file
     * encryption is used it is combined with the file password.
     *
     * @return the login password
     */
    protected String getPassword() {
        return getPassword("123");
    }

    /**
     * Get the base directory for tests.
     * If a special file system is used, the prefix is prepended.
     *
     * @return the directory, possibly including file system prefix
     */
    public String getBaseDir() {
        String dir = baseDir;
        if (config != null) {
            if (config.reopen) {
                dir = "rec:memFS:" + dir;
            }
            if (config.splitFileSystem) {
                dir = "split:16:" + dir;
            }
        }
        // return "split:nioMapped:" + baseDir;
        return dir;
    }



    /**
     * Get the small or the big value depending on the configuration.
     *
     * @param small the value to return if the current test mode is 'small'
     * @param big the value to return if the current test mode is 'big'
     * @return small or big, depending on the configuration
     */
    protected int getSize(int small, int big) {
        return config.endless ? Integer.MAX_VALUE : config.big ? big : small;
    }

    protected String getUser() {
        return "sa";
    }

    /**
     * Write a message to system out if trace is enabled.
     *
     * @param x the value to write
     */
    protected void trace(int x) {
        trace("" + x);
    }

    /**
     * Write a message to system out if trace is enabled.
     *
     * @param s the message to write
     */
    public void trace(String s) {
        if (config.traceTest) {
            println(s);
        }
    }

    /**
     * Print how much memory is currently used.
     */
    protected void traceMemory() {
        if (config.traceTest) {
            trace("mem=" + getMemoryUsed());
        }
    }

    /**
     * Print the currently used memory, the message and the given time in
     * milliseconds.
     *
     * @param s the message
     * @param time the time in millis
     */
    public void printTimeMemory(String s, long time) {
        if (config.big) {
            Runtime rt = Runtime.getRuntime();
            long memNow = rt.totalMemory() - rt.freeMemory();
            println(memNow / 1024 / 1024 + " MB: " + s + " ms: " + time);
        }
    }

    /**
     * Get the number of megabytes heap memory in use.
     *
     * @return the used megabytes
     */
    public static int getMemoryUsed() {
        return (int) (getMemoryUsedBytes() / 1024 / 1024);
    }

    /**
     * Get the number of bytes heap memory in use.
     *
     * @return the used bytes
     */
    public static long getMemoryUsedBytes() {
        Runtime rt = Runtime.getRuntime();
        long memory = Long.MAX_VALUE;
        for (int i = 0; i < 8; i++) {
            rt.gc();
            long memNow = rt.totalMemory() - rt.freeMemory();
            if (memNow >= memory) {
                break;
            }
            memory = memNow;
        }
        return memory;
    }

    /**
     * Called if the test reached a point that was not expected.
     *
     * @throws AssertionError always throws an AssertionError
     */
    public void fail() {
        fail("Failure");
    }

    /**
     * Called if the test reached a point that was not expected.
     *
     * @param string the error message
     * @throws AssertionError always throws an AssertionError
     */
    protected void fail(String string) {
        if (string.length() > 100) {
            // avoid long strings with special characters, because they are slow
            // to display in Eclipse
            char[] data = string.toCharArray();
            for (int i = 0; i < data.length; i++) {
                char c = data[i];
                if (c >= 128 || c < 32) {
                    data[i] = (char) ('a' + (c & 15));
                    string = null;
                }
            }
            if (string == null) {
                string = new String(data);
            }
        }
        println(string);
        throw new AssertionError(string);
    }

    /**
     * Log an error message.
     *
     * @param s the message
     */
    public static void logErrorMessage(String s) {
        System.out.flush();
        System.err.println("ERROR: " + s + "------------------------------");
        logThrowable(s, null);
    }

    /**
     * Log an error message.
     *
     * @param s the message
     * @param e the exception
     */
    public static void logError(String s, Throwable e) {
        if (e == null) {
            e = new Exception(s);
        }
        System.out.flush();
        System.err.println("ERROR: " + s + " " + e.toString()
                + " ------------------------------");
        e.printStackTrace();
        logThrowable(null, e);
    }

    private static void logThrowable(String s, Throwable e) {
        // synchronize on this class, because file locks are only visible to
        // other JVMs
        synchronized (TestBase.class) {
            try {
                // lock
                FileChannel fc = FilePath.get("error.lock").open("rw");
                FileLock lock;
                while (true) {
                    lock = fc.tryLock();
                    if (lock != null) {
                        break;
                    }
                    Thread.sleep(10);
                }
                // append
                FileWriter fw = new FileWriter("error.txt", true);
                if (s != null) {
                    fw.write(s);
                }
                if (e != null) {
                    PrintWriter pw = new PrintWriter(fw);
                    e.printStackTrace(pw);
                    pw.close();
                }
                fw.close();
                // unlock
                lock.release();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
        System.err.flush();
    }

    /**
     * Print a message to system out.
     *
     * @param s the message
     */
    public void println(String s) {
        long now = System.nanoTime();
        long time = TimeUnit.NANOSECONDS.toMillis(now - start);
        printlnWithTime(time, getClass().getName() + ' ' + s);
    }

    /**
     * Print a message, prepended with the specified time in milliseconds.
     *
     * @param millis the time in milliseconds
     * @param s the message
     */
    static synchronized void printlnWithTime(long millis, String s) {
        StringBuilder builder = new StringBuilder(s.length() + 19);
        timeFormat.formatTo(LocalTime.now(), builder);
        System.out.println(formatTime(builder.append(' '), millis).append(' ').append(s).toString());
    }

    /**
     * Print the current time and a message to system out.
     *
     * @param s the message
     */
    protected void printTime(String s) {
        StringBuilder builder = new StringBuilder(s.length() + 9);
        timeFormat.formatTo(LocalTime.now(), builder);
        println(builder.append(' ').append(s).toString());
    }

    /**
     * Format the time in the format mm:ss.123 or hh:mm:ss.123 where 123 is
     * milliseconds.
     *
     * @param builder the string builder to append to
     * @param millis the time in milliseconds, non-negative
     * @return the specified string builder
     */
    static StringBuilder formatTime(StringBuilder builder, long millis) {
        int s = (int) (millis / 1_000);
        int m = s / 60;
        s %= 60;
        int h = m / 60;
        if (h != 0) {
            builder.append(h).append(':');
            m %= 60;
        }
        StringUtils.appendTwoDigits(builder, m).append(':');
        StringUtils.appendTwoDigits(builder, s).append('.');
        StringUtils.appendZeroPadded(builder, 3, (int) (millis % 1_000));
        return builder;
    }

    /**
     * @return whether this test is enabled in the current configuration
     */
    public boolean isEnabled() {
        return true;
    }

    /**
     * This method will be called by the test framework.
     *
     * @throws Exception if an exception in the test occurs
     */
    public abstract void test() throws Exception;

    /**
     * Only called from individual test classes main() method,
     * makes sure to run the before/after stuff.
     *
     * @throws Exception if an exception in the test occurs
     */
    public final void testFromMain() throws Exception {
        config.beforeTest();
        test();
        config.afterTest();
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     *
     * @param message the message to print in case of error
     * @param expected the expected value
     * @param actual the actual value
     * @throws AssertionError if the values are not equal
     */
    public void assertEquals(String message, int expected, int actual) {
        if (expected != actual) {
            fail("Expected: " + expected + " actual: " + actual + " message: " + message);
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     *
     * @param expected the expected value
     * @param actual the actual value
     * @throws AssertionError if the values are not equal
     */
    public void assertEquals(int expected, int actual) {
        if (expected != actual) {
            fail("Expected: " + expected + " actual: " + actual);
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     *
     * @param expected the expected value
     * @param actual the actual value
     * @throws AssertionError if the values are not equal
     */
    public void assertEquals(byte[] expected, byte[] actual) {
        if (expected == null || actual == null) {
            assertTrue(expected == actual);
            return;
        }
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != actual[i]) {
                fail("[" + i + "]: expected: " + (int) expected[i] +
                        " actual: " + (int) actual[i]);
            }
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     *
     * @param expected the expected value
     * @param actual the actual value
     * @throws AssertionError if the values are not equal
     */
    public void assertEquals(java.util.Date expected, java.util.Date actual) {
        if (!Objects.equals(expected, actual)) {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            SimpleTimeZone gmt = new SimpleTimeZone(0, "Z");
            df.setTimeZone(gmt);
            fail("Expected: " +
                    (expected != null ? df.format(expected) : "null") +
                    " actual: " +
                    (actual != null ? df.format(actual) : "null"));
        }
    }

    /**
     * Check if two arrays are equal, and if not throw an exception.
     * If some of the elements in the arrays are themselves arrays this
     * check is called recursively.
     *
     * @param expected the expected value
     * @param actual the actual value
     * @throws AssertionError if the values are not equal
     */
    public void assertEquals(Object[] expected, Object[] actual) {
        if (expected == null || actual == null) {
            assertTrue(expected == actual);
            return;
        }
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            if (expected[i] == null || actual[i] == null) {
                if (expected[i] != actual[i]) {
                    fail("[" + i + "]: expected: " + expected[i] + " actual: " + actual[i]);
                }
            } else if (expected[i] instanceof Object[] && actual[i] instanceof Object[]) {
                assertEquals((Object[]) expected[i], (Object[]) actual[i]);
            } else if (!expected[i].equals(actual[i])) {
                fail("[" + i + "]: expected: " + expected[i] + " actual: " + actual[i]);
            }
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     *
     * @param expected the expected value
     * @param actual the actual value
     * @throws AssertionError if the values are not equal
     */
    public void assertEquals(Object expected, Object actual) {
        if (!Objects.equals(expected, actual)) {
            fail(" expected: " + expected + " actual: " + actual);
        }
    }

    /**
     * Check if two readers are equal, and if not throw an exception.
     *
     * @param expected the expected value
     * @param actual the actual value
     * @param len the maximum length, or -1
     * @throws AssertionError if the values are not equal
     */
    protected void assertEqualReaders(Reader expected, Reader actual, int len)
            throws IOException {
        for (int i = 0; len < 0 || i < len; i++) {
            int ce = expected.read();
            int ca = actual.read();
            assertEquals("pos:" + i, ce, ca);
            if (ce == -1) {
                break;
            }
        }
        expected.close();
        actual.close();
    }

    /**
     * Check if two streams are equal, and if not throw an exception.
     *
     * @param expected the expected value
     * @param actual the actual value
     * @param len the maximum length, or -1
     * @throws AssertionError if the values are not equal
     */
    protected void assertEqualStreams(InputStream expected, InputStream actual,
            int len) throws IOException {
        // this doesn't actually read anything - just tests reading 0 bytes
        actual.read(new byte[0]);
        expected.read(new byte[0]);
        actual.read(new byte[10], 3, 0);
        expected.read(new byte[10], 0, 0);

        for (int i = 0; len < 0 || i < len; i++) {
            int ca = actual.read();
            actual.read(new byte[0]);
            int ce = expected.read();
            if (ca != ce) {
                assertEquals("Error at index " + i, ce, ca);
            }
            if (ca == -1) {
                break;
            }
        }
        actual.read(new byte[10], 3, 0);
        expected.read(new byte[10], 0, 0);
        actual.read(new byte[0]);
        expected.read(new byte[0]);
        actual.close();
        expected.close();
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     *
     * @param message the message to use if the check fails
     * @param expected the expected value
     * @param actual the actual value
     * @throws AssertionError if the values are not equal
     */
    protected void assertEquals(String message, String expected, String actual) {
        if (expected == null && actual == null) {
            return;
        } else if (expected == null || actual == null) {
            fail("Expected: " + expected + " Actual: " + actual + " " + message);
        } else if (!expected.equals(actual)) {
            int al = expected.length();
            int bl = actual.length();
            for (int i = 0; i < expected.length(); i++) {
                String s = expected.substring(0, i);
                if (!actual.startsWith(s)) {
                    expected = expected.substring(0, i) + "<*>" + expected.substring(i);
                    if (al > 20) {
                        expected = "@" + i + " " + expected;
                    }
                    break;
                }
            }
            if (al > 4000) {
                expected = expected.substring(0, 4000);
            }
            if (bl > 4000) {
                actual = actual.substring(0, 4000);
            }
            fail("Expected: " + expected + " (" + al + ") actual: " + actual
                    + " (" + bl + ") " + message);
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     *
     * @param expected the expected value
     * @param actual the actual value
     * @throws AssertionError if the values are not equal
     */
    protected void assertEquals(String expected, String actual) {
        assertEquals("", expected, actual);
    }

    /**
     * Check if two result sets are equal, and if not throw an exception.
     *
     * @param message the message to use if the check fails
     * @param rs0 the first result set
     * @param rs1 the second result set
     * @throws AssertionError if the values are not equal
     */
    protected void assertEquals(String message, ResultSet rs0, ResultSet rs1)
            throws SQLException {
        ResultSetMetaData meta = rs0.getMetaData();
        int columns = meta.getColumnCount();
        assertEquals(columns, rs1.getMetaData().getColumnCount());
        while (rs0.next()) {
            assertTrue(message, rs1.next());
            for (int i = 0; i < columns; i++) {
                assertEquals(message, rs0.getString(i + 1), rs1.getString(i + 1));
            }
        }
        assertFalse(message, rs0.next());
        assertFalse(message, rs1.next());
    }

    /**
     * Check if two objects are the same, and if not throw an exception.
     *
     * @param expected the expected value
     * @param actual the actual value
     * @throws AssertionError if the objects are not the same
     */
    public void assertSame(Object expected, Object actual) {
        if (expected != actual) {
            fail(" expected: " + expected + " != actual: " + actual);
        }
    }

    /**
     * Check if the first value is larger or equal than the second value, and if
     * not throw an exception.
     *
     * @param a the first value
     * @param b the second value (must be smaller than the first value)
     * @throws AssertionError if the first value is smaller
     */
    protected void assertSmaller(long a, long b) {
        if (a >= b) {
            fail("a: " + a + " is not smaller than b: " + b);
        }
    }

    /**
     * Check that a result contains the given substring.
     *
     * @param result the result value
     * @param contains the term that should appear in the result
     * @throws AssertionError if the term was not found
     */
    protected void assertContains(String result, String contains) {
        if (result.indexOf(contains) < 0) {
            fail(result + " does not contain: " + contains);
        }
    }

    /**
     * Check that a text starts with the expected characters..
     *
     * @param text the text
     * @param expectedStart the expected prefix
     * @throws AssertionError if the text does not start with the expected
     *             characters
     */
    protected void assertStartsWith(String text, String expectedStart) {
        if (!text.startsWith(expectedStart)) {
            fail("[" + text + "] does not start with: [" + expectedStart + "]");
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     *
     * @param expected the expected value
     * @param actual the actual value
     * @throws AssertionError if the values are not equal
     */
    protected void assertEquals(long expected, long actual) {
        if (expected != actual) {
            fail("Expected: " + expected + " actual: " + actual);
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     *
     * @param expected the expected value
     * @param actual the actual value
     * @throws AssertionError if the values are not equal
     */
    protected void assertEquals(double expected, double actual) {
        if (expected != actual) {
            if (Double.isNaN(expected) && Double.isNaN(actual)) {
                // if both a NaN, then there is no error
            } else {
                fail("Expected: " + expected + " actual: " + actual);
            }
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     *
     * @param expected the expected value
     * @param actual the actual value
     * @throws AssertionError if the values are not equal
     */
    protected void assertEquals(float expected, float actual) {
        if (expected != actual) {
            if (Float.isNaN(expected) && Float.isNaN(actual)) {
                // if both a NaN, then there is no error
            } else {
                fail("Expected: " + expected + " actual: " + actual);
            }
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     *
     * @param expected the expected value
     * @param actual the actual value
     * @throws AssertionError if the values are not equal
     */
    protected void assertEquals(boolean expected, boolean actual) {
        if (expected != actual) {
            fail("Boolean expected: " + expected + " actual: " + actual);
        }
    }

    /**
     * Check that the passed boolean is true.
     *
     * @param condition the condition
     * @throws AssertionError if the condition is false
     */
    public void assertTrue(boolean condition) {
        if (!condition) {
            fail("Expected: true got: false");
        }
    }

    /**
     * Check that the passed object is null.
     *
     * @param obj the object
     * @throws AssertionError if the condition is false
     */
    public void assertNull(Object obj) {
        if (obj != null) {
            fail("Expected: null got: " + obj);
        }
    }

    /**
     * Check that the passed object is not null.
     *
     * @param obj the object
     * @throws AssertionError if the condition is false
     */
    public void assertNotNull(Object obj) {
        if (obj == null) {
            fail("Expected: not null got: null");
        }
    }

    /**
     * Check that the passed object is not null.
     *
     * @param message the message to print if the condition is false
     * @param obj the object
     * @throws AssertionError if the condition is false
     */
    public void assertNotNull(String message, Object obj) {
        if (obj == null) {
            fail(message);
        }
    }

    /**
     * Check that the passed boolean is true.
     *
     * @param message the message to print if the condition is false
     * @param condition the condition
     * @throws AssertionError if the condition is false
     */
    public void assertTrue(String message, boolean condition) {
        if (!condition) {
            fail(message);
        }
    }

    /**
     * Check that the passed boolean is false.
     *
     * @param value the condition
     * @throws AssertionError if the condition is true
     */
    protected void assertFalse(boolean value) {
        if (value) {
            fail("Expected: false got: true");
        }
    }

    /**
     * Check that the passed boolean is false.
     *
     * @param message the message to print if the condition is false
     * @param value the condition
     * @throws AssertionError if the condition is true
     */
    protected void assertFalse(String message, boolean value) {
        if (value) {
            fail(message);
        }
    }

    /**
     * Check that the result set row count matches.
     *
     * @param expected the number of expected rows
     * @param rs the result set
     * @throws AssertionError if a different number of rows have been found
     */
    protected void assertResultRowCount(int expected, ResultSet rs)
            throws SQLException {
        int i = 0;
        while (rs.next()) {
            i++;
        }
        assertEquals(expected, i);
    }

    /**
     * Check that the result set of a query is exactly this value.
     *
     * @param stat the statement
     * @param sql the SQL statement to execute
     * @param expected the expected result value
     * @throws AssertionError if a different result value was returned
     */
    protected void assertSingleValue(Statement stat, String sql, int expected)
            throws SQLException {
        ResultSet rs = stat.executeQuery(sql);
        assertTrue(rs.next());
        assertEquals(expected, rs.getInt(1));
        assertFalse(rs.next());
    }

    /**
     * Check that the result set of a query is exactly this value.
     *
     * @param expected the expected result value
     * @param stat the statement
     * @param sql the SQL statement to execute
     * @throws AssertionError if a different result value was returned
     */
    protected void assertResult(String expected, Statement stat, String sql)
            throws SQLException {
        ResultSet rs = stat.executeQuery(sql);
        if (rs.next()) {
            String actual = rs.getString(1);
            assertEquals(expected, actual);
        } else {
            assertEquals(expected, null);
        }
    }

    /**
     * Check that executing the specified query results in the specified error.
     *
     * @param expectedErrorCode the expected error code
     * @param stat the statement
     * @param sql the SQL statement to execute
     */
    protected void assertThrows(int expectedErrorCode, Statement stat,
            String sql) {
        try {
            execute(stat, sql);
            fail("Expected error: " + expectedErrorCode);
        } catch (SQLException ex) {
            assertEquals(expectedErrorCode, ex.getErrorCode());
        }
    }

    /**
     * Execute the statement.
     *
     * @param stat the statement
     */
    public void execute(PreparedStatement stat) throws SQLException {
        execute(stat, null);
    }

    /**
     * Execute the statement.
     *
     * @param stat the statement
     * @param sql the SQL command
     */
    protected void execute(Statement stat, String sql) throws SQLException {
        boolean query = sql == null ? ((PreparedStatement) stat).execute() :
            stat.execute(sql);

        if (query && config.lazy) {
            try (ResultSet rs = stat.getResultSet()) {
                while (rs.next()) {
                    // just loop
                }
            }
        }
    }

    /**
     * Check if the result set meta data is correct.
     *
     * @param rs the result set
     * @param columnCount the expected column count
     * @param labels the expected column labels
     * @param datatypes the expected data types
     * @param precision the expected precisions
     * @param scale the expected scales
     */
    protected void assertResultSetMeta(ResultSet rs, int columnCount,
            String[] labels, int[] datatypes, int[] precision, int[] scale)
            throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int cc = meta.getColumnCount();
        if (cc != columnCount) {
            fail("result set contains " + cc + " columns not " + columnCount);
        }
        for (int i = 0; i < columnCount; i++) {
            if (labels != null) {
                String l = meta.getColumnLabel(i + 1);
                if (!labels[i].equals(l)) {
                    fail("column label " + i + " is " + l + " not " + labels[i]);
                }
            }
            if (datatypes != null) {
                int t = meta.getColumnType(i + 1);
                if (datatypes[i] != t) {
                    fail("column datatype " + i + " is " + t + " not " + datatypes[i] + " (prec="
                            + meta.getPrecision(i + 1) + " scale=" + meta.getScale(i + 1) + ")");
                }
                String typeName = meta.getColumnTypeName(i + 1);
                String className = meta.getColumnClassName(i + 1);
                switch (t) {
                case Types.INTEGER:
                    assertEquals("INTEGER", typeName);
                    assertEquals("java.lang.Integer", className);
                    break;
                case Types.VARCHAR:
                    assertEquals("CHARACTER VARYING", typeName);
                    assertEquals("java.lang.String", className);
                    break;
                case Types.SMALLINT:
                    assertEquals("SMALLINT", typeName);
                    assertEquals("java.lang.Integer", className);
                    break;
                case Types.TIMESTAMP:
                    assertEquals("TIMESTAMP", typeName);
                    assertEquals("java.sql.Timestamp", className);
                    break;
                case Types.NUMERIC:
                    assertEquals("NUMERIC", typeName);
                    assertEquals("java.math.BigDecimal", className);
                    break;
                default:
                }
            }
            if (precision != null) {
                int p = meta.getPrecision(i + 1);
                if (precision[i] != p) {
                    fail("column precision " + i + " is " + p + " not " + precision[i]);
                }
            }
            if (scale != null) {
                int s = meta.getScale(i + 1);
                if (scale[i] != s) {
                    fail("column scale " + i + " is " + s + " not " + scale[i]);
                }
            }

        }
    }

    /**
     * Check if a result set contains the expected data.
     * The sort order is significant
     *
     * @param rs the result set
     * @param data the expected data
     * @param ignoreColumns columns to ignore, or {@code null}
     * @throws AssertionError if there is a mismatch
     */
    protected void assertResultSetOrdered(ResultSet rs, String[][] data, int[] ignoreColumns)
            throws SQLException {
        assertResultSet(true, rs, data, ignoreColumns);
    }

    /**
     * Check if a result set contains the expected data.
     * The sort order is significant
     *
     * @param rs the result set
     * @param data the expected data
     * @throws AssertionError if there is a mismatch
     */
    protected void assertResultSetOrdered(ResultSet rs, String[][] data)
            throws SQLException {
        assertResultSet(true, rs, data, null);
    }

    /**
     * Check if a result set contains the expected data.
     *
     * @param ordered if the sort order is significant
     * @param rs the result set
     * @param data the expected data
     * @param ignoreColumns columns to ignore, or {@code null}
     * @throws AssertionError if there is a mismatch
     */
    private void assertResultSet(boolean ordered, ResultSet rs, String[][] data, int[] ignoreColumns)
            throws SQLException {
        int len = rs.getMetaData().getColumnCount();
        int rows = data.length;
        if (rows == 0) {
            // special case: no rows
            if (rs.next()) {
                fail("testResultSet expected rowCount:" + rows + " got:0");
            }
        }
        int len2 = data[0].length;
        if (len < len2) {
            fail("testResultSet expected columnCount:" + len2 + " got:" + len);
        }
        for (int i = 0; i < rows; i++) {
            if (!rs.next()) {
                fail("testResultSet expected rowCount:" + rows + " got:" + i);
            }
            String[] row = getData(rs, len);
            if (ordered) {
                String[] good = data[i];
                if (!testRow(good, row, good.length, ignoreColumns)) {
                    fail("testResultSet row not equal, got:\n" + formatRow(row)
                            + "\n" + formatRow(good));
                }
            } else {
                boolean found = false;
                for (int j = 0; j < rows; j++) {
                    String[] good = data[i];
                    if (testRow(good, row, good.length, ignoreColumns)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    fail("testResultSet no match for row:" + formatRow(row));
                }
            }
        }
        if (rs.next()) {
            String[] row = getData(rs, len);
            fail("testResultSet expected rowcount:" + rows + " got:>="
                    + (rows + 1) + " data:" + formatRow(row));
        }
    }

    private static boolean testRow(String[] a, String[] b, int len, int[] ignoreColumns) {
        loop: for (int i = 0; i < len; i++) {
            if (ignoreColumns != null) {
                for (int c : ignoreColumns) {
                    if (c == i) {
                        continue loop;
                    }
                }
            }
            String sa = a[i];
            String sb = b[i];
            if (sa == null || sb == null) {
                if (sa != sb) {
                    return false;
                }
            } else {
                if (!sa.equals(sb)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static String[] getData(ResultSet rs, int len) throws SQLException {
        String[] data = new String[len];
        for (int i = 0; i < len; i++) {
            data[i] = rs.getString(i + 1);
            // just check if it works
            rs.getObject(i + 1);
        }
        return data;
    }

    private static String formatRow(String[] row) {
        String sb = "";
        for (String r : row) {
            sb += "{" + r + "}";
        }
        return "{" + sb + "}";
    }

    /**
     * Simulate a database crash. This method will also close the database
     * files, but the files are in a state as the power was switched off. It
     * doesn't throw an exception.
     *
     * @param conn the database connection
     */
    protected void crash(Connection conn) {
        setPowerOffCount(conn, 1);
        try {
            conn.createStatement().execute("SET WRITE_DELAY 0");
            conn.createStatement().execute("CREATE TABLE TEST_A(ID INT)");
            fail("should be crashed already");
        } catch (SQLException e) {
            // expected
        }
        try {
            conn.close();
        } catch (SQLException e) {
            // ignore
        }
    }

    /**
     * Set the number of disk operations before power failure is simulated.
     * To disable the countdown, use 0.
     *
     * @param conn the connection
     * @param i the number of operations
     */
    public static void setPowerOffCount(Connection conn, int i) {
        SessionLocal session = (SessionLocal) ((JdbcConnection) conn).getSession();
        if (session != null) {
            session.getDatabase().setPowerOffCount(i);
        }
    }

    /**
     * Returns the number of disk operations before power failure is simulated.
     *
     * @param conn the connection
     * @return the number of disk operations before power failure is simulated
     */
    protected static int getPowerOffCount(Connection conn) {
        SessionLocal session = (SessionLocal) ((JdbcConnection) conn).getSession();
        return session != null && !session.isClosed() ? session.getDatabase().getPowerOffCount() : 0;
    }

    /**
     * Read a string from the reader. This method reads until end of file.
     *
     * @param reader the reader
     * @return the string read
     */
    protected String readString(Reader reader) {
        if (reader == null) {
            return null;
        }
        StringBuilder buffer = new StringBuilder();
        try {
            while (true) {
                int c = reader.read();
                if (c == -1) {
                    break;
                }
                buffer.append((char) c);
            }
            return buffer.toString();
        } catch (Exception e) {
            assertTrue(false);
            return null;
        }
    }

    /**
     * Check that a given exception is not an unexpected 'general error'
     * exception.
     *
     * @param e the error
     */
    public void assertKnownException(SQLException e) {
        assertKnownException("", e);
    }

    /**
     * Check that a given exception is not an unexpected 'general error'
     * exception.
     *
     * @param message the message
     * @param e the exception
     */
    protected void assertKnownException(String message, SQLException e) {
        if (e != null && e.getSQLState().startsWith("HY000")) {
            TestBase.logError("Unexpected General error " + message, e);
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     *
     * @param expected the expected value
     * @param actual the actual value
     * @throws AssertionError if the values are not equal
     */
    protected void assertEquals(Integer expected, Integer actual) {
        if (expected == null || actual == null) {
            if (expected != actual) {
                assertEquals("" + expected, "" + actual);
            }
        } else {
            assertEquals(expected.intValue(), actual.intValue());
        }
    }

    /**
     * Check if two databases contain the same meta data.
     *
     * @param stat1 the connection to the first database
     * @param stat2 the connection to the second database
     * @throws AssertionError if the databases don't match
     */
    protected void assertEqualDatabases(Statement stat1, Statement stat2)
            throws SQLException {
        ResultSet rs = stat1.executeQuery(
                "SELECT SETTING_VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE SETTING_NAME = 'ANALYZE_AUTO'");
        int analyzeAuto = rs.next() ? rs.getInt(1) : 0;
        if (analyzeAuto > 0) {
            stat1.execute("analyze");
            stat2.execute("analyze");
        }
        ResultSet rs1 = stat1.executeQuery("SCRIPT simple NOPASSWORDS");
        ResultSet rs2 = stat2.executeQuery("SCRIPT simple NOPASSWORDS");
        ArrayList<String> list1 = new ArrayList<>();
        ArrayList<String> list2 = new ArrayList<>();
        while (rs1.next()) {
            String s1 = rs1.getString(1);
            s1 = removeRowCount(s1);
            if (!rs2.next()) {
                fail("expected: " + s1);
            }
            String s2 = rs2.getString(1);
            s2 = removeRowCount(s2);
            if (!s1.equals(s2)) {
                list1.add(s1);
                list2.add(s2);
            }
        }
        for (String s : list1) {
            if (!list2.remove(s)) {
                fail("only found in first: " + s + " remaining: " + list2);
            }
        }
        assertEquals("remaining: " + list2, 0, list2.size());
        assertFalse(rs2.next());
    }

    private static String removeRowCount(String scriptLine) {
        int index = scriptLine.indexOf("+/-");
        if (index >= 0) {
            scriptLine = scriptLine.substring(index);
        }
        return scriptLine;
    }

    /**
     * Create a new object of the calling class.
     *
     * @return the new test
     */
    public static TestBase createCaller() {
        org.h2.Driver.load();
        try {
            return (TestBase) new SecurityManager() {
                Class<?> clazz = getClassContext()[2];
            }.clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the classpath list used to execute java -cp ...
     *
     * @return the classpath list
     */
    protected String getClassPath() {
        return System.getProperty("java.class.path");
    }

    /**
     * Get the path to a java executable of the current process
     *
     * @return the path to java
     */
    public static String getJVM() {
        return System.getProperty("java.home") + File.separatorChar + "bin"
                + File.separator + "java";
    }

    /**
     * Use up almost all memory.
     *
     * @param remainingKB the number of kilobytes that are not referenced
     */
    protected void eatMemory(int remainingKB) {
        long memoryFreeKB;
        try {
            while ((memoryFreeKB = Utils.getMemoryFree()) > remainingKB) {
                long blockSize = Math.max((memoryFreeKB - remainingKB) / 16, 16) * 1024;
                memory.add(new byte[blockSize > MAX_ARRAY_SIZE ? MAX_ARRAY_SIZE : (int) blockSize]);
            }
        } catch (OutOfMemoryError e) {
            if (remainingKB >= 3000) { // OOM is not expected
                memory.clear();
                throw e;
            }
            // OOM can be ignored because it's tolerable (separate process?)
        }
    }

    /**
     * Remove the hard reference to the memory.
     */
    protected void freeMemory() {
        memory.clear();
        for (int i = 0; i < 5; i++) {
            System.gc();
            try {
                Thread.sleep(20);
            } catch (InterruptedException ignore) {/**/}
        }
    }

    /**
     * Verify the next method call on the object will throw an exception.
     *
     * @param <T> the class of the object
     * @param expectedExceptionClass the expected exception class to be thrown
     * @param obj the object to wrap
     * @return a proxy for the object
     */
    protected <T> T assertThrows(final Class<?> expectedExceptionClass,
            final T obj) {
        return assertThrows((returnValue, t, m, args) -> {
            if (t == null) {
                throw new AssertionError("Expected an exception of type " +
                        expectedExceptionClass.getSimpleName() +
                        " to be thrown, but the method returned " +
                        returnValue +
                        " for " + formatMethodCall(m, args));
            }
            if (!expectedExceptionClass.isAssignableFrom(t.getClass())) {
                AssertionError ae = new AssertionError("Expected an exception of type\n" +
                        expectedExceptionClass.getSimpleName() +
                        " to be thrown, but the method under test threw an exception of type\n" +
                        t.getClass().getSimpleName() +
                        " (see in the 'Caused by' for the exception that was thrown) for " +
                        formatMethodCall(m, args));
                ae.initCause(t);
                throw ae;
            }
            return false;
        }, obj);
    }

    private static String formatMethodCall(Method m, Object... args) {
        StringBuilder builder = new StringBuilder();
        builder.append(m.getName()).append('(');
        for (int i = 0; i < args.length; i++) {
            Object a = args[i];
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(a == null ? "null" : a.toString());
        }
        builder.append(")");
        return builder.toString();
    }

    /**
     * Verify the next method call on the object will throw an exception.
     *
     * @param <T> the class of the object
     * @param expectedErrorCode the expected error code
     * @param obj the object to wrap
     * @return a proxy for the object
     */
    protected <T> T assertThrows(int expectedErrorCode, T obj) {
        return assertThrows((returnValue, t, m, args) -> {
            checkErrorCode(expectedErrorCode, t);
            return false;
        }, obj);
    }

    /**
     * Verify the next method call on the object will throw an exception.
     *
     * @param <T> the class of the object
     * @param verifier the result verifier to call
     * @param obj the object to wrap
     * @return a proxy for the object
     */
    @SuppressWarnings("unchecked")
    protected <T> T assertThrows(final ResultVerifier verifier, final T obj) {
        Class<?> c = obj.getClass();
        InvocationHandler ih = new InvocationHandler() {
            private Exception called = new Exception("No method called");
            @Override
            protected void finalize() {
                if (called != null) {
                    called.printStackTrace(System.err);
                }
            }
            @Override
            public Object invoke(Object proxy, Method method, Object[] args)
                    throws Exception {
                try {
                    called = null;
                    Object ret = method.invoke(obj, args);
                    verifier.verify(ret, null, method, args);
                    return ret;
                } catch (InvocationTargetException e) {
                    verifier.verify(null, e.getTargetException(), method, args);
                    Class<?> retClass = method.getReturnType();
                    if (!retClass.isPrimitive()) {
                        return null;
                    }
                    if (retClass == boolean.class) {
                        return false;
                    } else if (retClass == byte.class) {
                        return (byte) 0;
                    } else if (retClass == char.class) {
                        return (char) 0;
                    } else if (retClass == short.class) {
                        return (short) 0;
                    } else if (retClass == int.class) {
                        return 0;
                    } else if (retClass == long.class) {
                        return 0L;
                    } else if (retClass == float.class) {
                        return 0F;
                    } else if (retClass == double.class) {
                        return 0D;
                    }
                    return null;
                }
            }
        };
        Class<?>[] interfaces = c.getInterfaces();
        if (interfaces.length == 0) {
            throw new RuntimeException("Can not create a proxy for the class " +
                    c.getSimpleName() +
                    " because it doesn't implement any interfaces and is final");
        }
        return (T) Proxy.newProxyInstance(c.getClassLoader(), interfaces, ih);
    }

    @FunctionalInterface
    protected interface VoidCallable {

        /**
         * call the lambda
         */
        void call() throws Exception;

    }

    /**
     * Assert that the lambda function throws an exception of the expected class.
     *
     * @param expectedExceptionClass expected exception class
     * @param c lambda function
     */
    protected void assertThrows(Class<?> expectedExceptionClass, Callable<?> c) {
        try {
            Object returnValue = c.call();
            throw new AssertionError("Expected an exception of type " + expectedExceptionClass.getSimpleName()
                    + " to be thrown, but the method returned " + returnValue);
        } catch (Throwable t) {
            checkException(expectedExceptionClass, t);
        }
    }

    /**
     * Assert that the lambda function throws an exception of the expected class.
     *
     * @param expectedExceptionClass expected exception class
     * @param c lambda function
     */
    protected void assertThrows(Class<?> expectedExceptionClass, VoidCallable c) {
        try {
            c.call();
            throw new AssertionError("Expected an exception of type " + expectedExceptionClass.getSimpleName()
                    + " to be thrown, but the method returned successfully");
        } catch (Throwable t) {
            checkException(expectedExceptionClass, t);
        }
    }

    /**
     * Assert that the lambda function throws a SQLException or DbException with the
     * expected error code.
     *
     * @param expectedErrorCode SQL error code
     * @param c lambda function
     */
    protected void assertThrows(int expectedErrorCode, Callable<?> c) {
        try {
            Object returnValue = c.call();
            throw new AssertionError("Expected an SQLException or DbException with error code " + expectedErrorCode
                    + " to be thrown, but the method returned " + returnValue);
        } catch (Throwable t) {
            checkErrorCode(expectedErrorCode, t);
        }
    }

    /**
     * Assert that the lambda function throws a SQLException or DbException with the
     * expected error code.
     *
     * @param expectedErrorCode SQL error code
     * @param c lambda function
     */
    protected void assertThrows(int expectedErrorCode, VoidCallable c) {
        try {
            c.call();
            throw new AssertionError("Expected an SQLException or DbException with error code " + expectedErrorCode
                    + " to be thrown, but the method returned successfully");
        } catch (Throwable t) {
            checkErrorCode(expectedErrorCode, t);
        }
    }

    private static void checkException(Class<?> expectedExceptionClass, Throwable t) throws AssertionError {
        if (!expectedExceptionClass.isAssignableFrom(t.getClass())) {
            AssertionError ae = new AssertionError("Expected an exception of type\n"
                    + expectedExceptionClass.getSimpleName() + " to be thrown, but an exception of type\n"
                    + t.getClass().getSimpleName() + " was thrown");
            ae.initCause(t);
            throw ae;
        }
    }

    /**
     * Verify that actual error code is the one expected
     * @param expectedErrorCode to compare against
     * @param t actual exception to extract error code from
     * @throws AssertionError if code is unexpected
     */
    public static void checkErrorCode(int expectedErrorCode, Throwable t) throws AssertionError {
        int errorCode;
        if (t instanceof DbException) {
            errorCode = ((DbException) t).getErrorCode();
        } else if (t instanceof SQLException) {
            errorCode = ((SQLException) t).getErrorCode();
        } else if (t instanceof MVStoreException) {
            errorCode = ((MVStoreException) t).getErrorCode();
        } else {
            errorCode = 0;
        }
        if (errorCode != expectedErrorCode) {
            AssertionError ae = new AssertionError("Expected an SQLException or DbException with error code "
                    + expectedErrorCode + ", but got a "
                    + (t == null ? "null" : t.getClass().getName() + " exception " + " with error code " + errorCode));
            ae.initCause(t);
            throw ae;
        }
    }

    /**
     * Construct a stream of 20 KB that fails while reading with the provided
     * exception.
     *
     * @param e the exception
     * @return the stream
     */
    public static ByteArrayInputStream createFailingStream(final Exception e) {
        return new ByteArrayInputStream(new byte[20 * 1024]) {
            @Override
            public int read(byte[] buffer, int off, int len) {
                if (this.pos > 10 * 1024) {
                    throwException(e);
                }
                return super.read(buffer, off, len);
            }
        };
    }

    /**
     * Throw a checked exception, without having to declare the method as
     * throwing a checked exception.
     *
     * @param e the exception to throw
     */
    public static void throwException(Throwable e) {
        TestBase.throwThis(e);
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void throwThis(Throwable e) throws E {
        throw (E) e;
    }

    /**
     * Get the name of the test.
     *
     * @return the name of the test class
     */
    public String getTestName() {
        return getClass().getSimpleName();
    }
}
