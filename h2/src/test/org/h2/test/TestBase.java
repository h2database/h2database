/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (license2)
 * Initial Developer: H2 Group
 */
package org.h2.test;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Properties;

import org.h2.jdbc.JdbcConnection;
import org.h2.message.TraceSystem;
import org.h2.store.FileLock;
import org.h2.tools.DeleteDbFiles;

/**
 * The base class for all tests.
 */
public abstract class TestBase {

    // private static final String BASE_TEST_DIR = System.getProperty("java.io.tmpdir") + "/h2";
    private static final String BASE_TEST_DIR = "data";
    
    public static String getTestDir(String name) {
        return BASE_TEST_DIR + "/test-" + name;
    }
    
    protected static String baseDir = BASE_TEST_DIR + "/test";

    protected TestAll config;
    private long start;

    protected void startServerIfRequired() throws SQLException {
        config.beforeTest();
    }

    public TestBase init(TestAll conf) throws Exception {
        this.config = conf;
        return this;
    }

    public void testCase(int i) throws Exception {
        // do nothing
    }

    public void runTest(TestAll conf) {
        try {
            init(conf);
            start = System.currentTimeMillis();
            test();
            println("");
        } catch (Exception e) {
            fail("FAIL " + e.toString(), e);
            if (config.stopOnError) {
                throw new Error("ERROR");
            }
        }
    }

    public Connection getConnection(String name) throws Exception {
        return getConnectionInternal(getURL(name, true), getUser(), getPassword());
    }

    protected Connection getConnection(String name, String user, String password) throws Exception {
        return getConnectionInternal(getURL(name, false), user, password);
    }

    protected String getPassword() {
        return "123";
    }

    private void deleteIndexFiles(String name) throws SQLException {
        if (name.indexOf(";") > 0) {
            name = name.substring(0, name.indexOf(';'));
        }
        name += ".index.db";
        if (new File(name).canWrite()) {
            new File(name).delete();
        }
    }

    protected String getURL(String name, boolean admin) throws SQLException {
        String url;
        if (name.startsWith("jdbc:")) {
            return name;
        }
        if (config.memory) {
            url = "mem:" + name;
        } else {
            if (!name.startsWith("memFS:") && !name.startsWith(baseDir + "/")) {
                name = baseDir + "/" + name;
            }
            if (config.deleteIndex) {
                deleteIndexFiles(name);
            }
            if (config.networked) {
                if (config.ssl) {
                    url = "ssl://localhost:9192/" + name;
                } else {
                    url = "tcp://localhost:9192/" + name;
                }
            } else {
                url = name;
            }
            if (config.traceSystemOut) {
                url += ";TRACE_LEVEL_SYSTEM_OUT=2";
            }
            if (config.traceLevelFile > 0 && admin) {
                url += ";TRACE_LEVEL_FILE=" + config.traceLevelFile;
            }
        }
        if (config.throttle > 0) {
            url += ";THROTTLE=" + config.throttle;
        }
        if (config.textStorage) {
            url += ";STORAGE=TEXT";
        }
        url += ";LOCK_TIMEOUT=50";
        if (admin) {
            url += ";LOG=" + config.logMode;
        }
        if (config.smallLog && admin) {
            url += ";MAX_LOG_SIZE=1";
        }
        if (config.diskUndo && admin) {
            url += ";MAX_MEMORY_UNDO=3";
        }
        if (config.big && admin) {
            // force operations to disk
            url += ";MAX_OPERATION_MEMORY=1";
        }
        if (config.mvcc) {
            url += ";MVCC=TRUE";
        }
        if (config.cache2Q) {
            url += ";CACHE_TYPE=TQ";
        }
        if (config.diskResult && admin) {
            url += ";MAX_MEMORY_ROWS=100;CACHE_SIZE=0";
        }
        return "jdbc:h2:" + url;
    }

    private Connection getConnectionInternal(String url, String user, String password) throws Exception {
        Class.forName("org.h2.Driver");
        // url += ";DEFAULT_TABLE_TYPE=1";
        // Class.forName("org.hsqldb.jdbcDriver");
        // return DriverManager.getConnection("jdbc:hsqldb:" + name, "sa", "");
        Connection conn;
        if (config.cipher != null) {
            url += ";cipher=" + config.cipher;
            password = "filePassword " + password;
            Properties info = new Properties();
            info.setProperty("user", user);
            info.setProperty("password", password);
            // a bug in the PostgreSQL driver: throws a NullPointerException if we do this
            // info.put("password", password.toCharArray());
            conn = DriverManager.getConnection(url, info);
        } else {
            conn = DriverManager.getConnection(url, user, password);
        }
        return conn;
    }

    protected int getSize(int small, int big) throws Exception {
        return config.endless ? Integer.MAX_VALUE : config.big ? big : small;
    }

    protected String getUser() {
        return "sa";
    }

    protected void trace(int x) {
        trace("" + x);
    }

    public void trace(String s) {
        if (config.traceTest) {
            println(s);
        }
    }

    protected void traceMemory() {
        if (config.traceTest) {
            trace("mem=" + getMemoryUsed());
        }
    }

    public void printTimeMemory(String s, long time) {
        if (config.big) {
            println(getMemoryUsed() + " MB: " + s);
        }
    }

    public static int getMemoryUsed() {
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
        int mb = (int) (memory / 1024 / 1024);
        return mb;
    }

    protected void error() throws Exception {
        error("Unexpected success");
    }

    protected void error(String string) throws Exception {
        println(string);
        throw new Exception(string);
    }

    protected void fail(String s, Throwable e) {
        println(s);
        logError(s, e);
    }

    public static void logError(String s, Throwable e) {
        if (e == null) {
            e = new Exception(s);
        }
        System.out.println("ERROR: " + s + " " + e.toString() + " ------------------------------");
        e.printStackTrace();
        try {
            TraceSystem ts = new TraceSystem(null, false);
            FileLock lock = new FileLock(ts, 1000);
            lock.lock("error.lock", false);
            FileWriter fw = new FileWriter("ERROR.txt", true);
            PrintWriter pw = new PrintWriter(fw);
            e.printStackTrace(pw);
            pw.close();
            fw.close();
            lock.unlock();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    protected void println(String s) {
        long time = System.currentTimeMillis() - start;
        printlnWithTime(time, getClass().getName() + " " + s);
    }

    static void printlnWithTime(long time, String s) {
        String t = "0000000000" + time;
        t = t.substring(t.length() - 6);
        System.out.println(t + " " + s);
    }

    protected void printTime(String s) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        println(dateFormat.format(new java.util.Date()) + " " + s);
    }

    protected void deleteDb(String name) throws Exception {
        DeleteDbFiles.execute(baseDir, name, true);
    }

    protected void deleteDb(String dir, String name) throws Exception {
        DeleteDbFiles.execute(dir, name, true);
    }

    public abstract void test() throws Exception;

    public void check(int a, int b) throws Exception {
        if (a != b) {
            error("int a: " + a + " b: " + b);
        }
    }

    protected void check(byte[] a, byte[] b) throws Exception {
        check(a.length == b.length);
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                error("byte[" + i + "]: a=" + (int) a[i] + " b=" + (int) b[i]);
            }
        }
    }

    protected void check(String a, String b) throws Exception {
        if (a == null && b == null) {
            return;
        } else if (a == null || b == null) {
            error("string a: " + a + " b: " + b);
        }
        if (!a.equals(b)) {
            for (int i = 0; i < a.length(); i++) {
                String s = a.substring(0, i);
                if (!b.startsWith(s)) {
                    a = a.substring(0, i) + "<*>" + a.substring(i);
                    break;
                }
            }
            error("string a: " + a + " (" + a.length() + ") b: " + b + " (" + b.length() + ")");
        }
    }

    protected void checkFalse(String a, String b) throws Exception {
        if (a.equals(b)) {
            error("string false a: " + a + " b: " + b);
        }
    }

    protected void check(long a, long b) throws Exception {
        if (a != b) {
            error("long a: " + a + " b: " + b);
        }
    }

    protected void checkSmaller(long a, long b) throws Exception {
        if (a >= b) {
            error("a: " + a + " is not smaller than b: " + b);
        }
    }

    protected void checkContains(String result, String contains) throws Exception {
        if (result.indexOf(contains) < 0) {
            error(result + " does not contain: " + contains);
        }
    }

    protected void check(double a, double b) throws Exception {
        if (a != b) {
            error("double a: " + a + " b: " + b);
        }
    }

    protected void check(float a, float b) throws Exception {
        if (a != b) {
            error("float a: " + a + " b: " + b);
        }
    }

    protected void check(boolean a, boolean b) throws Exception {
        if (a != b) {
            error("boolean a: " + a + " b: " + b);
        }
    }

    protected void check(boolean value) throws Exception {
        if (!value) {
            error("expected: true got: false");
        }
    }

    protected void checkFalse(boolean value) throws Exception {
        if (value) {
            error("expected: false got: true");
        }
    }

    protected void checkResultRowCount(ResultSet rs, int expected) throws Exception {
        int i = 0;
        while (rs.next()) {
            i++;
        }
        check(i, expected);
    }

    protected void checkSingleValue(Statement stat, String sql, int value) throws Exception {
        ResultSet rs = stat.executeQuery(sql);
        check(rs.next());
        check(rs.getInt(1), value);
        checkFalse(rs.next());
    }

    protected void testResultSetMeta(ResultSet rs, int columnCount, String[] labels, int[] datatypes, int[] precision,
            int[] scale) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        int cc = meta.getColumnCount();
        if (cc != columnCount) {
            error("result set contains " + cc + " columns not " + columnCount);
        }
        for (int i = 0; i < columnCount; i++) {
            if (labels != null) {
                String l = meta.getColumnLabel(i + 1);
                if (!labels[i].equals(l)) {
                    error("column label " + i + " is " + l + " not " + labels[i]);
                }
            }
            if (datatypes != null) {
                int t = meta.getColumnType(i + 1);
                if (datatypes[i] != t) {
                    error("column datatype " + i + " is " + t + " not " + datatypes[i] + " (prec="
                            + meta.getPrecision(i + 1) + " scale=" + meta.getScale(i + 1) + ")");
                }
                String typeName = meta.getColumnTypeName(i + 1);
                String className = meta.getColumnClassName(i + 1);
                switch (t) {
                case Types.INTEGER:
                    check(typeName, "INTEGER");
                    check(className, "java.lang.Integer");
                    break;
                case Types.VARCHAR:
                    check(typeName, "VARCHAR");
                    check(className, "java.lang.String");
                    break;
                case Types.SMALLINT:
                    check(typeName, "SMALLINT");
                    check(className, "java.lang.Short");
                    break;
                case Types.TIMESTAMP:
                    check(typeName, "TIMESTAMP");
                    check(className, "java.sql.Timestamp");
                    break;
                case Types.DECIMAL:
                    check(typeName, "DECIMAL");
                    check(className, "java.math.BigDecimal");
                    break;
                default:
                }
            }
            if (precision != null) {
                int p = meta.getPrecision(i + 1);
                if (precision[i] != p) {
                    error("column precision " + i + " is " + p + " not " + precision[i]);
                }
            }
            if (scale != null) {
                int s = meta.getScale(i + 1);
                if (scale[i] != s) {
                    error("column scale " + i + " is " + s + " not " + scale[i]);
                }
            }

        }
    }

    protected void testResultSetOrdered(ResultSet rs, String[][] data) throws Exception {
        testResultSet(true, rs, data);
    }

    void testResultSetUnordered(ResultSet rs, String[][] data) throws Exception {
        testResultSet(false, rs, data);
    }

    void testResultSet(boolean ordered, ResultSet rs, String[][] data) throws Exception {
        int len = rs.getMetaData().getColumnCount();
        int rows = data.length;
        if (rows == 0) {
            // special case: no rows
            if (rs.next()) {
                error("testResultSet expected rowCount:" + rows + " got:0");
            }
        }
        int len2 = data[0].length;
        if (len < len2) {
            error("testResultSet expected columnCount:" + len2 + " got:" + len);
        }
        for (int i = 0; i < rows; i++) {
            if (!rs.next()) {
                error("testResultSet expected rowCount:" + rows + " got:" + i);
            }
            String[] row = getData(rs, len);
            if (ordered) {
                String[] good = data[i];
                if (!testRow(good, row, good.length)) {
                    error("testResultSet row not equal, got:\n" + formatRow(row) + "\n" + formatRow(good));
                }
            } else {
                boolean found = false;
                for (int j = 0; j < rows; j++) {
                    String[] good = data[i];
                    if (testRow(good, row, good.length)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    error("testResultSet no match for row:" + formatRow(row));
                }
            }
        }
        if (rs.next()) {
            String[] row = getData(rs, len);
            error("testResultSet expected rowcount:" + rows + " got:>=" + (rows + 1) + " data:" + formatRow(row));
        }
    }

    boolean testRow(String[] a, String[] b, int len) {
        for (int i = 0; i < len; i++) {
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

    String[] getData(ResultSet rs, int len) throws SQLException {
        String[] data = new String[len];
        for (int i = 0; i < len; i++) {
            data[i] = rs.getString(i + 1);
            // just check if it works
            rs.getObject(i + 1);
        }
        return data;
    }

    String formatRow(String[] row) {
        String sb = "";
        for (int i = 0; i < row.length; i++) {
            sb += "{" + row[i] + "}";
        }
        return "{" + sb + "}";
    }

    protected void crash(Connection conn) throws Exception {
        ((JdbcConnection) conn).setPowerOffCount(1);
        try {
            conn.createStatement().execute("SET WRITE_DELAY 0");
            conn.createStatement().execute("CREATE TABLE TEST_A(ID INT)");
            error("should be crashed already");
        } catch (SQLException e) {
            // expected
        }
        try {
            conn.close();
        } catch (SQLException e) {
            // ignore
        }
    }

    protected String readString(Reader reader) throws Exception {
        if (reader == null) {
            return null;
        }
        StringBuffer buffer = new StringBuffer();
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
            check(false);
            return null;
        }
    }

    protected void checkNotGeneralException(SQLException e) throws Exception {
        if (e != null && e.getSQLState().startsWith("HY000")) {
            TestBase.logError("Unexpected General error", e);
        }
    }

    protected void check(Integer a, Integer b) throws Exception {
        if (a == null || b == null) {
            check(a == b);
        } else {
            check(a.intValue(), b.intValue());
        }
    }

    protected void compareDatabases(Statement stat1, Statement stat2) throws Exception {
        ResultSet rs1 = stat1.executeQuery("SCRIPT NOPASSWORDS");
        ResultSet rs2 = stat2.executeQuery("SCRIPT NOPASSWORDS");
        ArrayList list1 = new ArrayList();
        ArrayList list2 = new ArrayList();
        while (rs1.next()) {
            check(rs2.next());
            String s1 = rs1.getString(1);
            list1.add(s1);
            String s2 = rs2.getString(1);
            list2.add(s2);
        }
        for (int i = 0; i < list1.size(); i++) {
            String s = (String) list1.get(i);
            if (!list2.remove(s)) {
                error("not found: " + s);
            }
        }
        check(list2.size(), 0);
        checkFalse(rs2.next());
    }

}
