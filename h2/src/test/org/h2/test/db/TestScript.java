/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

import org.h2.test.TestAll;
import org.h2.test.TestBase;
import org.h2.util.New;
import org.h2.util.StringUtils;

/**
 * This test runs a SQL script file and compares the output with the expected
 * output.
 */
public class TestScript extends TestBase {

    private static final String FILENAME = "org/h2/test/testScript.sql";

    private boolean failFast;

    private boolean reconnectOften;
    private Connection conn;
    private Statement stat;
    private LineNumberReader in;
    private int line;
    private PrintStream out;
    private final ArrayList<String[]> result = New.arrayList();
    private String putBack;
    private StringBuilder errors;
    private ArrayList<String> statements;

    private Random random = new Random(1);

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    /**
     * Get all SQL statements of this file.
     *
     * @param conf the configuration
     * @return the list of statements
     */
    public ArrayList<String> getAllStatements(TestAll conf) throws Exception {
        config = conf;
        statements = New.arrayList();
        test();
        return statements;
    }

    @Override
    public void test() throws Exception {
        if (config.networked && config.big) {
            return;
        }
        reconnectOften = false;
        if (!config.memory) {
            if (config.big) {
                reconnectOften = true;
            }
        }
        testScript();
        deleteDb("script");
    }

    private void testScript() throws Exception {
        deleteDb("script");
        String outFile = "test.out.txt";
        String inFile = FILENAME;
        conn = getConnection("script");
        stat = conn.createStatement();
        out = new PrintStream(new FileOutputStream(outFile));
        errors = new StringBuilder();
        testFile(inFile);
        conn.close();
        out.close();
        if (errors.length() > 0) {
            throw new Exception("errors:\n" + errors.toString());
        }
        // new File(outFile).delete();
    }

    private String readLine() throws IOException {
        if (putBack != null) {
            String s = putBack;
            putBack = null;
            return s;
        }
        while (true) {
            String s = in.readLine();
            if (s == null) {
                return s;
            }
            s = s.trim();
            if (s.length() > 0) {
                return s;
            }
        }
    }

    private void testFile(String inFile) throws Exception {
        InputStream is = getClass().getClassLoader().getResourceAsStream(inFile);
        in = new LineNumberReader(new InputStreamReader(is, "Cp1252"));
        StringBuilder buff = new StringBuilder();
        while (true) {
            String sql = readLine();
            if (sql == null) {
                break;
            }
            if (sql.startsWith("--")) {
                write(sql);
            } else if (sql.startsWith(">")) {
                // do nothing
            } else if (sql.endsWith(";")) {
                write(sql);
                buff.append(sql.substring(0, sql.length() - 1));
                sql = buff.toString();
                buff = new StringBuilder();
                process(sql);
            } else {
                write(sql);
                buff.append(sql);
                buff.append('\n');
            }
        }
    }

    private boolean containsTempTables() throws SQLException {
        ResultSet rs = conn.getMetaData().getTables(null, null, null,
                new String[] { "TABLE" });
        while (rs.next()) {
            String sql = rs.getString("SQL");
            if (sql != null) {
                if (sql.contains("TEMPORARY")) {
                    return true;
                }
            }
        }
        return false;
    }

    private void process(String sql) throws Exception {
        if (reconnectOften) {
            if (!containsTempTables()) {
                boolean autocommit = conn.getAutoCommit();
                if (autocommit && random.nextInt(10) < 1) {
                    // reconnect 10% of the time
                    conn.close();
                    conn = getConnection("script");
                    conn.setAutoCommit(autocommit);
                    stat = conn.createStatement();
                }
            }
        }
        if (statements != null) {
            statements.add(sql);
        }
        if (sql.indexOf('?') == -1) {
            processStatement(sql);
        } else {
            String param = readLine();
            write(param);
            if (!param.equals("{")) {
                throw new AssertionError("expected '{', got " + param + " in " + sql);
            }
            try {
                PreparedStatement prep = conn.prepareStatement(sql);
                int count = 0;
                while (true) {
                    param = readLine();
                    write(param);
                    if (param.startsWith("}")) {
                        break;
                    }
                    count += processPrepared(sql, prep, param);
                }
                writeResult(sql, "update count: " + count, null);
            } catch (SQLException e) {
                writeException(sql, e);
            }
        }
        write("");
    }

    private static void setParameter(PreparedStatement prep, int i, String param)
            throws SQLException {
        if (param.equalsIgnoreCase("null")) {
            param = null;
        }
        prep.setString(i, param);
    }

    private int processPrepared(String sql, PreparedStatement prep, String param)
            throws Exception {
        try {
            StringBuilder buff = new StringBuilder();
            int index = 0;
            for (int i = 0; i < param.length(); i++) {
                char c = param.charAt(i);
                if (c == ',') {
                    setParameter(prep, ++index, buff.toString());
                    buff = new StringBuilder();
                } else if (c == '"') {
                    while (true) {
                        c = param.charAt(++i);
                        if (c == '"') {
                            break;
                        }
                        buff.append(c);
                    }
                } else if (c > ' ') {
                    buff.append(c);
                }
            }
            if (buff.length() > 0) {
                setParameter(prep, ++index, buff.toString());
            }
            if (prep.execute()) {
                writeResultSet(sql, prep.getResultSet());
                return 0;
            }
            return prep.getUpdateCount();
        } catch (SQLException e) {
            writeException(sql, e);
            return 0;
        }
    }

    private int processStatement(String sql) throws Exception {
        try {
            if (stat.execute(sql)) {
                writeResultSet(sql, stat.getResultSet());
            } else {
                int count = stat.getUpdateCount();
                writeResult(sql, count < 1 ? "ok" : "update count: " + count, null);
            }
        } catch (SQLException e) {
            writeException(sql, e);
        }
        return 0;
    }

    private static String formatString(String s) {
        if (s == null) {
            return "null";
        }
        s = StringUtils.replaceAll(s, "\r\n", "\n");
        s = s.replace('\n', ' ');
        s = StringUtils.replaceAll(s, "    ", " ");
        while (true) {
            String s2 = StringUtils.replaceAll(s, "  ", " ");
            if (s2.length() == s.length()) {
                break;
            }
            s = s2;
        }
        return s;
    }

    private void writeResultSet(String sql, ResultSet rs) throws Exception {
        boolean ordered = StringUtils.toLowerEnglish(sql).contains("order by");
        ResultSetMetaData meta = rs.getMetaData();
        int len = meta.getColumnCount();
        int[] max = new int[len];
        result.clear();
        while (rs.next()) {
            String[] row = new String[len];
            for (int i = 0; i < len; i++) {
                String data = formatString(rs.getString(i + 1));
                if (max[i] < data.length()) {
                    max[i] = data.length();
                }
                row[i] = data;
            }
            result.add(row);
        }
        String[] head = new String[len];
        for (int i = 0; i < len; i++) {
            String label = formatString(meta.getColumnLabel(i + 1));
            if (max[i] < label.length()) {
                max[i] = label.length();
            }
            head[i] = label;
        }
        rs.close();
        writeResult(sql, format(head, max), null);
        writeResult(sql, format(null, max), null);
        String[] array = new String[result.size()];
        for (int i = 0; i < result.size(); i++) {
            array[i] = format(result.get(i), max);
        }
        if (!ordered) {
            sort(array);
        }
        int i = 0;
        for (; i < array.length; i++) {
            writeResult(sql, array[i], null);
        }
        writeResult(sql, (ordered ? "rows (ordered): " : "rows: ") + i, null);
    }

    private static String format(String[] row, int[] max) {
        int length = max.length;
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                buff.append(' ');
            }
            if (row == null) {
                for (int j = 0; j < max[i]; j++) {
                    buff.append('-');
                }
            } else {
                int len = row[i].length();
                buff.append(row[i]);
                if (i < length - 1) {
                    for (int j = len; j < max[i]; j++) {
                        buff.append(' ');
                    }
                }
            }
        }
        return buff.toString();
    }

    private void writeException(String sql, SQLException e) throws Exception {
        writeResult(sql, "exception", e);
    }

    private void writeResult(String sql, String s, SQLException e)
            throws Exception {
        assertKnownException(sql, e);
        s = ("> " + s).trim();
        String compare = readLine();
        if (compare != null && compare.startsWith(">")) {
            if (!compare.equals(s)) {
                if (reconnectOften && sql.toUpperCase().startsWith("EXPLAIN")) {
                    return;
                }
                errors.append("line: ");
                errors.append(line);
                errors.append("\n" + "exp: ");
                errors.append(compare);
                errors.append("\n" + "got: ");
                errors.append(s);
                errors.append("\n");
                if (e != null) {
                    TestBase.logError("script", e);
                }
                TestBase.logError(errors.toString(), null);
                if (failFast) {
                    conn.close();
                    System.exit(1);
                }
            }
        } else {
            putBack = compare;
        }
        write(s);
    }

    private void write(String s) {
        line++;
        out.println(s);
    }

    private static void sort(String[] a) {
        for (int i = 1, j, len = a.length; i < len; i++) {
            String t = a[i];
            for (j = i - 1; j >= 0 && t.compareTo(a[j]) < 0; j--) {
                a[j + 1] = a[j];
            }
            a[j + 1] = t;
        }
    }

}
