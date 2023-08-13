/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.scripts;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.h2.api.ErrorCode;
import org.h2.command.CommandContainer;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.command.dml.ScriptCommand;
import org.h2.command.query.Query;
import org.h2.engine.Mode.ModeEnum;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.test.TestAll;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.util.StringUtils;
import org.h2.value.DataType;

/**
 * This test runs a SQL script file and compares the output with the expected
 * output.
 */
public class TestScript extends TestDb {

    private static final String BASE_DIR = "org/h2/test/scripts/";

    private static final boolean FIX_OUTPUT = false;

    private static final Field COMMAND;

    private static final Field PREPARED;

    private static boolean CHECK_ORDERING;

    /** If set to true, the test will exit at the first failure. */
    private boolean failFast;
    /** If set to a value the test will add all executed statements to this list */
    private ArrayList<String> statements;

    private boolean reconnectOften;
    private Connection conn;
    private Statement stat;
    private String fileName;
    private LineNumberReader in;
    private PrintStream out;
    private final ArrayList<String[]> result = new ArrayList<>();
    private final ArrayDeque<String> putBack = new ArrayDeque<>();
    private boolean foundErrors;

    private Random random = new Random(1);

    static {
        try {
            COMMAND = JdbcPreparedStatement.class.getDeclaredField("command");
            COMMAND.setAccessible(true);
            PREPARED = CommandContainer.class.getDeclaredField("prepared");
            PREPARED.setAccessible(true);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        CHECK_ORDERING = true;
        TestBase.createCaller().init().testFromMain();
    }

    /**
     * Get all SQL statements of this file.
     *
     * @param conf the configuration
     * @return the list of statements
     */
    public ArrayList<String> getAllStatements(TestAll conf) throws Exception {
        config = conf;
        ArrayList<String> result = new ArrayList<>(4000);
        try {
            statements = result;
            test();
        } finally {
            this.statements = null;
        }
        return result;
    }

    @Override
    public boolean isEnabled() {
        if (config.networked && config.big) {
            return false;
        }
        return true;
    }

    @Override
    public void test() throws Exception {
        reconnectOften = !config.memory && config.big;

        testScript("testScript.sql");
        if (!config.memory && !config.big && !config.networked) {
            testScript("testSimple.sql");
        }
        testScript("dual.sql");
        testScript("indexes.sql");
        testScript("information_schema.sql");
        testScript("range_table.sql");
        testScript("altertable-index-reuse.sql");
        testScript("altertable-fk.sql");
        testScript("default-and-on_update.sql");

        for (String s : new String[] { "add_months", "compatibility", "group_by", "strict_and_legacy"}) {
            testScript("compatibility/" + s + ".sql");
        }
        for (String s : new String[] { "array", "bigint", "binary", "blob",
                "boolean", "char", "clob", "date", "decfloat", "double_precision", "enum",
                "geometry", "identity", "int", "interval", "java_object", "json", "numeric", "real", "row", "smallint",
                "time-with-time-zone", "time", "timestamp-with-time-zone", "timestamp", "tinyint",
                "uuid", "varbinary", "varchar", "varchar-ignorecase" }) {
            testScript("datatypes/" + s + ".sql");
        }
        for (String s : new String[] { "alterDomain", "alterTableAdd", "alterTableAlterColumn", "alterTableDropColumn",
                "alterTableDropConstraint",
                "alterTableRename", "alterTableRenameConstraint",
                "analyze", "commentOn", "createAlias", "createConstant", "createDomain",
                "createIndex", "createSchema", "createSequence", "createSynonym",
                "createTable", "createTrigger", "createView", "dropAllObjects", "dropDomain", "dropIndex",
                "dropSchema", "dropTable", "grant", "truncateTable" }) {
            testScript("ddl/" + s + ".sql");
        }
        for (String s : new String[] { "delete", "error_reporting", "execute_immediate", "insert", "insertIgnore",
                "merge", "mergeUsing", "replace", "script", "show", "update", "with" }) {
            testScript("dml/" + s + ".sql");
        }
        for (String s : new String[] { "any_value", "any", "array_agg", "avg",
                "bit_and_agg", "bit_or_agg", "bit_xor_agg",
                "corr",
                "count",
                "covar_pop", "covar_samp",
                "envelope", "every", "histogram",
                "json_arrayagg", "json_objectagg",
                "listagg", "max", "min", "mode", "percentile", "rank",
                "regr_avgx", "regr_avgy", "regr_count", "regr_intercept", "regr_r2", "regr_slope",
                "regr_sxx", "regr_sxy", "regr_syy",
                "stddev_pop", "stddev_samp", "sum", "var_pop", "var_samp" }) {
            testScript("functions/aggregate/" + s + ".sql");
        }
        for (String s : new String[] { "json_array", "json_object" }) {
            testScript("functions/json/" + s + ".sql");
        }
        for (String s : new String[] { "abs", "acos", "asin", "atan", "atan2",
                "bitand", "bitcount", "bitget", "bitnot", "bitor", "bitxor", "ceil", "compress",
                "cos", "cosh", "cot", "decrypt", "degrees", "encrypt", "exp",
                "expand", "floor", "hash", "length", "log", "lshift", "mod", "ora-hash", "pi",
                "power", "radians", "rand", "random-uuid", "rotate", "round",
                "roundmagic", "rshift", "secure-rand", "sign", "sin", "sinh", "sqrt",
                "tan", "tanh", "truncate", "zero" }) {
            testScript("functions/numeric/" + s + ".sql");
        }
        for (String s : new String[] { "array-to-string",
                "ascii", "bit-length", "btrim", "char", "concat",
                "concat-ws", "difference", "hextoraw", "insert",
                "left", "length", "locate", "lower", "lpad", "ltrim",
                "octet-length", "quote_ident", "rawtohex", "regexp-like",
                "regex-replace", "regexp-substr", "repeat", "replace", "right", "rpad", "rtrim",
                "soundex", "space", "stringdecode", "stringencode",
                "stringtoutf8", "substring", "to-char", "translate", "trim",
                "upper", "utf8tostring", "xmlattr", "xmlcdata", "xmlcomment",
                "xmlnode", "xmlstartdoc", "xmltext" }) {
            testScript("functions/string/" + s + ".sql");
        }
        for (String s : new String[] { "array-cat", "array-contains", "array-get",
                "array-slice", "autocommit", "cancel-session", "casewhen",
                "cardinality", "cast", "coalesce", "convert", "csvread", "csvwrite", "current_catalog",
                "current_schema", "current_user", "currval", "data_type_sql",
                "database-path", "db_object", "decode", "disk-space-used",
                "file-read", "file-write", "greatest", "h2version", "identity",
                "ifnull", "last-insert-id", "least", "link-schema", "lock-mode", "lock-timeout",
                "memory-free", "memory-used", "nextval", "nullif", "nvl2",
                "readonly", "rownum", "session-id",
                "table", "transaction-id", "trim_array", "truncate-value", "unnest" }) {
            testScript("functions/system/" + s + ".sql");
        }
        for (String s : new String[] { "current_date", "current_timestamp",
                "current-time", "dateadd", "datediff", "dayname",
                "day-of-month", "day-of-week", "day-of-year", "extract",
                "formatdatetime", "hour", "last_day", "minute", "month", "monthname",
                "parsedatetime", "quarter", "second", "truncate", "week", "year", "date_trunc" }) {
            testScript("functions/timeanddate/" + s + ".sql");
        }
        for (String s : new String[] { "lead", "nth_value", "ntile", "ratio_to_report", "row_number" }) {
            testScript("functions/window/" + s + ".sql");
        }
        for (String s : new String[] { "at-time-zone", "boolean-test", "case", "concatenation", "conditions",
                "data-change-delta-table", "field-reference", "help", "sequence", "set" }) {
            testScript("other/" + s + ".sql");
        }
        for (String s : new String[] { "comments", "identifiers" }) {
            testScript("parser/" + s + ".sql");
        }
        for (String s : new String[] { "between", "distinct", "in", "like", "null", "quantified-comparison-with-array",
                "type", "unique" }) {
            testScript("predicates/" + s + ".sql");
        }
        for (String s : new String[] { "derived-column-names", "distinct", "joins", "query-optimisations", "select",
                "table", "values", "window" }) {
            testScript("queries/" + s + ".sql");
        }
        testScript("other/two_phase_commit.sql");
        testScript("other/unique_include.sql");

        deleteDb("script");
        System.out.flush();
        if (foundErrors) {
            throw new Exception("errors in script found");
        }
    }

    private void testScript(String scriptFileName) throws Exception {
        deleteDb("script");

        // Reset all the state in case there is anything left over from the previous file
        // we processed.
        conn = null;
        stat = null;
        fileName = null;
        in = null;
        out = null;
        result.clear();
        putBack.clear();

        String outFile;
        if (FIX_OUTPUT) {
            outFile = scriptFileName;
            int idx = outFile.lastIndexOf('/');
            if (idx >= 0) {
                outFile = outFile.substring(idx + 1);
            }
        } else {
            outFile = "test.out.txt";
        }
        conn = getConnection("script");
        stat = conn.createStatement();
        out = new PrintStream(new FileOutputStream(outFile));
        testFile(BASE_DIR + scriptFileName);
        conn.close();
        out.close();
        if (FIX_OUTPUT) {
            File file = new File(outFile);
            // If there are two trailing newline characters remove one
            try (RandomAccessFile r = new RandomAccessFile(file, "rw")) {
                byte[] separator = System.lineSeparator().getBytes(StandardCharsets.ISO_8859_1);
                int separatorLength = separator.length;
                long length = r.length() - (separatorLength * 2);
                truncate: if (length >= 0) {
                    r.seek(length);
                    for (int i = 0; i < 2; i++) {
                        for (int j = 0; j < separatorLength; j++) {
                            if (r.readByte() != separator[j]) {
                                break truncate;
                            }
                        }
                    }
                    r.setLength(length + separatorLength);
                }
            }
            file.renameTo(new File("h2/src/test/org/h2/test/scripts/" + scriptFileName));
            return;
        }
    }

    private String readLine() throws IOException {
        String s = putBack.pollFirst();
        return s != null ? s : readNextLine();
    }

    private String readNextLine() throws IOException {
        String s;
        boolean comment = false;
        while ((s = in.readLine()) != null) {
            if (s.startsWith("--")) {
                write(s);
                comment = true;
                continue;
            }
            if (!FIX_OUTPUT) {
                s = s.trim();
            }
            if (!s.isEmpty()) {
                break;
            }
            if (comment) {
                write("");
                comment = false;
            }
        }
        return s;
    }

    private void putBack(String line) {
        putBack.addLast(line);
    }

    private void testFile(String inFile) throws Exception {
        InputStream is = getClass().getClassLoader().getResourceAsStream(inFile);
        if (is == null) {
            throw new IOException("could not find " + inFile);
        }
        fileName = inFile;
        in = new LineNumberReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        StringBuilder buff = new StringBuilder();
        boolean allowReconnect = true;
        for (String sql; (sql = readLine()) != null;) {
            if (sql.startsWith("--")) {
                write(sql);
            } else if (sql.startsWith(">")) {
                addWriteResultError("<command>", sql);
            } else if (sql.endsWith(";")) {
                write(sql);
                buff.append(sql, 0, sql.length() - 1);
                sql = buff.toString();
                buff.setLength(0);
                process(sql, allowReconnect);
            } else if (sql.startsWith("@")) {
                if (buff.length() > 0) {
                    addWriteResultError("<command>", sql);
                } else {
                    switch (sql) {
                    case "@reconnect":
                        write(sql);
                        write("");
                        if (!config.memory) {
                            reconnect(conn.getAutoCommit());
                        }
                        break;
                    case "@reconnect on":
                        write(sql);
                        write("");
                        allowReconnect = true;
                        break;
                    case "@reconnect off":
                        write(sql);
                        write("");
                        allowReconnect = false;
                        break;
                    case "@autocommit on":
                        conn.setAutoCommit(true);
                        break;
                    case "@autocommit off":
                        conn.setAutoCommit(false);
                        break;
                    default:
                        addWriteResultError("<command>", sql);
                    }
                }
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

    private void process(String sql, boolean allowReconnect) throws Exception {
        if (allowReconnect && reconnectOften) {
            if (!containsTempTables()
                    && ((JdbcConnection) conn).getMode().getEnum() == ModeEnum.REGULAR
                    && conn.getSchema().equals("PUBLIC")) {
                boolean autocommit = conn.getAutoCommit();
                if (autocommit && random.nextInt(10) < 1) {
                    // reconnect 10% of the time
                    reconnect(autocommit);
                }
            }
        }
        if (statements != null) {
            statements.add(sql);
        }
        if (!hasParameters(sql)) {
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

    private static boolean hasParameters(String sql) {
        int index = 0;
        for (;;) {
            index = sql.indexOf('?', index);
            if (index < 0) {
                return false;
            }
            int length = sql.length();
            if (++index == length || sql.charAt(index) != '?') {
                return true;
            }
            index++;
        }
    }

    private void reconnect(boolean autocommit) throws SQLException {
        conn.close();
        conn = getConnection("script");
        conn.setAutoCommit(autocommit);
        stat = conn.createStatement();
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
                    buff.setLength(0);
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
            boolean res;
            Statement s;
            if (/* TestScript */ CHECK_ORDERING || /* TestAll */ config.memory && !config.lazy && !config.networked) {
                PreparedStatement prep = conn.prepareStatement(sql);
                res = prep.execute();
                s = prep;
            } else {
                res = stat.execute(sql);
                s = stat;
            }
            if (res) {
                writeResultSet(sql, s.getResultSet());
            } else {
                int count = s.getUpdateCount();
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

    private static String formatBinary(byte[] b) {
        if (b == null) {
            return "null";
        }
        return StringUtils.convertBytesToHex(new StringBuilder("X'"), b).append('\'').toString();
    }

    private void writeResultSet(String sql, ResultSet rs) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        int len = meta.getColumnCount();
        int[] max = new int[len];
        result.clear();
        while (rs.next()) {
            String[] row = new String[len];
            for (int i = 0; i < len; i++) {
                String data = readValue(rs, meta, i + 1);
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
        Boolean gotOrdered = null;
        Statement st = rs.getStatement();
        if (st instanceof JdbcPreparedStatement) {
            CommandInterface ci = (CommandInterface) COMMAND.get(st);
            if (ci instanceof CommandContainer) {
                Prepared p = (Prepared) PREPARED.get(ci);
                if (p instanceof Query) {
                    gotOrdered = ((Query) p).hasOrder();
                } else if (p instanceof ScriptCommand) {
                    gotOrdered = true;
                }
            }
        }
        rs.close();
        String line = readLine();
        putBack(line);
        if (line != null && line.startsWith(">> ")) {
            switch (result.size()) {
            case 0:
                writeResult(sql, "<no result>", null, ">> ");
                return;
            case 1:
                String[] row = result.get(0);
                if (row.length == 1) {
                    writeResult(sql, row[0], null, ">> ");
                } else {
                    writeResult(sql, "<row with " + row.length + " values>", null, ">> ");
                }
                return;
            default:
                writeResult(sql, "<" + result.size() + " rows>", null, ">> ");
                return;
            }
        }
        Boolean ordered;
        for (;;) {
            line = readNextLine();
            if (line == null) {
                addWriteResultError("<row count>", "<eof>");
                return;
            }
            putBack(line);
            if (line.startsWith("> rows: ")) {
                ordered = false;
                break;
            } else if (line.startsWith("> rows (ordered): ")) {
                ordered = true;
                break;
            } else if (line.startsWith("> rows (partially ordered): ")) {
                ordered = null;
                break;
            }
        }
        if (gotOrdered != null) {
            if (ordered == null || ordered) {
                if (!gotOrdered) {
                    addWriteResultError("<ordered result set>", "<result set>");
                }
            } else {
                if (gotOrdered) {
                    addWriteResultError("<result set>", "<ordered result set>");
                }
            }
        }
        writeResult(sql, format(head, max), null);
        writeResult(sql, format(null, max), null);
        String[] array = new String[result.size()];
        for (int i = 0; i < result.size(); i++) {
            array[i] = format(result.get(i), max);
        }
        if (!Boolean.TRUE.equals(ordered)) {
            sort(array);
        }
        int i = 0;
        for (; i < array.length; i++) {
            writeResult(sql, array[i], null);
        }
        writeResult(sql,
                (ordered != null ? ordered ? "rows (ordered): " : "rows: " : "rows (partially ordered): ") + i,
                null);
    }

    private static String readValue(ResultSet rs, ResultSetMetaData meta, int column) throws SQLException {
        return DataType.isBinaryColumn(meta, column) ? formatBinary(rs.getBytes(column))
                : formatString(rs.getString(column));
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

    /** Convert the error code to a symbolic name from ErrorCode. */
    private static final Map<Integer, String> ERROR_CODE_TO_NAME = new HashMap<>(256);
    static {
        try {
            for (Field field : ErrorCode.class.getDeclaredFields()) {
                if (field.getModifiers() == (Modifier.PUBLIC | Modifier.STATIC | Modifier.FINAL)
                        && field.getAnnotation(Deprecated.class) == null) {
                    ERROR_CODE_TO_NAME.put(field.getInt(null), field.getName());
                }
            }
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void writeException(String sql, SQLException ex) throws Exception {
        writeResult(sql, "exception " + ERROR_CODE_TO_NAME.get(ex.getErrorCode()), ex);
    }

    private void writeResult(String sql, String s, SQLException ex) throws Exception {
        writeResult(sql, s, ex, "> ");
    }

    private void writeResult(String sql, String s, SQLException ex, String prefix) throws Exception {
        assertKnownException(sql, ex);
        s = (prefix + s).trim();
        String compare = readLine();
        if (compare != null && compare.startsWith(">")) {
            if (!compare.equals(s)) {
                if (reconnectOften && sql.toUpperCase().startsWith("EXPLAIN")) {
                    return;
                }
                addWriteResultError(compare, s);
                if (ex != null) {
                    TestBase.logError("script", ex);
                }
                if (failFast) {
                    conn.close();
                    System.exit(1);
                }
            }
        } else {
            addWriteResultError("<nothing>", s);
            if (compare != null) {
                putBack(compare);
            }
        }
        write(s);
    }

    private void addWriteResultError(String expected, String got) {
        foundErrors = true;
        final String msg = fileName + '\n' + //
                "line: " + in.getLineNumber() + '\n' + //
                "exp: " + expected + '\n' + //
                "got: " + got + '\n';
        TestBase.logErrorMessage(msg);
    }

    private void write(String s) {
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
