/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.util.ClassUtils;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.ScriptReader;
import org.h2.util.StringUtils;
import org.h2.util.Tool;

/**
 * Executes the contents of a SQL script file against a database.
 * This tool is usually used to create a database from script.
 * It can also be used to analyze performance problems by running
 * the tool using Java profiler settings such as:
 * <pre>
 * java -Xrunhprof:cpu=samples ...
 * </pre>
 */
public class RunScript extends Tool {

    private boolean showResults;
    private boolean checkResults;

    private void showUsage() {
        out.println("Runs a SQL script.");
        out.println("java "+getClass().getName() + "\n" +
                " -url <url>         The database URL\n" +
                " -user <user>       The user name\n" +
                " [-password <pwd>]  The password\n" +
                " [-script <file>]   The script file to run (default: backup.sql)\n" +
                " [-driver <class>]  The JDBC driver class to use (not required in most cases)\n" +
                " [-showResults]     Show the statements and the results of queries\n" +
                " [-checkResults]    Check if the query results match the expected results\n" +
                " [-options ...]     The list of options (only for H2 embedded mode)");
        out.println("See also http://h2database.com/javadoc/" + getClass().getName().replace('.', '/') + ".html");
    }

    /**
     * The command line interface for this tool. The options must be split into
     * strings like this: "-user", "sa",... Options are case sensitive. The
     * following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options) </li>
     * <li>-url jdbc:h2:... (database URL) </li>
     * <li>-user username </li>
     * <li>-password password </li>
     * <li>-script filename (default file name is backup.sql) </li>
     * <li>-driver driver (the JDBC driver class name; not required for most
     * databases) </li>
     * <li>-showResults (show the statements and the results of queries)</li>
     * <li>-checkResults (check if the query results match the expected results</li>
     * <li>-options (to specify a list of options ;only for H2 and only when
     * using the embedded mode) </li>
     * </ul>
     * To include local files when using remote databases, use the special
     * syntax:
     *
     * <pre>
     * &#064;INCLUDE fileName
     * </pre>
     *
     * This syntax is only supported by this tool. Embedded RUNSCRIPT SQL
     * statements will be executed by the database.
     *
     * @param args the command line arguments
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        new RunScript().run(args);
    }

    public void run(String[] args) throws SQLException {
        String url = null;
        String user = null;
        String password = "";
        String script = "backup.sql";
        String options = null;
        boolean continueOnError = false;
        boolean showTime = false;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-url")) {
                url = args[++i];
            } else if (arg.equals("-user")) {
                user = args[++i];
            } else if (arg.equals("-password")) {
                password = args[++i];
            } else if (arg.equals("-continueOnError")) {
                continueOnError = true;
            } else if (arg.equals("-checkResults")) {
                checkResults = true;
            } else if (arg.equals("-showResults")) {
                showResults = true;
            } else if (arg.equals("-script")) {
                script = args[++i];
            } else if (arg.equals("-time")) {
                showTime = true;
            } else if (arg.equals("-driver")) {
                String driver = args[++i];
                ClassUtils.loadUserClass(driver);
            } else if (arg.equals("-options")) {
                StringBuffer buff = new StringBuffer();
                i++;
                for (; i < args.length; i++) {
                    buff.append(' ');
                    buff.append(args[i]);
                }
                options = buff.toString();
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                out.println("Unsupported option: " + arg);
                showUsage();
                return;
            }
        }
        if (url == null || user == null || password == null || script == null) {
            showUsage();
            return;
        }
        long time = System.currentTimeMillis();
        if (options != null) {
            processRunscript(url, user, password, script, options);
        } else {
            process(url, user, password, script, null, continueOnError);
        }
        if (showTime) {
            time = System.currentTimeMillis() - time;
            out.println("Done in " + time + " ms");
        }
    }

    /**
     * Executes the SQL commands in a script file against a database.
     *
     * @param conn the connection to a database
     * @param reader the reader
     * @return the last result set
     */
    public static ResultSet execute(Connection conn, Reader reader) throws SQLException {
        return new RunScript().process(conn, reader);
    }

    private ResultSet process(Connection conn, Reader reader) throws SQLException {
        reader = new BufferedReader(reader);
        Statement stat = conn.createStatement();
        ResultSet rs = null;
        ScriptReader r = new ScriptReader(reader);
        while (true) {
            String sql = r.readStatement();
            if (sql == null) {
                break;
            }
            boolean resultSet = stat.execute(sql);
            if (resultSet) {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                rs = stat.getResultSet();
            }
        }
        return rs;
    }

    private void process(Connection conn, String fileName, boolean continueOnError, String charsetName) throws SQLException, IOException {
        InputStream in = FileUtils.openFileInputStream(fileName);
        String path = FileUtils.getParent(fileName);
        try {
            in = new BufferedInputStream(in, Constants.IO_BUFFER_SIZE);
            Reader reader = new InputStreamReader(in, charsetName);
            process(conn, continueOnError, path, reader, charsetName);
        } finally {
            IOUtils.closeSilently(in);
        }
    }

    private void process(Connection conn, boolean continueOnError, String path, Reader reader, String charsetName) throws SQLException, IOException {
        Statement stat = conn.createStatement();
        ScriptReader r = new ScriptReader(new BufferedReader(reader));
        while (true) {
            String sql = r.readStatement();
            if (sql == null) {
                break;
            }
            String trim = sql.trim();
            if (trim.startsWith("@") && StringUtils.toUpperEnglish(trim).startsWith("@INCLUDE")) {
                sql = trim;
                sql = sql.substring("@INCLUDE".length()).trim();
                if (!FileUtils.isAbsolute(sql)) {
                    sql = path + File.separator + sql;
                }
                process(conn, sql, continueOnError, charsetName);
            } else {
                try {
                    if (trim.length() > 0) {
                        if (showResults && !trim.startsWith("-->")) {
                            out.print(sql + ";");
                        }
                        if (showResults || checkResults) {
                            boolean query = stat.execute(sql);
                            if (query) {
                                ResultSet rs = stat.getResultSet();
                                int columns = rs.getMetaData().getColumnCount();
                                StringBuffer buff = new StringBuffer();
                                while (rs.next()) {
                                    buff.append("\n-->");
                                    for (int i = 0; i < columns; i++) {
                                        String s = rs.getString(i + 1);
                                        if (s != null) {
                                            s = StringUtils.replaceAll(s, "\r\n", "\n");
                                            s = StringUtils.replaceAll(s, "\n", "\n-->    ");
                                            s = StringUtils.replaceAll(s, "\r", "\r-->    ");
                                        }
                                        buff.append(' ');
                                        buff.append(s);
                                    }
                                }
                                buff.append("\n;");
                                String result = buff.toString();
                                if (showResults) {
                                    out.print(result);
                                }
                                if (checkResults) {
                                    String expected = r.readStatement() + ";";
                                    expected = StringUtils.replaceAll(expected, "\r\n", "\n");
                                    expected = StringUtils.replaceAll(expected, "\r", "\n");
                                    if (!expected.equals(result)) {
                                        expected = StringUtils.replaceAll(expected, " ", "+");
                                        result = StringUtils.replaceAll(result, " ", "+");
                                        throw new SQLException("Unexpected output for:\n" + sql.trim() + "\nGot:\n" + result + "\nExpected:\n" + expected);
                                    }
                                }

                            }
                        } else {
                            stat.execute(sql);
                        }
                    }
                } catch (SQLException e) {
                    if (continueOnError) {
                        e.printStackTrace(out);
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    private static void processRunscript(String url, String user, String password, String fileName, String options) throws SQLException {
        Connection conn = null;
        Statement stat = null;
        try {
            org.h2.Driver.load();
            conn = DriverManager.getConnection(url, user, password);
            stat = conn.createStatement();
            String sql = "RUNSCRIPT FROM '" + fileName + "' " + options;
            stat.execute(sql);
        } finally {
            JdbcUtils.closeSilently(stat);
            JdbcUtils.closeSilently(conn);
        }
    }

    /**
     * Executes the SQL commands in a script file against a database.
     *
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @param fileName the script file
     * @param charsetName the character set name or null for UTF-8
     * @param continueOnError if execution should be continued if an error occurs
     */
    public static void execute(String url, String user, String password, String fileName, String charsetName, boolean continueOnError) throws SQLException {
        new RunScript().process(url, user, password, fileName, charsetName, continueOnError);
    }

    /**
     * Executes the SQL commands in a script file against a database.
     *
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @param fileName the script file
     * @param charsetName the character set name or null for UTF-8
     * @param continueOnError if execution should be continued if an error occurs
     */
    void process(String url, String user, String password, String fileName, String charsetName, boolean continueOnError) throws SQLException {
        try {
            org.h2.Driver.load();
            Connection conn = DriverManager.getConnection(url, user, password);
            if (charsetName == null) {
                charsetName = Constants.UTF8;
            }
            try {
                process(conn, fileName, continueOnError, charsetName);
            } finally {
                conn.close();
            }
        } catch (IOException e) {
            throw Message.convertIOException(e, fileName);
        }
    }

    /**
     * If the statements as well as the results should be printed to the output.
     *
     * @param show true if yes
     */
    public void setShowResults(boolean show) {
        this.showResults = show;
    }

    /**
     * If results of statements should be cross-checked with the expected
     * output. The expected result is the next line(s) of the script, commented.
     *
     * @param check true if yes
     */
    public void setCheckResults(boolean check) {
        this.checkResults = check;
    }

}
