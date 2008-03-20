/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.server.web.ConnectionInfo;
import org.h2.util.ClassUtils;
import org.h2.util.FileUtils;
import org.h2.util.JdbcDriverUtils;
import org.h2.util.JdbcUtils;

/**
 * Interactive command line tool to access a database using JDBC.
 */
public class Shell {

    private Connection conn;
    private Statement stat;
    private PrintStream out = System.out;
    private boolean listMode;
    private int maxColumnSize = 100;
    private char boxVertical = '|'; // windows: '\u00b3';
    
    /**
     * The command line interface for this tool. The options must be split into
     * strings like this: "-user", "sa",... Options are case sensitive. The
     * following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options) </li>
     * <li>-url jdbc:h2:... (database URL) </li>
     * <li>-user username </li>
     * <li>-password password </li>
     * <li>-driver driver the JDBC driver class name (not required for most
     * databases) </li>
     * </ul>
     * 
     * @param args the command line arguments
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        new Shell().run(args);
    }
    
    private void showUsage() {
        out.println("java " + getClass().getName() + " [-url <url> -user <user> -password <pwd> -driver <driver]");
        out.println("See also http://h2database.com/javadoc/org/h2/tools/Prompt.html");
    }
    
    private void run(String[] args) throws SQLException {
        String url = null;
        String user = "";
        String password = "";
        for (int i = 0; args != null && i < args.length; i++) {
            if (args[i].equals("-url")) {
                url = args[++i];
            } else if (args[i].equals("-user")) {
                user = args[++i];
            } else if (args[i].equals("-password")) {
                password = args[++i];
            } else if (args[i].equals("-driver")) {
                String driver = args[++i];
                try {
                    ClassUtils.loadUserClass(driver);
                } catch (ClassNotFoundException e) {
                    throw Message.convert(e);
                }
            } else {
                out.println("Unsupported option: " + args[i]);
                showUsage();
                return;
            }
        }
        if (url != null) {
            org.h2.Driver.load();
            conn = DriverManager.getConnection(url, user, password);
            stat = conn.createStatement();
        }
        promptLoop();
    }
    
    private void showHelp() {
        out.println("Commands are case insensitive; SQL statements end with ';'");
        out.println("help or ?     - Display this help");
        out.println("list          - Toggle result list mode");
        out.println("maxwidth      - Set maximum column width (default is 100)");
        out.println("show          - List all tables");
        out.println("describe      - Describe a table");
        out.println("quit or exit  - Close the connection and exit");
        out.println();
    }

    private void promptLoop() {
        out.println();
        out.println("Welcome to H2 Shell " + Constants.getVersion());
        out.println("Exit with Ctrl+C");
        if (conn != null) {
            showHelp();
        }
        String statement = null;
        while (true) {
            try {
                if (conn == null) {
                    connect();
                    showHelp();
                }
                if (statement == null) {
                    out.print("sql> ");
                } else {
                    out.print("...> ");
                }
                String line = readLine();
                if (line == null) {
                    break;
                }
                line = line.trim();
                if (line.length() == 0) {
                    continue;
                }
                boolean end = line.endsWith(";");
                if (end) {
                    line = line.substring(0, line.length() - 1).trim();
                }
                String upper = line.toUpperCase();
                if ("EXIT".equals(upper) || "QUIT".equals(upper)) {
                    break;
                } else if ("HELP".equals(upper) || "?".equals(upper)) {
                    showHelp();
                } else if ("LIST".equals(upper)) {
                    listMode = !listMode;
                    out.println("Result list mode is now " + (listMode ? "on" : "off"));
                } else if (upper.startsWith("DESCRIBE")) {
                    String tableName = upper.substring("DESCRIBE".length()).trim();
                    if (tableName.length() == 0) {
                        out.println("Usage: describe <table name>");
                    } else {
                        try {
                            PreparedStatement prep = conn.prepareStatement(
                                    "SELECT CAST(COLUMN_NAME AS VARCHAR(32)) \"Column Name\", " + 
                                    "CAST(TYPE_NAME AS VARCHAR(14)) \"Type\", " + 
                                    "NUMERIC_PRECISION \"Precision\", " + 
                                    "CAST(IS_NULLABLE AS VARCHAR(8)) \"Nullable\", " + 
                                    "CAST(COLUMN_DEFAULT AS VARCHAR(20)) \"Default\" " + 
                                    "FROM INFORMATION_SCHEMA.COLUMNS " + 
                                    "WHERE UPPER(TABLE_NAME)=? ORDER BY ORDINAL_POSITION");
                            prep.setString(1, tableName.toUpperCase());
                            ResultSet rs = prep.executeQuery();
                            printResult(rs, false);
                        } catch (SQLException e) {
                            out.println("Exception: " + e.toString());
                            e.printStackTrace();
                        }
                    }
                } else if (upper.startsWith("SHOW")) {
                    try {
                        ResultSet rs = stat.executeQuery(
                                "SELECT CAST(TABLE_SCHEMA AS VARCHAR(32)) \"Schema\", TABLE_NAME \"Table Name\" " +
                                "FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA, TABLE_NAME");
                        printResult(rs, false);
                    } catch (SQLException e) {
                        out.println("Exception: " + e.toString());
                        e.printStackTrace();
                    }
                } else if (upper.startsWith("MAXWIDTH")) {
                    upper = upper.substring("MAXWIDTH".length()).trim();
                    try {
                        maxColumnSize = Integer.parseInt(upper);
                    } catch (Exception e) {
                        out.println("Usage: maxwidth <integer value>");
                    }
                    out.println("Maximum column width is now " + maxColumnSize);
                } else {
                    if (statement == null) {
                        statement = line;
                    } else {
                        statement = statement + " " + line;
                    }
                    if (end) {
                        execute(statement, listMode);
                        statement = null;
                    }
                }
            } catch (SQLException e) {
                out.println("SQL Exception: " + e.getMessage());
                statement = null;
            } catch (IOException e) {
                out.println(e.getMessage());
                break;
            } catch (Exception e) {
                out.println("Exception: " + e.toString());
                e.printStackTrace();
                break;
            }
        }
        if (conn != null) {
            try {
                conn.close();
                out.println("Connection closed");
            } catch (SQLException e) {
                out.println("SQL Exception:");
                e.printStackTrace();
            }
        }
    }

    private void connect() throws IOException, SQLException {
        String propertiesFileName = FileUtils.getFileInUserHome(Constants.SERVER_PROPERTIES_FILE);
        String url = "jdbc:h2:~/test";
        String user = "sa";
        String driver = null;
        try {
            Properties prop = FileUtils.loadProperties(propertiesFileName);
            String data = null;
            for (int i = 0;; i++) {
                String d = prop.getProperty(String.valueOf(i));
                if (d == null) {
                    break;
                }
                data = d;
            }
            if (data != null) {
                ConnectionInfo info = new ConnectionInfo(data);
                url = info.url;
                user = info.user;
                driver = info.driver;
            }
        } catch (IOException e) {
            // ignore
        }
        out.println("[Enter]   " + url);
        out.print("URL       ");
        url = readLine(url);
        if (driver == null) {
            driver = JdbcDriverUtils.getDriver(url);
        }
        if (driver != null) {
            out.println("[Enter]   " + driver);
        }
        out.print("Driver    ");
        driver = readLine(driver);
        out.println("[Enter]   " + user);
        out.print("User      ");
        user = readLine(user);
        out.println("[Enter]   Hide");
        out.print("Password  ");
        String password = readLine();
        if (password.length() == 0) {
            password = readPassword();
        }
        conn = JdbcUtils.getConnection(driver, url, user, password);
        stat = conn.createStatement();
        out.println("Connected");
    }

    private String readPassword() throws IOException {
        class PasswordHider extends Thread {
            volatile boolean stop;
            public void run() {
                while (!stop) {
                    out.print("\b\b><");
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }
        PasswordHider thread = new PasswordHider();
        thread.start();
        out.print("Password  > ");
        String p = readLine();
        thread.stop = true;
        try {
            thread.join();
        } catch (InterruptedException e) {
            // ignore
        }
        out.print("\b\b");
        return p;
    }
    
    private String readLine(String defaultValue) throws IOException {
        String s = readLine();
        return s.length() == 0 ? defaultValue : s;
    }

    private String readLine() throws IOException {
        String line = new BufferedReader(new InputStreamReader(System.in)).readLine();
        if (line == null) {
            throw new IOException("Aborted");
        }
        return line;
    }
    
    private void execute(String sql, boolean listMode) throws SQLException {
        long time = System.currentTimeMillis();
        boolean result;
        try {
            result = stat.execute(sql);
        } catch (SQLException e) {
            out.println("Error: " + e.toString());
            return;
        }
        try {
            if (result) {
                ResultSet rs = stat.getResultSet();
                int rowCount = printResult(rs, listMode);
                time = System.currentTimeMillis() - time;
                out.println("(" + rowCount + (rowCount == 1 ? " row, " : " rows, ") + time + " ms)");
            } else {
                int updateCount = stat.getUpdateCount();
                time = System.currentTimeMillis() - time;
                out.println("(Update count: " + updateCount + ", " + time + " ms)");
            }
        } catch (SQLException e) {
            out.println("Error: " + e.toString());
            e.printStackTrace();
        }
    }
    
    private int printResult(ResultSet rs, boolean listMode) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int longest = 0;
        int len = meta.getColumnCount();
        String[] columns = new String[len];
        int[] columnSizes = new int[len];
        int total = 0;
        for (int i = 0; i < len; i++) {
            String s = meta.getColumnLabel(i + 1);
            int l = s.length();
            if (!listMode) {
                l = Math.max(l, meta.getColumnDisplaySize(i + 1));
                l = Math.min(maxColumnSize, l);
            }
            if (s.length() > l) {
                s = s.substring(0, l);
            }
            columns[i] = s;
            columnSizes[i] = l;
            longest = Math.max(longest, l);
            total += l;
        }
        StringBuffer buff = new StringBuffer();
        if (!listMode) {
            for (int i = 0; i < len; i++) {
                if (i > 0) {
                    buff.append(boxVertical);
                }
                String s = columns[i];
                buff.append(s);
                if (i < len - 1) {
                    for (int j = s.length(); j < columnSizes[i]; j++) {
                        buff.append(' ');
                    }
                }
            }
            out.println(buff.toString());
        }
        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
            buff.setLength(0);
            if (listMode) {
                if (rowCount > 1) {
                    out.println();
                }
                for (int i = 0; i < len; i++) {
                    if (i > 0) {
                        buff.append('\n');
                    }
                    String label = columns[i];
                    buff.append(label);
                    for (int j = label.length(); j < longest; j++) {
                        buff.append(' ');
                    }
                    buff.append(": ");
                    buff.append(rs.getString(i + 1));
                }
            } else {
                for (int i = 0; i < len; i++) {
                    if (i > 0) {
                        buff.append(boxVertical);
                    }
                    String s = rs.getString(i + 1);
                    if (s == null) {
                        s = "null";
                    }
                    int m = columnSizes[i];
                    if (!listMode && s.length() > m) {
                        s = s.substring(0, m);
                    }
                    buff.append(s);
                    if (i < len - 1) {
                        for (int j = s.length(); j < m; j++) {
                            buff.append(' ');
                        }
                    }
                }
            }
            out.println(buff.toString());
        }
        if (rowCount == 0 && listMode) {
            for (int i = 0; i < len; i++) {
                String label = columns[i];
                buff.append(label);
                buff.append('\n');
            }
            out.println(buff.toString());
        }
        return rowCount;
    }

}
