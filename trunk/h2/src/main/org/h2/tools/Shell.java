/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.StringTokenizer;

import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.util.ClassUtils;
import org.h2.util.JdbcUtils;

/**
 * Interactive command line tool to access a database using JDBC.
 */
public class Shell {

    private Connection conn;
    private Statement stat;
    
    private void showUsage() {
        System.out.println("java " + getClass().getName() + " [-url <url> -user <user> -password <pwd> -driver <driver]");
        System.out.println("See also http://h2database.com/javadoc/org/h2/tools/Prompt.html");
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
     * <li>-driver driver the JDBC driver class name (not required for H2)
     * </li>
     * </ul>
     * 
     * @param args the command line arguments
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        new Shell().run(args);
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
                System.out.println("Unsupported option: " + args[i]);
                showUsage();
                return;
            }
        }
        if (url != null) {
            org.h2.Driver.load();
            conn = DriverManager.getConnection(url, user, password);
        }
        promptLoop();
    }
    
    private void showHelp() {
        System.out.println("Commands are case insensitive; SQL statements end with ';'");
        System.out.println("HELP or ?     - Display this help");
        System.out.println("CONNECT       - Connect to a database. Optional arguments: url, user, password");
        System.out.println("DRIVER        - Load a JDBC driver class (usually not required)");
        System.out.println("QUIT or EXIT  - End this program");
        System.out.println();
    }

    private void promptLoop() {
        System.out.println();
        System.out.println("Welcome to the H2 Shell " + Constants.getVersion());
        showHelp();
        String statement = null;
        while (true) {
            try {
                if (statement == null) {
                    System.out.print("sql> ");
                } else {
                    System.out.print("...> ");
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
                } else {
                    if (statement == null) {
                        if (upper.startsWith("DRIVER")) {
                            loadDriver(line);
                        } else if (upper.startsWith("CONNECT")) {
                            conn = connect(line);
                            stat = conn.createStatement();
                        } else {
                            statement = line;
                        }
                    } else {
                        statement = statement + " " + line;
                    }
                    if (end) {
                        execute(statement);
                        statement = null;
                    }
                }
            } catch (SQLException e) {
                System.out.println("SQL Exception: " + e.getMessage());
                statement = null;
            } catch (IOException e) {
                System.out.println(e.getMessage());
                break;
            } catch (Exception e) {
                System.out.println("Exception: " + e.toString());
                e.printStackTrace();
                break;
            }
        }
        if (conn != null) {
            try {
                conn.close();
                System.out.println("Connection closed");
            } catch (SQLException e) {
                System.out.println("SQL Exception:");
                e.printStackTrace();
            }
        }
    }

    private void execute(String sql) throws SQLException {
        if (stat == null) {
            System.out.println("Not connected; type CONNECT to open a connection");
            return;
        }
        stat.execute(sql);
    }
    
    private void loadDriver(String statement) throws IOException, ClassNotFoundException, SQLException {
        StringTokenizer tokenizer = new StringTokenizer(statement);
        tokenizer.nextToken();
        String driver;
        if (tokenizer.hasMoreTokens()) {
            driver = tokenizer.nextToken();
        } else {
            System.out.print("URL: ");
            driver = readLine();
        }
        ClassUtils.loadUserClass(driver);
    }

    private Connection connect(String statement) throws IOException, SQLException {
        StringTokenizer tokenizer = new StringTokenizer(statement);
        tokenizer.nextToken();
        String url, user, password;
        if (tokenizer.hasMoreTokens()) {
            url = tokenizer.nextToken();
        } else {
            System.out.print("URL     : ");
            url = readLine();
        }
        if (tokenizer.hasMoreTokens()) {
            user = tokenizer.nextToken();
        } else {
            System.out.print("User    : ");
            user = readLine();
        }
        if (tokenizer.hasMoreTokens()) {
            password = tokenizer.nextToken();
        } else {
            password = readPassword();
        }
        Connection conn = JdbcUtils.getConnection(null, url, user, password);
        System.out.println("Connected");
        return conn;
    }

    private String readPassword() throws IOException {
        class PasswordHider extends Thread {
            volatile boolean stop;
            public void run() {
                while (!stop) {
                    System.out.print("\b\b><");
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }
        PasswordHider thread = new PasswordHider();
        thread.start();
        System.out.print("Password: > ");
        String p = readLine();
        thread.stop = true;
        try {
            thread.join();
        } catch (InterruptedException e) {
            // ignore
        }
        System.out.print("\b\b");
        return p;
    }

    private String readLine() throws IOException {
        String line = new BufferedReader(new InputStreamReader(System.in)).readLine();
        if (line == null) {
            throw new IOException("Aborted");
        }
        return line;
    }

}
