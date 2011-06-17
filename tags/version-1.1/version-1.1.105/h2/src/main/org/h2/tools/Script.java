/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;
import org.h2.util.Tool;

/**
 * Creates a SQL script file by extracting the schema and data of a database.
 */
public class Script extends Tool {

    private void showUsage() {
        out.println("Allows converting a database to a SQL script.");
        out.println("java "+getClass().getName() + "\n" +
                " -url <url>         The database URL\n" +
                " -user <user>       The user name\n" +
                " [-password <pwd>]  The password\n" +
                " [-script <file>]   The script file to run (default: backup.sql)\n" +
                " [-quiet]           Do not print progress information\n" +
                " [-options ...]     The list of options (only for H2 embedded mode)");
        out.println("See also http://h2database.com/javadoc/" + getClass().getName().replace('.', '/') + ".html");
    }

    /**
     * The command line interface for this tool.
     * The options must be split into strings like this: "-user", "sa",...
     * Options are case sensitive. The following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options)
     * </li><li>-url jdbc:h2:... (database URL)
     * </li><li>-user username
     * </li><li>-password password
     * </li><li>-script filename (default file name is backup.sql)
     * </li><li>-options to specify a list of options (only for H2)
     * </li></ul>
     *
     * @param args the command line arguments
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        new Script().run(args);
    }

    public void run(String[] args) throws SQLException {
        String url = null;
        String user = null;
        String password = "";
        String file = "backup.sql";
        String options1 = null, options2 = null;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-url")) {
                url = args[++i];
            } else if (arg.equals("-user")) {
                user = args[++i];
            } else if (arg.equals("-password")) {
                password = args[++i];
            } else if (arg.equals("-script")) {
                file = args[++i];
            } else if (arg.equals("-options")) {
                StringBuffer buff1 = new StringBuffer();
                StringBuffer buff2 = new StringBuffer();
                i++;
                for (; i < args.length; i++) {
                    String a = args[i];
                    String upper = StringUtils.toUpperEnglish(a);
                    if (upper.startsWith("NO") || "DROP".equals(upper)) {
                        buff1.append(' ');
                        buff1.append(args[i]);
                    } else {
                        buff2.append(' ');
                        buff2.append(args[i]);
                    }
                }
                options1 = buff1.toString();
                options2 = buff2.toString();
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                out.println("Unsupported option: " + arg);
                showUsage();
                return;
            }
        }
        if (url == null || user == null || file == null) {
            showUsage();
            return;
        }
        if (options1 != null) {
            processScript(url, user, password, file, options1, options2);
        } else {
            process(url, user, password, file);
        }
    }

    private void processScript(String url, String user, String password, String fileName, String options1, String options2) throws SQLException {
        Connection conn = null;
        Statement stat = null;
        try {
            org.h2.Driver.load();
            conn = DriverManager.getConnection(url, user, password);
            stat = conn.createStatement();
            String sql = "SCRIPT " + options1 + " TO '" + fileName + "' " + options2;
            stat.execute(sql);
        } finally {
            JdbcUtils.closeSilently(stat);
            JdbcUtils.closeSilently(conn);
        }
    }

    /**
     * Backs up a database to a SQL script file.
     *
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @param fileName the script file
     */
    public static void execute(String url, String user, String password, String fileName) throws SQLException {
        new Script().process(url, user, password, fileName);
    }

    /**
     * Backs up a database to a SQL script file.
     *
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @param fileName the script file
     */
    void process(String url, String user, String password, String fileName) throws SQLException {
        Connection conn = null;
        Statement stat = null;
        Writer fileWriter = null;
        try {
            org.h2.Driver.load();
            conn = DriverManager.getConnection(url, user, password);
            stat = conn.createStatement();
            fileWriter = IOUtils.getWriter(FileUtils.openFileOutputStream(fileName, false));
            PrintWriter writer = new PrintWriter(fileWriter);
            ResultSet rs = stat.executeQuery("SCRIPT");
            while (rs.next()) {
                String s = rs.getString(1);
                writer.println(s);
            }
            writer.close();
        } finally {
            JdbcUtils.closeSilently(stat);
            JdbcUtils.closeSilently(conn);
            IOUtils.closeSilently(fileWriter);
        }
    }

}
