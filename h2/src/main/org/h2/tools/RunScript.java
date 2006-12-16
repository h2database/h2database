/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;

import org.h2.engine.Constants;
import org.h2.message.Message;
import org.h2.util.ScriptReader;
import org.h2.util.StringUtils;

/**
 * Executes the contents of a SQL script file against a database.
 * 
 * @author Tom
 * 
 */
public class RunScript {

    private static final boolean MULTI_THREAD = false;

    private void showUsage() {
        System.out.println("java " + getClass().getName() + " -url <url> -user <user> [-password <pwd>] [-script <file>] [-driver <driver] [-options <option> ...]");
    }

    /**
     * The command line interface for this tool. The options must be split into strings like this: "-user", "sa",... The
     * following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options)
     * <li>-url jdbc:h2:... (database URL)
     * <li>-user username
     * <li>-password password
     * <li>-script filename (default file name is backup.sql)
     * <li>-driver driver the JDBC driver class name (not required for H2)
     * <li>-options to specify a list of options (only for H2 and only when using the embedded mode)
     * </ul>
     * To include local files when using remote databases, use the special syntax:
     * <pre>
     * &#64;INCLUDE fileName
     * </pre>
     * This syntax is only supported by this tool. 
     * Embedded RUNSCRIPT SQL statements will be executed by the database.
     * 
     * @param args the command line arguments
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        new RunScript().run(args);
    }

    private void run(String[] args) throws SQLException {
        String url = null;
        String user = null;
        String password = "";
        String script = "backup.sql";
        String options = null;
        boolean continueOnError = false;
        for (int i = 0; args != null && i < args.length; i++) {
            if (args[i].equals("-url")) {
                url = args[++i];
            } else if (args[i].equals("-user")) {
                user = args[++i];
            } else if (args[i].equals("-password")) {
                password = args[++i];
            } else if (args[i].equals("-continueOnError")) {
                continueOnError = true;
            } else if (args[i].equals("-script")) {
                script = args[++i];
            } else if (args[i].equals("-driver")) {
                String driver = args[++i];
                try {
                    Class.forName(driver);
                } catch (ClassNotFoundException e) {
                    throw Message.convert(e);
                }
            } else if (args[i].equals("-options")) {
                StringBuffer buff = new StringBuffer();
                i++;
                for (; i < args.length; i++) {
                    buff.append(' ');
                    buff.append(args[i]);
                }
                options = buff.toString();
            } else {
                showUsage();
                return;
            }
        }
        if (url == null || user == null || password == null || script == null) {
            showUsage();
            return;
        }
        // long time = System.currentTimeMillis();
        // for(int i=0; i<10; i++) {
        // int test;
        if (options != null) {
            executeRunscript(url, user, password, script, options);
        } else {
            execute(url, user, password, script, null, continueOnError);
        }
        // }
        // time = System.currentTimeMillis() - time;
        // System.out.println("Done in " + time + " ms");
    }

    /**
     * Executes the SQL commands in a script file against a database.
     * 
     * @param conn the connection to a database
     * @param reader the reader
     * @return the last result set
     */
    public static ResultSet execute(Connection conn, Reader reader) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = null;
        ScriptReader r = new ScriptReader(reader);
        while (true) {
            String sql = r.readStatement();
            if (sql == null) {
                break;
            }
            boolean resultset = stat.execute(sql);
            if (resultset) {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                rs = stat.getResultSet();
            }
        }
        return rs;
    }

    private static void execute(Connection conn, HashMap threadMap, String fileName, boolean continueOnError, String charsetName) throws SQLException, IOException {
        InputStream in = new FileInputStream(fileName);
        String path = new File(fileName).getAbsoluteFile().getParent();
        try {
            BufferedInputStream bin = new BufferedInputStream(in, Constants.IO_BUFFER_SIZE);
            InputStreamReader reader = new InputStreamReader(bin, charsetName);
            execute(conn, threadMap, continueOnError, path, reader, charsetName);
        } finally {
            in.close();
        }
    }

    private static void execute(Connection conn, HashMap threadMap, boolean continueOnError, String path, Reader reader, String charsetName) throws SQLException, IOException {
        Statement stat = conn.createStatement();
        ScriptReader r = new ScriptReader(reader);
        while (true) {
            String sql = r.readStatement();
            if (sql == null) {
                break;
            }
            sql = sql.trim();
            if (sql.startsWith("@") && StringUtils.toUpperEnglish(sql).startsWith("@INCLUDE")) {
                sql = sql.substring("@INCLUDE".length()).trim();
                if(!new File(sql).isAbsolute()) {
                    sql = path + File.separator + sql;
                }
                execute(conn, threadMap, sql, continueOnError, charsetName);
            } else if (MULTI_THREAD && sql.startsWith("/*")) {
                int idx = sql.indexOf(']');
                Integer id = new Integer(Integer.parseInt(sql.substring("/*".length(), idx)));
                RunScriptThread thread = (RunScriptThread) threadMap.get(id);
                if (thread == null) {
                    Connection c = DriverManager.getConnection(conn.getMetaData().getURL());
                    thread = new RunScriptThread(id.intValue(), c);
                    threadMap.put(id, thread);
                    thread.start();
                }
                sql = sql.substring(sql.indexOf("*/") + 2).trim();
                String up = StringUtils.toUpperEnglish(sql);
                thread.addStatement(sql);
                if (up.startsWith("CREATE") || up.startsWith("DROP") || up.startsWith("ALTER")) {
                    thread.executeAll();
                } else {
                }
            } else {
                try {
                    stat.execute(sql);
                } catch (SQLException e) {
                    if (continueOnError) {
                        e.printStackTrace();
                    } else {
                        throw e;
                    }
                }
            }
        }
        Iterator it = threadMap.values().iterator();
        while (it.hasNext()) {
            RunScriptThread thread = (RunScriptThread) it.next();
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void executeRunscript(String url, String user, String password, String fileName, String options) throws SQLException {
        try {
            org.h2.Driver.load();
            Connection conn = DriverManager.getConnection(url, user, password);
            Statement stat = conn.createStatement();
            String sql = "RUNSCRIPT FROM '" + fileName + "' " + options;
            try {
                stat.execute(sql);
            } finally {
                conn.close();
            }
        } catch (Exception e) {
            throw Message.convert(e);
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
     * @throws SQLException
     */
    public static void execute(String url, String user, String password, String fileName, String charsetName, boolean continueOnError) throws SQLException {
        try {
            org.h2.Driver.load();
            Connection conn = DriverManager.getConnection(url, user, password);
            if (charsetName == null) {
                charsetName = Constants.UTF8;
            }
            HashMap threadMap = new HashMap();
            try {
                execute(conn, threadMap, fileName, continueOnError, charsetName);
            } finally {
                conn.close();
            }
        } catch (Exception e) {
            throw Message.convert(e);
        }
    }

}
