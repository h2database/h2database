/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.message.Message;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.StringUtils;

/**
 * Creates a SQL script file by extracting the schema and data of a database.
 * 
 * @author Thomas
 */
public class Script {

    private void showUsage() {
        System.out.println("java "+getClass().getName()
                + " -url <url> -user <user> [-password <pwd>] [-script <filename>] [-options <option> ...]");
    }

    /**
     * The command line interface for this tool.
     * The options must be split into strings like this: "-user", "sa",... 
     * The following options are supported:
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

    private void run(String[] args) throws SQLException {
        String url = null;
        String user = null;
        String password = "";
        String file = "backup.sql";
        String options1 = null, options2 = null;
        for(int i=0; args != null && i<args.length; i++) {
            if(args[i].equals("-url")) {
                url = args[++i];
            } else if(args[i].equals("-user")) {
                user = args[++i];
            } else if(args[i].equals("-password")) {
                password = args[++i];
            } else if(args[i].equals("-script")) {
                file = args[++i];
            } else if(args[i].equals("-options")) {
                StringBuffer buff1 = new StringBuffer();
                StringBuffer buff2 = new StringBuffer();
                i++;
                for(; i<args.length; i++) {
                    String a = args[i];
                    String upper = StringUtils.toUpperEnglish(a);
                    if(upper.startsWith("NO") || upper.equals("DROP")) {
                        buff1.append(' ');
                        buff1.append(args[i]);
                    } else {
                        buff2.append(' ');
                        buff2.append(args[i]);
                    }
                }
                options1 = buff1.toString();
                options2 = buff2.toString();
            } else {
                showUsage();
                return;
            }
        }
        if(url==null || user==null || file == null) {
            showUsage();
            return;
        }        
        if(options1 != null) {
            executeScript(url, user, password, file, options1, options2);
        } else {       
            execute(url, user, password, file);
        }
    }
    
    /**
     * INTERNAL
     */
    public static void executeScript(String url, String user, String password, String fileName, String options1, String options2) throws SQLException {
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
     * Backs up a database to a file.
     * 
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @param fileName the script file
     * @throws SQLException
     */
    public static void execute(String url, String user, String password, String fileName) throws SQLException {
        Connection conn = null;
        Statement stat = null;        
        FileWriter fileWriter = null;
        try {
            org.h2.Driver.load();
            conn = DriverManager.getConnection(url, user, password);
            stat = conn.createStatement();
            fileWriter = new FileWriter(fileName);
            PrintWriter writer = new PrintWriter(new BufferedWriter(fileWriter));
            ResultSet rs = stat.executeQuery("SCRIPT");
            while(rs.next()) {
                String s = rs.getString(1);
                writer.println(s + ";");
            }
            writer.close();
        } catch(IOException e) {
            throw Message.convertIOException(e, fileName);
        } finally {
            JdbcUtils.closeSilently(stat);
            JdbcUtils.closeSilently(conn);
            IOUtils.closeSilently(fileWriter);
        }
    }
    
}
