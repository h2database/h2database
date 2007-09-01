/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.util.FileUtils;
import org.h2.util.JdbcUtils;

/**
 * Tool to create a database cluster.
 * This will copy a database to another location if required, and modify the cluster setting.
 * 
 * @author Thomas
 */
public class CreateCluster {
    
    private void showUsage() {
        System.out.println("java "+getClass().getName()
                + " -urlSource <url> -urlTarget <url> -user <user> [-password <pwd>] -serverlist <serverlist>");
    }

    /**
     * The command line interface for this tool.
     * The options must be split into strings like this: "-urlSource", "jdbc:h2:test",... 
     * The following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options)
     * </li><li>-urlSource jdbc:h2:... (the database URL of the source database)
     * </li><li>-urlTarget jdbc:h2:... (the database URL of the target database)
     * </li></ul>
     * 
     * @param args the command line arguments
     * @throws SQLException
     */    
    public static void main(String[] args) throws SQLException {
        new CreateCluster().run(args);
    }

    private void run(String[] args) throws SQLException {
        String urlSource = null;
        String urlTarget = null;
        String user = null;
        String password = "";
        String serverlist = null;
        for (int i = 0; args != null && i < args.length; i++) {
            if (args[i].equals("-urlSource")) {
                urlSource = args[++i];
            } else if (args[i].equals("-urlTarget")) {
                urlTarget = args[++i];
            } else if (args[i].equals("-user")) {
                user = args[++i];
            } else if (args[i].equals("-password")) {
                password = args[++i];
            } else if (args[i].equals("-serverlist")) {
                serverlist = args[++i];
            } else {
                showUsage();
                return;
            }
        }
        if (urlSource == null || urlTarget == null || user == null || serverlist == null) {
            showUsage();
            return;
        }
        
        execute(urlSource, urlTarget, user, password, serverlist);
    }
    
    /**
     * Creates a cluster.
     * 
     * @param urlSource the database URL of the original database
     * @param urlTarget the database URL of the copy
     * @param user the user name
     * @param password the password
     * @param serverlist the server list 
     * @throws SQLException
     */
    public static void execute(String urlSource, String urlTarget, String user, String password, String serverlist) throws SQLException {
        Connection conn = null;
        Statement stat = null;
        try {
            org.h2.Driver.load();
            // use cluster='' so connecting is possible even if the cluster is enabled
            conn = DriverManager.getConnection(urlSource + ";CLUSTER=''", user, password);
            conn.close();
            boolean exists;
            try {
                conn = DriverManager.getConnection(urlTarget + ";IFEXISTS=TRUE", user, password);
                conn.close();
                exists = true;
            } catch (SQLException e) {
                // database does not exists - ok
                exists = false;
            }
            if (exists) {
                throw new SQLException("Target database must not yet exist. Please delete it first");
            }
            
            // TODO cluster: need to open the database in exclusive mode, so that other applications
            // cannot change the data while it is restoring the second database. But there is currently no exclusive mode.
            
            String scriptFile = "backup.sql";
            Script.execute(urlSource, user, password, scriptFile);
            RunScript.execute(urlTarget, user, password, scriptFile, null, false);
            FileUtils.delete(scriptFile);
            
            // set the cluster to the serverlist on both databases
            conn = DriverManager.getConnection(urlSource, user, password);
            stat = conn.createStatement();
            stat.executeUpdate("SET CLUSTER '" + serverlist + "'");
            conn.close();
            conn = DriverManager.getConnection(urlTarget, user, password);
            stat = conn.createStatement();
            stat.executeUpdate("SET CLUSTER '" + serverlist + "'");
        } finally {
            JdbcUtils.closeSilently(conn);
            JdbcUtils.closeSilently(stat);
        }
    }
    
}
