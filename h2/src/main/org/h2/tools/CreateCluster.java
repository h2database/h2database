/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.util.FileUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.Tool;

/**
 * Tool to create a database cluster. This will copy a database to another
 * location if required, and modify the cluster setting.
 */
public class CreateCluster extends Tool {

    private void showUsage() {
        out.println("Creates a cluster from a standalone database.");
        out.println("java "+getClass().getName() + "\n" +
                " -urlSource <url>    The database URL of the source database (jdbc:h2:...)\n" +
                " -urlTarget <url>    The database URL of the target database (jdbc:h2:...)\n" +
                " -user <user>        The user name\n" +
                " [-password <pwd>]   The password\n" +
                " -serverList <list>  The comma separated list of host names or IP addresses");
        out.println("See also http://h2database.com/javadoc/" + getClass().getName().replace('.', '/') + ".html");
    }

    /**
     * The command line interface for this tool. The options must be split into
     * strings like this: "-urlSource", "jdbc:h2:test",... Options are case
     * sensitive. The following options are supported:
     * <ul>
     * <li>-help or -? (print the list of options) </li>
     * <li>-urlSource jdbc:h2:... (the database URL of the source database)
     * </li>
     * <li>-urlTarget jdbc:h2:... (the database URL of the target database)
     * </li><li>-user (the user name)
     * </li><li>-password (the password)
     * </li><li>-serverList (the server list)
     * </li></ul>
     * 
     * @param args the command line arguments
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        new CreateCluster().run(args);
    }

    public void run(String[] args) throws SQLException {
        String urlSource = null;
        String urlTarget = null;
        String user = null;
        String password = "";
        String serverList = null;
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("-urlSource")) {
                urlSource = args[++i];
            } else if (arg.equals("-urlTarget")) {
                urlTarget = args[++i];
            } else if (arg.equals("-user")) {
                user = args[++i];
            } else if (arg.equals("-password")) {
                password = args[++i];
            } else if (arg.equals("-serverList")) {
                serverList = args[++i];
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                out.println("Unsupported option: " + arg);
                showUsage();
                return;
            }
        }
        if (urlSource == null || urlTarget == null || user == null || serverList == null) {
            showUsage();
            return;
        }
        process(urlSource, urlTarget, user, password, serverList);
    }

    /**
     * Creates a cluster.
     *
     * @param urlSource the database URL of the original database
     * @param urlTarget the database URL of the copy
     * @param user the user name
     * @param password the password
     * @param serverList the server list
     * @throws SQLException
     */
    public void execute(String urlSource, String urlTarget, String user, String password, String serverList) throws SQLException {
        new CreateCluster().process(urlSource, urlTarget, user, password, serverList);
    }
    
    private void process(String urlSource, String urlTarget, String user, String password, String serverList) throws SQLException {
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

            // TODO cluster: need to open the database in exclusive mode, 
            // so that other applications
            // cannot change the data while it is restoring the second database. 
            // But there is currently no exclusive mode.

            String scriptFile = "backup.sql";
            Script sc = new Script();
            sc.setOut(out);
            sc.process(urlSource, user, password, scriptFile);
            RunScript runscript = new RunScript();
            runscript.setOut(out);
            runscript.process(urlTarget, user, password, scriptFile, null, false);
            FileUtils.delete(scriptFile);

            // set the cluster to the serverList on both databases
            conn = DriverManager.getConnection(urlSource, user, password);
            stat = conn.createStatement();
            stat.executeUpdate("SET CLUSTER '" + serverList + "'");
            conn.close();
            conn = DriverManager.getConnection(urlTarget, user, password);
            stat = conn.createStatement();
            stat.executeUpdate("SET CLUSTER '" + serverList + "'");
        } finally {
            JdbcUtils.closeSilently(conn);
            JdbcUtils.closeSilently(stat);
        }
    }

}
