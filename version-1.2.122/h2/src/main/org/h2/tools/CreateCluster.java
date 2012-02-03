/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
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
 * Creates a cluster from a standalone database.
 * <br />
 * Copies a database to another location if required.
 * @h2.resource
 */
public class CreateCluster extends Tool {

    /**
     * Options are case sensitive. Supported options are:
     * <table>
     * <tr><td>[-help] or [-?]</td>
     * <td>Print the list of options</td></tr>
     * <tr><td>[-urlSource "&lt;url&gt;"]</td>
     * <td>The database URL of the source database (jdbc:h2:...)</td></tr>
     * <tr><td>[-urlTarget "&lt;url&gt;"]</td>
     * <td>The database URL of the target database (jdbc:h2:...)</td></tr>
     * <tr><td>[-user &lt;user&gt;]</td>
     * <td>The user name (default: sa)</td></tr>
     * <tr><td>[-password &lt;pwd&gt;]</td>
     * <td>The password</td></tr>
     * <tr><td>[-serverList &lt;list&gt;]</td>
     * <td>The comma separated list of host names or IP addresses</td></tr>
     * </table>
     * @h2.resource
     *
     * @param args the command line arguments
     */
    public static void main(String... args) throws SQLException {
        new CreateCluster().run(args);
    }

    public void run(String... args) throws SQLException {
        String urlSource = null;
        String urlTarget = null;
        String user = "sa";
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
                throwUnsupportedOption(arg);
            }
        }
        if (urlSource == null || urlTarget == null || serverList == null) {
            showUsage();
            throw new SQLException("Source URL, target URL, or server list not set");
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
