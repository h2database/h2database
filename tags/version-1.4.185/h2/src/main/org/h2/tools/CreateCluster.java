/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.store.fs.FileUtils;
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
        new CreateCluster().runTool(args);
    }

    @Override
    public void runTool(String... args) throws SQLException {
        String urlSource = null;
        String urlTarget = null;
        String user = "";
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
                showUsageAndThrowUnsupportedOption(arg);
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
     */
    public void execute(String urlSource, String urlTarget,
            String user, String password, String serverList) throws SQLException {
        process(urlSource, urlTarget, user, password, serverList);
    }

    private void process(String urlSource, String urlTarget,
            String user, String password, String serverList) throws SQLException {
        Connection connSource = null, connTarget = null;
        Statement statSource = null, statTarget = null;
        String scriptFile = "backup.sql";
        try {
            org.h2.Driver.load();

            // verify that the database doesn't exist,
            // or if it exists (an old cluster instance), it is deleted
            boolean exists = true;
            try {
                connTarget = DriverManager.getConnection(urlTarget +
                        ";IFEXISTS=TRUE;CLUSTER=" + Constants.CLUSTERING_ENABLED,
                        user, password);
                Statement stat = connTarget.createStatement();
                stat.execute("DROP ALL OBJECTS DELETE FILES");
                stat.close();
                exists = false;
                connTarget.close();
            } catch (SQLException e) {
                if (e.getErrorCode() == ErrorCode.DATABASE_NOT_FOUND_1) {
                    // database does not exists yet - ok
                    exists = false;
                } else {
                    throw e;
                }
            }
            if (exists) {
                throw new SQLException(
                        "Target database must not yet exist. Please delete it first: " +
                        urlTarget);
            }

            // use cluster='' so connecting is possible
            // even if the cluster is enabled
            connSource = DriverManager.getConnection(urlSource +
                    ";CLUSTER=''", user, password);
            statSource = connSource.createStatement();

            // enable the exclusive mode and close other connections,
            // so that data can't change while restoring the second database
            statSource.execute("SET EXCLUSIVE 2");

            try {

                // backup
                Script script = new Script();
                script.setOut(out);
                Script.process(connSource, scriptFile, "", "");

                // delete the target database and then restore
                connTarget = DriverManager.getConnection(
                        urlTarget + ";CLUSTER=''", user, password);
                statTarget = connTarget.createStatement();
                statTarget.execute("DROP ALL OBJECTS DELETE FILES");
                connTarget.close();

                RunScript runScript = new RunScript();
                runScript.setOut(out);
                runScript.process(urlTarget, user, password, scriptFile, null, false);

                connTarget = DriverManager.getConnection(urlTarget, user, password);
                statTarget = connTarget.createStatement();

                // set the cluster to the serverList on both databases
                statSource.executeUpdate("SET CLUSTER '" + serverList + "'");
                statTarget.executeUpdate("SET CLUSTER '" + serverList + "'");
            } finally {

                // switch back to the regular mode
                statSource.execute("SET EXCLUSIVE FALSE");
            }
        } finally {
            FileUtils.delete(scriptFile);
            JdbcUtils.closeSilently(statSource);
            JdbcUtils.closeSilently(statTarget);
            JdbcUtils.closeSilently(connSource);
            JdbcUtils.closeSilently(connTarget);
        }
    }

}
