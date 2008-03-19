/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

/**
 * This class tries to automatically load the right JDBC driver for a given
 * database URL.
 */
public class JdbcDriverLoader {

    private static final String[] DRIVERS = {
        "jdbc:h2:", "org.h2.Driver",
        "jdbc:firebirdsql:", "org.firebirdsql.jdbc.FBDriver",
        "jdbc:db2:", "COM.ibm.db2.jdbc.net.DB2Driver",
        "jdbc:oracle:", "oracle.jdbc.driver.OracleDriver",
        "jdbc:microsoft:", "com.microsoft.jdbc.sqlserver.SQLServerDriver",
        "jdbc:sqlserver:", "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "jdbc:postgresql:", "org.postgresql.Driver",
        "jdbc:mysql:", "com.mysql.jdbc.Driver",
        "jdbc:derby://", "org.apache.derby.jdbc.ClientDriver",
        "jdbc:derby:", "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc:hsqldb:", "org.hsqldb.jdbcDriver"
    };

    public static void load(String url) throws ClassNotFoundException {
        for (int i = 0; i < DRIVERS.length; i += 2) {
            String prefix = DRIVERS[i];
            if (url.startsWith(prefix)) {
                Class.forName(DRIVERS[i + 1]);
                break;
            }
        }
    }

}
