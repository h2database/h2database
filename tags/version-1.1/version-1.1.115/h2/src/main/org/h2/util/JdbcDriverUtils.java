/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.SQLException;

/**
 * This class tries to automatically load the right JDBC driver for a given
 * database URL.
 */
public class JdbcDriverUtils {

    private static final String[] DRIVERS = {
        "jdbc:h2:", "org.h2.Driver",
        "jdbc:Cache:", "com.intersys.jdbc.CacheDriver",
        "jdbc:daffodilDB://", "in.co.daffodil.db.rmi.RmiDaffodilDBDriver",
        "jdbc:daffodil", "in.co.daffodil.db.jdbc.DaffodilDBDriver",
        "jdbc:db2:", "COM.ibm.db2.jdbc.net.DB2Driver",
        "jdbc:derby:net:", "org.apache.derby.jdbc.ClientDriver",
        "jdbc:derby://", "org.apache.derby.jdbc.ClientDriver",
        "jdbc:derby:", "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc:FrontBase:", "com.frontbase.jdbc.FBJDriver",
        "jdbc:firebirdsql:", "org.firebirdsql.jdbc.FBDriver",
        "jdbc:hsqldb:", "org.hsqldb.jdbcDriver",
        "jdbc:informix-sqli:", "com.informix.jdbc.IfxDriver",
        "jdbc:jtds:", "net.sourceforge.jtds.jdbc.Driver",
        "jdbc:microsoft:", "com.microsoft.jdbc.sqlserver.SQLServerDriver",
        "jdbc:mimer:", "com.mimer.jdbc.Driver",
        "jdbc:mysql:", "com.mysql.jdbc.Driver",
        "jdbc:odbc:", "sun.jdbc.odbc.JdbcOdbcDriver",
        "jdbc:oracle:", "oracle.jdbc.driver.OracleDriver",
        "jdbc:pervasive:", "com.pervasive.jdbc.v2.Driver",
        "jdbc:pointbase:micro:", "com.pointbase.me.jdbc.jdbcDriver",
        "jdbc:pointbase:", "com.pointbase.jdbc.jdbcUniversalDriver",
        "jdbc:postgresql:", "org.postgresql.Driver",
        "jdbc:sybase:", "com.sybase.jdbc3.jdbc.SybDriver",
        "jdbc:sqlserver:", "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "jdbc:teradata:", "com.ncr.teradata.TeraDriver",
    };

    private JdbcDriverUtils() {
        // utility class
    }

    /**
     * Get the driver class name for the given URL, or null if the URL is
     * unknown.
     *
     * @param url the database URL
     * @return the driver class name
     */
    public static String getDriver(String url) {
        for (int i = 0; i < DRIVERS.length; i += 2) {
            String prefix = DRIVERS[i];
            if (url.startsWith(prefix)) {
                return DRIVERS[i + 1];
            }
        }
        return null;
    }

    /**
     * Load the driver class for the given URL, if the database URL is known.
     *
     * @param url the database URL
     */
    public static void load(String url) throws SQLException {
        String driver = getDriver(url);
        if (driver != null) {
            ClassUtils.loadUserClass(driver);
        }
    }

}
