/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.sql.XAConnection;

import org.h2.message.Message;

/**
 * This is a utility class with JDBC helper functions.
 */
public class JdbcUtils {

    private JdbcUtils() {
        // utility class
    }

    /**
     * Close a statement without throwing an exception.
     *
     * @param stat the statement or null
     */
    public static void closeSilently(Statement stat) {
        if (stat != null) {
            try {
                stat.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Close a connection without throwing an exception.
     *
     * @param conn the connection or null
     */
    public static void closeSilently(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Close a result set without throwing an exception.
     *
     * @param rs the result set or null
     */
    public static void closeSilently(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /**
     * Get the result set containing the generated keys from the given
     * statement. This method returns null for Java versions older than 1.4.
     *
     * @param stat the statement
     * @return the result set or null
     */
    public static ResultSet getGeneratedKeys(Statement stat) throws SQLException {
        ResultSet rs = null;
        //## Java 1.4 begin ##
        rs = stat.getGeneratedKeys();
        //## Java 1.4 end ##
        return rs;
    }

    /**
     * Close an XA connection set without throwing an exception.
     *
     * @param conn the XA connection or null
     */
//## Java 1.4 begin ##
    public static void closeSilently(XAConnection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }
//## Java 1.4 end ##

    /**
     * Open a new database connection with the given settings.
     *
     * @param driver the driver class name
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @return the database connection
     */
    public static Connection getConnection(String driver, String url, String user, String password) throws SQLException {
        Properties prop = new Properties();
        if (user != null) {
            prop.setProperty("user", user);
        }
        if (password != null) {
            prop.setProperty("password", password);
        }
        return getConnection(driver, url, prop);
    }

    /**
     * Escape table or schema patterns used for DatabaseMetaData functions.
     *
     * @param pattern the pattern
     * @return the escaped pattern
     */
    public static String escapeMetaDataPattern(String pattern) {
        if (pattern == null || pattern.length() == 0) {
            return pattern;
        }
        return StringUtils.replaceAll(pattern, "\\", "\\\\");
    }

    /**
     * Open a new database connection with the given settings.
     *
     * @param driver the driver class name
     * @param url the database URL
     * @param prop the properties containing at least the user name and password
     * @return the database connection
     */
    public static Connection getConnection(String driver, String url, Properties prop) throws SQLException {
        if (StringUtils.isNullOrEmpty(driver)) {
            JdbcDriverUtils.load(url);
        } else {
            Class< ? > d = ClassUtils.loadUserClass(driver);
            if (java.sql.Driver.class.isAssignableFrom(d)) {
                return DriverManager.getConnection(url, prop);
                //## Java 1.4 begin ##
            } else if (javax.naming.Context.class.isAssignableFrom(d)) {
                // JNDI context
                try {
                    Context context = (Context) d.newInstance();
                    DataSource ds = (DataSource) context.lookup(url);
                    String user = prop.getProperty("user");
                    String password = prop.getProperty("password");
                    if (StringUtils.isNullOrEmpty(user) && StringUtils.isNullOrEmpty(password)) {
                        return ds.getConnection();
                    }
                    return ds.getConnection(user, password);
                } catch (InstantiationException e) {
                    throw Message.convert(e);
                } catch (IllegalAccessException e) {
                    throw Message.convert(e);
                } catch (NamingException e) {
                    throw Message.convert(e);
                }
                //## Java 1.4 end ##
            } else {
                // Don't know, but maybe it loaded a JDBC Driver
                return DriverManager.getConnection(url, prop);
            }
        }
        return DriverManager.getConnection(url, prop);
    }

}
