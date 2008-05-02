/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
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

    public static void closeSilently(Statement stat) {
        if (stat != null) {
            try {
                stat.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    public static void closeSilently(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    public static void closeSilently(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    public static ResultSet getGeneratedKeys(Statement stat) throws SQLException {
        ResultSet rs = null;
        //## Java 1.4 begin ##
        rs = stat.getGeneratedKeys();
        //## Java 1.4 end ##
        return rs;
    }

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
     * Create a new database connection with the given settings.
     * 
     * @param driver the driver class name
     * @param url the database URL
     * @param user the user name
     * @param password the password
     * @return the database connection
     */
    public static Connection getConnection(String driver, String url, String user, String password) throws SQLException {
        Properties prop = new Properties();
        prop.setProperty("user", user);
        prop.setProperty("password", password);
        return getConnection(driver, url, prop);
    }

    public static Connection getConnection(String driver, String url, Properties prop) throws SQLException {
        if (StringUtils.isNullOrEmpty(driver)) {
            JdbcDriverUtils.load(url);
        } else {
            Class d = ClassUtils.loadUserClass(driver);
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
                    } else {
                        return ds.getConnection(user, password);
                    }
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
