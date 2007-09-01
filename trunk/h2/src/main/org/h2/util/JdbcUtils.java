/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html). 
 * Initial Developer: H2 Group 
 */
package org.h2.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

//#ifdef JDK14
import javax.naming.Context;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.sql.XAConnection;
//#endif

import org.h2.constant.ErrorCode;
import org.h2.message.Message;

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
//#ifdef JDK14
        rs = stat.getGeneratedKeys();
//#endif
        return rs;
    }

//#ifdef JDK14
    public static void closeSilently(XAConnection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }
//#endif

    public static Connection getConnection(String driver, String url, String user, String password) throws SQLException {
        if (!StringUtils.isNullOrEmpty(driver)) {
            try {
                Class d = ClassUtils.loadClass(driver);
                if (java.sql.Driver.class.isAssignableFrom(d)) {
                    return DriverManager.getConnection(url, user, password);
                } else if (javax.naming.Context.class.isAssignableFrom(d)) {
                    // JNDI context
                    try {
                        Context context = (Context) d.newInstance();
                        DataSource ds = (DataSource) context.lookup(url);
                         return ds.getConnection(user, password);
                     } catch (InstantiationException e) {
                         throw Message.convert(e);
                     } catch (IllegalAccessException e) {
                         throw Message.convert(e);
                     } catch (NamingException e) {
                         throw Message.convert(e);
                     }
                 } else {
                    // Don't know, but maybe it loaded a JDBC Driver
                    return DriverManager.getConnection(url, user, password);
                 }                
            } catch (ClassNotFoundException e) {
                throw Message.getSQLException(ErrorCode.CLASS_NOT_FOUND_1, new String[]{driver}, e);
            }
        }
        return DriverManager.getConnection(url, user, password);
    }

}
