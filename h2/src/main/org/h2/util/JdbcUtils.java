/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html). 
 * Initial Developer: H2 Group 
 */
package org.h2.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcUtils {
    
    public static void closeSilently(Statement stat) {
        if(stat != null) {
            try {
                stat.close();
            } catch(SQLException e) {
                // ignore
            }
        }
    }

    public static void closeSilently(Connection conn) {
        if(conn != null) {
            try {
                conn.close();
            } catch(SQLException e) {
                // ignore
            }
        }
    }

    public static void closeSilently(ResultSet rs) {
        if(rs != null) {
            try {
                rs.close();
            } catch(SQLException e) {
                // ignore
            }
        }
    }

}
