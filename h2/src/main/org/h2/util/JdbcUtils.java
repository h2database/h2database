package org.h2.util;

import java.sql.Connection;
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

}
