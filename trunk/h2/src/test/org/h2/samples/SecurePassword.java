/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author Thomas
 */

public class SecurePassword {
    public static void main(String[] argv) throws Exception {
        
        Class.forName("org.h2.Driver");
        String url = "jdbc:h2:data/simple";
        String user = "sam";
        char[] password = {'t', 'i', 'a', 'E', 'T', 'r', 'p'};
        
        // 'unsafe' way to connect
        // the password may reside in the main memory for an undefined time
        // or even written to disk (swap file)
        // Connection conn = DriverManager.getConnection(url, user, new String(password));
        
        // 'safe' way to connect
        // the password is overwritten after use
        Properties prop = new Properties();
        prop.setProperty("user", user);
        prop.put("password", password);
        Connection conn = DriverManager.getConnection(url, prop);
        
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.executeUpdate("INSERT INTO TEST VALUES(1, 'Hello')");
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        while(rs.next()) {
            System.out.println(rs.getString("NAME"));
        }
        conn.close();
    }
        
}
