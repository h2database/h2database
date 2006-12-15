/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.*;
public class TestDate {
    public static void main(String[] args) throws Exception {
        testWith("org.postgresql.Driver", "jdbc:postgresql:jpox2", "sa", "sa");
        testWith("com.mysql.jdbc.Driver", "jdbc:mysql://localhost/test", "sa", "sa");
        testWith("org.h2.Driver", "jdbc:h2:test", "sa", "sa");
        testWith("org.hsqldb.jdbcDriver", "jdbc:hsqldb:test", "sa", "");
    }
    static void testWith(String driver, String url, String user, String password) 
            throws Exception {
        Class.forName(driver);
        System.out.println("URL: " + url);
        Connection conn = DriverManager.getConnection(url, user, password);
        Statement stat = conn.createStatement();
        try {
            stat.execute("DROP TABLE ts_trial");
        } catch (SQLException e) { }
        stat.execute("CREATE TABLE ts_trial(myts TIMESTAMP)");
        PreparedStatement prep = conn.prepareStatement(
                    "INSERT INTO ts_trial(myts) VALUES (?)");
        prep.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis()));
        prep.execute();
        prep.setDate(1, new java.sql.Date(System.currentTimeMillis()));
        prep.execute();
        ResultSet rs = stat.executeQuery("SELECT myts FROM ts_trial");
        rs.next();
        System.out.println("Timestamp: " + rs.getTimestamp("myts"));
        rs.next();
        System.out.println("Date: " + rs.getTimestamp("myts"));
        System.out.println();
    }
}
