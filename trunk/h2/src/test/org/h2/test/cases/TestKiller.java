/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Destroyer application ;->
 * 
 * @author Maciej Wegorkiewicz
 */
public class TestKiller {
    
    public static void main(String[] args) throws Exception {
        testWith("org.h2.Driver", "jdbc:h2:test", "sa", "sa");
        testWith("org.postgresql.Driver", "jdbc:postgresql:jpox2", "sa", "sa");
        testWith("com.mysql.jdbc.Driver", "jdbc:mysql://localhost/test", "sa", "sa");
        testWith("org.hsqldb.jdbcDriver", "jdbc:hsqldb:test", "sa", "");
        testWith("org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:test3;create=true", "sa", "sa");
    }
    
    public static class TestBigDecimal extends BigDecimal {
        public TestBigDecimal(String s) {
            super(s);
        }
        private static final long serialVersionUID = 6309132662083006892L;
        public int compareTo(BigDecimal obj) {
            return -super.compareTo(obj);
        }
        public String toString() {
            throw new NullPointerException();
        }
    }
    
    static void testWith(String driver, String url, String user, String password) 
            throws Exception {
        Class.forName(driver);
        System.out.println("URL: " + url);
        Connection conn = DriverManager.getConnection(url, user, password);

        try {
            Statement stmt = conn.createStatement();
            try {
                conn.createStatement().execute("DROP TABLE TESTTAB");
            } catch(SQLException e) {
                // ignore
            }
            stmt.execute("CREATE TABLE TESTTAB(VL DECIMAL(16,6) PRIMARY KEY)");
            stmt.close();
            PreparedStatement prep = conn.prepareStatement("INSERT INTO TESTTAB VALUES (?)");
            prep.setBigDecimal(1, new TestBigDecimal("1"));
            prep.execute();
            prep.setBigDecimal(1, new TestBigDecimal("2"));
            prep.execute();
            prep.setBigDecimal(1, new TestBigDecimal("3"));
            prep.execute();
            prep.close();
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TESTTAB ORDER BY VL");
            while(rs.next()) {
                System.out.println("VL:" + rs.getString("VL"));
            }            
            try {
                conn.createStatement().executeQuery("SHUTDOWN");
            } catch(SQLException e) {
                // ignore
            }
            conn.close();
            Class.forName(driver);
            
            conn = DriverManager.getConnection(url, user, password);
            rs = conn.createStatement().executeQuery("SELECT * FROM TESTTAB ORDER BY VL");
            while(rs.next()) {
                System.out.println("VL:" + rs.getString("VL"));
            }
            conn.close();
        } catch(Throwable t) {
            t.printStackTrace();
        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }
}
