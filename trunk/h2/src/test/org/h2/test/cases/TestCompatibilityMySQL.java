/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;
import java.sql.*;

public class TestCompatibilityMySQL {

    public static void main(String[] args) throws Exception {
        testWith("org.postgresql.Driver", "jdbc:postgresql:jpox2", "sa", "sa");
        testWith("com.mysql.jdbc.Driver", "jdbc:mysql://localhost/test", "sa", "sa");
        testWith("org.hsqldb.jdbcDriver", "jdbc:hsqldb:test", "sa", "");
        testWith("org.h2.Driver", "jdbc:h2:test", "sa", "sa");
    }
    static void testWith(String driver, String url, String user, String password) 
            throws Exception {
        Class.forName(driver);
        System.out.println("URL: " + url);
        Connection connection = DriverManager.getConnection(url, user, password);
//          Class.forName("com.mysql.jdbc.Driver");
//          Connection connection = DriverManager.getConnection( "jdbc:mysql://localhost/test", "sa", "sa");
        Statement statement = connection.createStatement();
        try {
            statement.execute("DROP TABLE PEOPLE");
        } catch(SQLException e) {}
        statement.execute("CREATE TABLE PEOPLE (ID BIGINT, NAME VARCHAR(32))");
        statement.execute("INSERT INTO PEOPLE (ID, NAME) VALUES (1, 'Adam')");
        ResultSet rs = statement.executeQuery("SELECT PEOPLE.NAME FROM PEOPLE");
        rs.next();
        assertEquals("Adam", rs.getString(1));
        assertEquals("Adam", rs.getString("NAME"));
        rs = statement.executeQuery("SELECT PEOPLE.NAME FROM PEOPLE");
        rs.next();
//        assertEquals( "Adam", rs.getString("PEOPLE.NAME"));
        statement.executeQuery("SELECT PEOPLE.NAME FROM PEOPLE");
        statement.executeQuery("SELECT PEOPLE.NAME FROM PEOPLE;");
        statement.executeQuery("SELECT PEOPLE.NAME FROM PEOPLE /* comment */");
//        try {
//            connection.createStatement().executeQuery("SELECT PEOPLE.NAME FROM PEOPLE; /* comment */");
//            throw new Error("error");
//        } catch(SQLException e) {
//            // expected
//        }
        connection.createStatement().executeQuery("SELECT PEOPLE.NAME FROM PEOPLE /* comment */ ;");
    }

    private static void assertEquals(String a, String b) {
        if(!a.equals(b)) {
            throw new Error("a=" + a + " b="+b);
        }
    }
}
