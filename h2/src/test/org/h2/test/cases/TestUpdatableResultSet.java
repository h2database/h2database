/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.*;

class TestUpdatableResultSet {

    Connection conn;

    public static void main(String[] args) throws Exception {
        new TestUpdatableResultSet().test();
    }

    int insertStudentUsingResultSet(String name) throws Exception {
        Statement stmt = conn.createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM students FOR UPDATE");
//        rs.moveToInsertRow();
//        rs.updateString("name", name);
//        rs.insertRow();
        int id = 0;
        try {
            rs.last();
            id = rs.getInt("student_id");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }

    void test() throws Exception {
         Class.forName("org.h2.Driver");
         conn = DriverManager.getConnection("jdbc:h2:test", "sa", "sa");
//        Class.forName("org.postgresql.Driver");
//        conn = DriverManager.getConnection("jdbc:postgresql:jpox2", "sa", "sa");
//      Class.forName("com.mysql.jdbc.Driver");
//      conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "sa", "sa");

        try {
            conn.createStatement().executeUpdate("DROP TABLE students");
        } catch (SQLException e) {
            // 
        }
//        conn.createStatement().executeUpdate("CREATE TABLE students (student_id int PRIMARY KEY, name char(20))");
        conn.createStatement().executeUpdate("CREATE TABLE students (student_id int AUTO_INCREMENT PRIMARY KEY, name char(20))");
        System.out.println("student id: " + insertStudentUsingResultSet("name-1")); // output must be 1,it will print 0,
        System.out.println("student id: " + insertStudentUsingResultSet("name-2")); // output must be 2, it will print 1
        conn.close();
    }
}
