/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.*;

public class TestObject {
    public static void main(String[] args) throws Exception {
        testWith("org.hsqldb.jdbcDriver", "jdbc:hsqldb:test", "sa", "");
//        testWith("org.postgresql.Driver", "jdbc:postgresql:jpox2", "sa", "sa");
//        testWith("com.mysql.jdbc.Driver", "jdbc:mysql://localhost/test", "sa", "sa");
        testWith("org.h2.Driver", "jdbc:h2:test", "sa", "sa");
    }

    static void testWith(String driver, String url, String user, String password) throws Exception {
        Class.forName(driver);
        System.out.println("URL: " + url);
        Connection conn = DriverManager.getConnection(url, user, password);
        conn.setAutoCommit(true);
        Statement st = conn.createStatement();

        try {
            st.executeUpdate("DROP TABLE test_object_table");
        } catch (SQLException e) {
            // ignore
        }
//        st.executeUpdate("CREATE TABLE test_object_table(id INTEGER NOT NULL, object0 JAVA_OBJECT NOT NULL, PRIMARY KEY(id))");
        st.executeUpdate("CREATE TABLE test_object_table(id INTEGER NOT NULL, object0 OBJECT NOT NULL, PRIMARY KEY(id))");
//        st.executeUpdate("CREATE TABLE test_object_table(id INTEGER NOT NULL, object0 OID NOT NULL, PRIMARY KEY(id))");

        PreparedStatement ps = conn.prepareStatement("INSERT INTO test_object_table values(?, ?)");
        ps.setInt(1, 1);
        ps.setObject(2, new Integer(3));
        ps.executeUpdate();

        ResultSet rs = st.executeQuery("select * from test_object_table");
        while (rs.next()) {
            int id = rs.getInt("id");
            Object object0 = rs.getObject("object0");
            System.out.println("id = " + id + ", object0 = " + object0.getClass().getName() + " / " + object0);
        }
        rs.close();

        conn.close();
    }
}
