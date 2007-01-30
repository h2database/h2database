/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;

public class TestOther {
    public static void main(String[] args) {
        Object[] list = {
//            new int[] {1, 2, 3, 4, 5},
//          new Integer(122), // Uncomment to see a "Hexadecimal string with odd number of characters"
          new String[] {"hello", "world"}, // Uncomment to see a "Deserialization failed"
//          new Integer(12) // Will save it somehow, but fails to retrieve
        };

        try {
            Class.forName("org.h2.Driver");
            Connection conn = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
            conn.setAutoCommit(true);

            conn.createStatement().executeUpdate("CREATE TABLE TestOtherJDBC (testData OTHER)");
            System.out.println("table created");

            PreparedStatement stmt = conn.prepareStatement("INSERT INTO TestOtherJDBC (testData) VALUES (?)");

            for (int i = 0; i < list.length; i++) {
                System.out.println(list[i].getClass().getName() + "\t" + list[i]);
                stmt.setObject(1, list[i], Types.OTHER);
                stmt.executeUpdate();
            }
            System.out.println("inserted");

            ResultSet rs = conn.createStatement().executeQuery("SELECT testData FROM TestOtherJDBC");

            while(rs.next()) {
                Object obj = rs.getObject(1);
                System.out.println(obj.getClass().getName() + "\t" + obj);
            }
            rs.close();
        } catch (Throwable e) {
            e.printStackTrace();
        }

    }
}
