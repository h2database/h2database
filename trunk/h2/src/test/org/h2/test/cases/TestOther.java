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
        Object[] tos = {
//            new int[] {1, 2, 3, 4, 5},
//          new Integer(122), // Uncomment to see a "Hexadecimal string with odd number of characters"
          new String[] {"hello", "world"}, // Uncomment to see a "Deserialization failed"
//          new Integer(12) // Will save it somehow, but fails to retrieve
        };

        try {
            Class.forName("org.h2.Driver");
            Connection con = DriverManager.getConnection("jdbc:h2:mem:", "sa", "");
            con.setAutoCommit(true);

            con.createStatement().executeUpdate("CREATE TABLE TestOtherJDBC (tstData OTHER)");
            System.out.println("table created");

            PreparedStatement stmt = con.prepareStatement("INSERT INTO TestOtherJDBC (tstData) VALUES (?)");

            for (int i = 0; i < tos.length; i++) {
                System.out.println(tos[i].getClass().getName() + "\t" + tos[i]);
                stmt.setObject(1, tos[i], Types.OTHER);
                stmt.executeUpdate();
            }
            System.out.println("inserted");

            ResultSet rs = con.createStatement().executeQuery("SELECT tstData FROM TestOtherJDBC");

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
