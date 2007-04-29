/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.h2.tools.DeleteDbFiles;

public class TestWithRecursive {
    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        DeleteDbFiles.execute(".", "test", true);
        String url = "jdbc:h2:test";
        Connection conn = DriverManager.getConnection(url, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table parent(id int primary key, parent int)");
        stat.execute("insert into parent values(1, null), (2, 1), (3, 1)");
        ResultSet rs = stat.executeQuery(                
                "with test_view(id, parent) as \n"+
                "select id, parent from parent where id = 1 \n"+
                "union all select parent.id, parent.parent from test_view, parent \n"+
                "where parent.parent = test_view.id \n" +
                "select * from test_view");
        System.out.println("query:");
        while(rs.next()) {
            System.out.println(rs.getString(1));
        }
        stat.execute("drop view if exists test_view");
        System.out.println("prepared:");
        PreparedStatement prep = conn.prepareStatement(
                "with test_view(id, parent) as \n"+
                "select id, parent from parent where id = ? \n"+
                "union all select parent.id, parent.parent from test_view, parent \n"+
                "where parent.parent = test_view.id \n" +
                "select * from test_view");
        prep.setInt(1, 1);
        rs = prep.executeQuery();
        while(rs.next()) {
            System.out.println(rs.getString(1));
        }
        conn.close();
    }

}
