package org.h2.test.cases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class TestMemoryDb {
    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        String url = "jdbc:h2:mem:EventDB";
        String user = "sa";
        String password = "sa";
        Connection conn1 = DriverManager.getConnection(url, user, password);
        Statement stat1 = conn1.createStatement();
        stat1.execute("create table test(id int)");
        Connection conn2 = DriverManager.getConnection(url, user, password);
        Statement stat2 = conn2.createStatement();
        stat2.executeQuery("select * from test");
        conn1.close();
        conn2.close();
    }
}
