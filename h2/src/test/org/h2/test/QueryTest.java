package org.h2.test;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;

public class QueryTest {

    static String dbUrl = "jdbc:h2:~/db1";

    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection(dbUrl, "sa", "");
        InputStream is = QueryTest.class.getResourceAsStream("query.txt");
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        String sql = s.next();
        long time1 = System.nanoTime();
        conn.prepareStatement(sql);
        long time2 = System.nanoTime();
        System.out.println("time = " + (time2-time1)/1000/1000);
    }

}
