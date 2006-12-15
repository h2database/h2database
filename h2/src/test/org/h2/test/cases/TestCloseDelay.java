/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.*;
import java.util.Random;

public class TestCloseDelay {

    private Connection conn;
    
    public static void main(String[] args) throws Exception {
        new TestCloseDelay().test();
        new TestCloseDelay().test();
    }

    public void test() throws Exception {
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection("jdbc:h2:test;DB_CLOSE_DELAY=-1");
        Statement stat;
        stat = conn.createStatement();
        conn.setAutoCommit(false);
        stat.execute("DROP TABLE TEST IF EXISTS");
        stat.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
        Random random = new Random();
        for(int j=0; j<100; j++) {
            System.out.println("j:" + j);
            for(int i=0; i<1000; i++) {
                switch(random.nextInt(10)) {
                case 0:
                    conn.close();
                    conn = DriverManager.getConnection("jdbc:h2:test;DB_CLOSE_DELAY=-1");
                    stat = conn.createStatement();
                    conn.setAutoCommit(false);
                    break;
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                    stat.execute("INSERT INTO TEST(NAME) VALUES('Hello')");
                    break;
                case 7:
                case 8:
                case 9:
                    stat.execute("UPDATE TEST SET NAME='Hello World' WHERE ROWNUM < 10");
                    break;
                }
                if((i% 100) == 0) {
                    ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
                    while(rs.next()) {
                        rs.getInt(1);
                        rs.getString(2);
                    }
                }
            }
            stat.execute("SHUTDOWN");
            conn.close();
            conn = DriverManager.getConnection("jdbc:h2:test;DB_CLOSE_DELAY=-1");
            stat = conn.createStatement();
            ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
            while(rs.next()) {
                rs.getInt(1);
                rs.getString(2);
            }
        }           
    }
    
}
