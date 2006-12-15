/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.*;
import java.util.Random;

/*
del *.db
java -Xrunhprof:cpu=samples org.h2.test.cases.TestConnect
java org.h2.test.cases.TestConnect
 */
public class TestConnect {
    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        long time = System.currentTimeMillis();
        String url = "jdbc:h2:test;LOG=2;STORAGE=TEXT;DATABASE_EVENT_LISTENER='org.h2.samples.ShowProgress'";
        Connection conn = DriverManager.getConnection(url, "sa", "sa");
        time = System.currentTimeMillis() - time;
        System.out.println("connected in " + time);
        ResultSet rs = conn.getMetaData().getTables(null, null, "TEST", null);
        Random random = new Random(1);
        if(!rs.next()) {
            conn.createStatement().execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
            
            PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(NAME) VALUES(space(2000))");
//            PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST(NAME) VALUES(space(20000))");
            int len = 5000;
            for(int i=0; i<len; i++) {
                long t2 = System.currentTimeMillis();
                if(t2 - time > 1000) {
                    System.out.println("Inserting " + i + " of " + len);
                    time = t2;
                }
                prep.executeUpdate();
            }
            prep = conn.prepareStatement("UPDATE TEST SET NAME = space(2000) WHERE ID=?");
            for(int i=0; i<len; i++) {
                long t2 = System.currentTimeMillis();
                if(t2 - time > 1000) {
                    System.out.println("Updating " + i + " of " + len);
                    time = t2;
                }
                prep.setInt(1, random.nextInt(len));
                prep.executeUpdate();
            }
            // @LOOP 50000 UPDATE TEST SET NAME=space(20000) WHERE ID=?/*RND*/;
            Thread.sleep(2000);
            Runtime.getRuntime().halt(0);
        }
        conn.close();
    }
    
}
