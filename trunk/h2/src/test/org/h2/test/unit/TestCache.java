/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Random;

import org.h2.test.TestBase;

public class TestCache extends TestBase {
    
    public void test() throws Exception {
        if(config.memory) {
            return;
        }
        deleteDb("cache");
        Connection conn = getConnection("cache");
        Statement stat = conn.createStatement();
        stat.execute("SET CACHE_SIZE 1024");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("CREATE TABLE MAIN(ID INT PRIMARY KEY, NAME VARCHAR)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        PreparedStatement prep2 = conn.prepareStatement("INSERT INTO MAIN VALUES(?, ?)");
        int max = 10000;
        for(int i=0; i<max; i++) {
            prep.setInt(1, i);
            prep.setString(2, "Hello " + i);
            prep.execute();
            prep2.setInt(1, i);
            prep2.setString(2, "World " + i);
            prep2.execute();
        }
        conn.close();
        conn = getConnection("cache");
        stat = conn.createStatement();
        stat.execute("SET CACHE_SIZE 1024");
        Random random = new Random(1);
        for(int i=0; i<100; i++) {
            stat.executeQuery("SELECT * FROM MAIN WHERE ID BETWEEN 40 AND 50");
            stat.executeQuery("SELECT * FROM MAIN WHERE ID = " + random.nextInt(max));
            if((i%10) == 0) {
                stat.executeQuery("SELECT * FROM TEST");
            }
        }
        conn.close();
    }

}
