/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.*;
import java.util.Random;

import org.h2.tools.DeleteDbFiles;
import org.h2.util.MemoryUtils;

public class TestRebuildIndex {
    public static void main (String[] args) throws Exception {
        boolean init = false;
        if(init) {
            DeleteDbFiles.execute(".", "test", false);
        }
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:test;cache_type=lzf", "sa", "sa");
        Statement stat = conn.createStatement();
        if(init) {
            stat.executeUpdate("DROP TABLE IF EXISTS TEST");
            stat.executeUpdate("CREATE TABLE TEST(ID INT PRIMARY KEY, DATA INT)");
            PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
            int max = 1000000;
            Random random = new Random(10);
            for(int i=0; i<max; i++) {
                prep.setInt(1, i);
//                prep.setInt(2, max - i); 
                prep.setInt(2, random.nextInt());
                prep.execute();
            }
        } else {
            long time = System.currentTimeMillis();
            stat.execute("CREATE INDEX IDXDATA ON TEST(DATA)");
            time = System.currentTimeMillis() - time;
            System.out.println("time: " + time);
            // lzf: 5688 / 11944 kb
            // tq: 4875 ms / 12131 kb
            System.out.println("mem: " + MemoryUtils.getMemoryUsed());
        }
        conn.close();
    }
}
