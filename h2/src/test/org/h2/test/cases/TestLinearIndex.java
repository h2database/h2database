/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.cases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Random;

import org.h2.tools.DeleteDbFiles;
import org.h2.util.MemoryUtils;

public class TestLinearIndex {
    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        // stat.execute("create unique index idxid on test(id)");
        int len = 1000;
        for(int a=0; ; a++) {
            testLoop(true, len);
            testLoop(false, len);
            len += 1000;
        }
        // hash: 23156
        // btree: 10250
    }
    
    private static void testLoop(boolean hashIndex, int len) throws Exception {
        DeleteDbFiles.execute(".", "test", true);
        String url = "jdbc:h2:test";
        Connection conn = DriverManager.getConnection(url, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(id int, name varchar)");
        if(hashIndex) {
            stat.execute("create unique hash index idxid on test(id)");
        } else {
            stat.execute("create unique index idxid on test(id)");
        }
        stat.execute("insert into test select x, 'Hello World' from system_range(1, "+len+")");
        PreparedStatement prep = conn.prepareStatement("select * from test where id=?");
        Random random = new Random(1);
        long time = System.currentTimeMillis();
        for(int i=0; i<len*50; i++) {
            prep.setInt(1, random.nextInt(len));
            prep.executeQuery();
        }
        time = System.currentTimeMillis() - time;
        System.out.println("len: " + len + " hash: " + hashIndex + " time: " + time+" used: " + MemoryUtils.getMemoryUsed());
        conn.close();
    }

}
