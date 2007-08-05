/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.h2.test.TestBase;

public class TestSequence extends TestBase {

    public void test() throws Exception {
        deleteDb("sequence");
        Connection conn=getConnection("sequence");
        Statement stat = conn.createStatement();
        stat.execute("create sequence testSequence");
        conn.setAutoCommit(false);

        Connection conn2=getConnection("sequence");
        Statement stat2 = conn2.createStatement();
        conn2.setAutoCommit(false);
        
        long last = 0;
        for(int i=0; i<100; i++) {
            long v1 = getNext(stat);
            check(v1 > last);
            last = v1;
            for(int j=0; j<100; j++) {
                long v2 = getNext(stat2);
                check(v2 > last);
                last = v2;
            }
        }
        
        conn2.close();
        conn.close();
    }

    private long getNext(Statement stat) throws Exception {
        ResultSet rs = stat.executeQuery("call next value for testSequence");
        rs.next();
        long value = rs.getLong(1);
        return value;
    }
}
