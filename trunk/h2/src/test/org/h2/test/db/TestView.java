/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.h2.test.TestBase;

/**
 * Test for views.
 */
public class TestView extends TestBase {

    public void test() throws Exception {
        testUnionReconnect();
        testManyViews();
    }
    
    private void testUnionReconnect() throws Exception {
        if (config.memory) {
            return;
        }
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("create table t1(k smallint, ts timestamp(6))");
        stat.execute("create table t2(k smallint, ts timestamp(6))");
        stat.execute("create table t3(k smallint, ts timestamp(6))");
        stat.execute("create view v_max_ts as select " + 
                "max(ts) from (select max(ts) as ts from t1 " + 
                "union select max(ts) as ts from t2 " + 
                "union select max(ts) as ts from t3)");
        stat.execute("create view v_test as select max(ts) as ts from t1 " +
                "union select max(ts) as ts from t2 " +
                "union select max(ts) as ts from t3");
        conn.close();
        conn = getConnection("view");
        stat = conn.createStatement();
        stat.execute("select * from v_max_ts");
        conn.close();
        deleteDb("view");
    }
    
    private void testManyViews() throws Exception {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement s = conn.createStatement();
        s.execute("create table t0(id int primary key)");
        s.execute("insert into t0 values(1), (2), (3)");
        for (int i = 0; i < 30; i++) {
            s.execute("create view t" + (i + 1) + " as select * from t" + i);
            s.execute("select * from t" + (i + 1));
            ResultSet rs = s.executeQuery("select count(*) from t" + (i + 1) + " where id=2");
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 1);
        }
        conn.close();
        conn = getConnection("view");
        conn.close();
        deleteDb("view");
    }
}
