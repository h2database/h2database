/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement s = conn.createStatement();
        s.execute("create table t0(id int primary key)");
        s.execute("insert into t0 values(1), (2), (3)");
        for (int i = 0; i < 30; i++) {
            s.execute("create view t" + (i + 1) + " as select * from t" + i);
            s.execute("select * from t" + (i + 1));
            ResultSet rs = s.executeQuery("select count(*) from t" + (i + 1) + " where id=2");
            check(rs.next());
            check(rs.getInt(1), 1);
        }
        conn.close();
        conn = getConnection("view");
        conn.close();
        deleteDb("view");
    }
}
