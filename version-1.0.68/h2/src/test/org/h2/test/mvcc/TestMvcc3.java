/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (license2)
 * Initial Developer: H2 Group
 */
package org.h2.test.mvcc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.h2.test.TestBase;

/**
 * Additional MVCC (multi version concurrency) test cases.
 */
public class TestMvcc3 extends TestBase {

    public void test() throws Exception {
        if (!config.mvcc) {
            return;
        }
        deleteDb("mvcc3");
        Connection conn = getConnection("mvcc3");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY)");
        stat.execute("INSERT INTO TEST VALUES(0)");
        conn.setAutoCommit(false);
        stat.execute("INSERT INTO TEST VALUES(1)");
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        check(0, rs.getInt(1));
        rs.next();
        check(1, rs.getInt(1));
        conn.close();
    }
}




