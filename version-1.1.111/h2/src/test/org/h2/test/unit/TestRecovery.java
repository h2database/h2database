/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.tools.DeleteDbFiles;

/**
 * Tests database recovery.
 */
public class TestRecovery extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        DeleteDbFiles.execute(baseDir, "recovery", true);
        org.h2.Driver.load();
        String url = "jdbc:h2:" + baseDir + "/recovery;write_delay=0";
        Connection conn1 = DriverManager.getConnection(url, "sa", "sa");
        Statement stat1 = conn1.createStatement();
        Connection conn2 = DriverManager.getConnection(url, "sa", "sa");
        Statement stat2 = conn2.createStatement();
        stat1.execute("create table test as select * from system_range(1, 100)");
        stat1.execute("create table abc(id int)");
        conn2.setAutoCommit(false);
        // this is not committed
        // recovery might try to roll back this
        stat2.execute("delete from test");
        // overwrite the data of test
        stat1.execute("insert into abc select * from system_range(1, 100)");
        stat1.execute("shutdown immediately");
        // Recover.execute("data", null);
        Connection conn = DriverManager.getConnection(url, "sa", "sa");
        conn.close();
        DeleteDbFiles.execute(baseDir, "recovery", true);
    }

}
