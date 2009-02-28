/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.test.TestBase;

/**
 * Test the serialized (server-less) mode.
 */
public class TestFileLockSerialized extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String[] a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws Exception {

        // TODO support long running queries

        deleteDb("fileLockSerialized");

        String url = "jdbc:h2:" + baseDir + "/fileLockSerialized";
        String writeUrl = url + ";FILE_LOCK=SERIALIZED;OPEN_NEW=TRUE";
        // ;TRACE_LEVEL_SYSTEM_OUT=3
        // String readUrl = writeUrl + ";ACCESS_MODE_LOG=R;ACCESS_MODE_DATA=R";

        trace("create database");
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection(writeUrl, "sa", "sa");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key)");

        Connection conn3 = DriverManager.getConnection(writeUrl, "sa", "sa");
        Statement stat3 = conn3.createStatement();

        Connection conn2 = DriverManager.getConnection(writeUrl, "sa", "sa");
        Statement stat2 = conn2.createStatement();
        printResult(stat2, "select * from test");

        stat2.execute("create local temporary table temp(name varchar)");
        printResult(stat2, "select * from temp");

        trace("insert row 1");
        stat.execute("insert into test values(1)");
        trace("insert row 2");
        stat3.execute("insert into test values(2)");
        printResult(stat2, "select * from test");
        printResult(stat2, "select * from temp");

        conn.close();
        conn2.close();
        conn3.close();
    }

    private void printResult(Statement stat, String sql) throws SQLException {
        trace("query: " + sql);
        ResultSet rs = stat.executeQuery(sql);
        int rowCount = 0;
        while (rs.next()) {
            trace("  " + rs.getString(1));
            rowCount++;
        }
        trace("  " + rowCount + " row(s)");
    }
}
