/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.test.TestBase;

/**
 */
public class TestTimeStampUtc extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        deleteDb("timestamp_utc");
        test1();
        deleteDb("timestamp_utc");
    }

    private void test1() throws SQLException {
        Connection conn = getConnection("timestamp_utc");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id identity, t1 timestamp_utc)");
        stat.execute("insert into test(t1) values(0)");
        ResultSet rs = stat.executeQuery("select t1 from test");
        rs.next();
        assertEquals(0, rs.getLong(1));
        assertEquals(new java.sql.Timestamp(0), rs.getTimestamp(1));
        conn.close();
    }

}
