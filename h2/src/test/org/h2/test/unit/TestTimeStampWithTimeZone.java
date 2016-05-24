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
import java.sql.Timestamp;

import org.h2.test.TestBase;

/**
 */
public class TestTimeStampWithTimeZone extends TestBase {

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
        deleteDb(getTestName());
        test1();
        deleteDb(getTestName());
    }

    private void test1() throws SQLException {
        Connection conn = getConnection(getTestName());
        Statement stat = conn.createStatement();
        stat.execute("create table test(id identity, t1 timestamp with timezone)");
        stat.execute("insert into test(t1) values('1970-01-01 12:00:00.00+00:15')");
        ResultSet rs = stat.executeQuery("select t1 from test");
        rs.next();
        assertEquals("1970-01-01 12:00:00.0+00:15", rs.getString(1));
        Timestamp ts = rs.getTimestamp(1);
        // TODO currently fails:
        //assertTrue("" + ts,
        //        new TimestampWithTimeZone(36000000, 00, (short) 15).equals(
        //                ts));
        conn.close();
    }

}
