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
import org.h2.api.TimestampWithTimeZone;
import org.h2.test.TestBase;
import org.h2.util.LocalDateTimeUtils;
import org.h2.value.ValueTimestampTimeZone;

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
        test2();
        test3();
        test4();
        testOrder();
        deleteDb(getTestName());
    }

    private void test1() throws SQLException {
        Connection conn = getConnection(getTestName());
        Statement stat = conn.createStatement();
        stat.execute("create table test(id identity, t1 timestamp with timezone)");
        stat.execute("insert into test(t1) values('1970-01-01 12:00:00.00+00:15')");
        // verify NanosSinceMidnight is in local time and not UTC
        stat.execute("insert into test(t1) values('2016-09-24 00:00:00.000000001+00:01')");
        stat.execute("insert into test(t1) values('2016-09-24 00:00:00.000000001-00:01')");
        // verify year month day is in local time and not UTC
        stat.execute("insert into test(t1) values('2016-01-01 05:00:00.00+10:00')");
        stat.execute("insert into test(t1) values('2015-12-31 19:00:00.00-10:00')");
        ResultSet rs = stat.executeQuery("select t1 from test");
        rs.next();
        assertEquals("1970-01-01 12:00:00.0+00:15", rs.getString(1));
        TimestampWithTimeZone ts = (TimestampWithTimeZone) rs.getObject(1);
        assertEquals(1970, ts.getYear());
        assertEquals(1, ts.getMonth());
        assertEquals(1, ts.getDay());
        assertEquals(15, ts.getTimeZoneOffsetMins());
        assertEquals(new TimestampWithTimeZone(1008673L, 43200000000000L, (short) 15), ts);
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("1970-01-01T12:00+00:15", rs.getObject(1,
                            LocalDateTimeUtils.getOffsetDateTimeClass()).toString());
        }
        rs.next();
        ts = (TimestampWithTimeZone) rs.getObject(1);
        assertEquals(2016, ts.getYear());
        assertEquals(9, ts.getMonth());
        assertEquals(24, ts.getDay());
        assertEquals(1, ts.getTimeZoneOffsetMins());
        assertEquals(1L, ts.getNanosSinceMidnight());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("2016-09-24T00:00:00.000000001+00:01", rs.getObject(1,
                            LocalDateTimeUtils.getOffsetDateTimeClass()).toString());
        }
        rs.next();
        ts = (TimestampWithTimeZone) rs.getObject(1);
        assertEquals(2016, ts.getYear());
        assertEquals(9, ts.getMonth());
        assertEquals(24, ts.getDay());
        assertEquals(-1, ts.getTimeZoneOffsetMins());
        assertEquals(1L, ts.getNanosSinceMidnight());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("2016-09-24T00:00:00.000000001-00:01", rs.getObject(1,
                            LocalDateTimeUtils.getOffsetDateTimeClass()).toString());
        }
        rs.next();
        ts = (TimestampWithTimeZone) rs.getObject(1);
        assertEquals(2016, ts.getYear());
        assertEquals(1, ts.getMonth());
        assertEquals(1, ts.getDay());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("2016-01-01T05:00+10:00", rs.getObject(1,
                            LocalDateTimeUtils.getOffsetDateTimeClass()).toString());
        }
        rs.next();
        ts = (TimestampWithTimeZone) rs.getObject(1);
        assertEquals(2015, ts.getYear());
        assertEquals(12, ts.getMonth());
        assertEquals(31, ts.getDay());
        if (LocalDateTimeUtils.isJava8DateApiPresent()) {
            assertEquals("2015-12-31T19:00-10:00", rs.getObject(1,
                            LocalDateTimeUtils.getOffsetDateTimeClass()).toString());
        }
        rs.close();
        stat.close();
        conn.close();
    }

    private void test2() {
        ValueTimestampTimeZone a = ValueTimestampTimeZone.parse("1970-01-01 12:00:00.00+00:15");
        ValueTimestampTimeZone b = ValueTimestampTimeZone.parse("1970-01-01 12:00:01.00+01:15");
        int c = a.compareTo(b, null);
        assertEquals(c, 1);
    }

    private void test3() {
        ValueTimestampTimeZone a = ValueTimestampTimeZone.parse("1970-01-02 00:00:02.00+01:15");
        ValueTimestampTimeZone b = ValueTimestampTimeZone.parse("1970-01-01 23:00:01.00+00:15");
        int c = a.compareTo(b, null);
        assertEquals(c, 1);
    }

    private void test4() {
        ValueTimestampTimeZone a = ValueTimestampTimeZone.parse("1970-01-02 00:00:01.00+01:15");
        ValueTimestampTimeZone b = ValueTimestampTimeZone.parse("1970-01-01 23:00:01.00+00:15");
        int c = a.compareTo(b, null);
        assertEquals(c, 0);
    }

    private void testOrder() throws SQLException {
        Connection conn = getConnection(getTestName());
        Statement stat = conn.createStatement();
        stat.execute("create table test_order(id identity, t1 timestamp with timezone)");
        stat.execute("insert into test_order(t1) values('1970-01-01 12:00:00.00+00:15')");
        stat.execute("insert into test_order(t1) values('1970-01-01 12:00:01.00+01:15')");
        ResultSet rs = stat.executeQuery("select t1 from test_order order by t1");
        rs.next();
        assertEquals("1970-01-01 12:00:01.0+01:15", rs.getString(1));
        conn.close();
    }

}
