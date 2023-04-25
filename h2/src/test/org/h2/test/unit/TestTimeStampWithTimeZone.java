/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.TimeZone;

import org.h2.engine.CastDataProvider;
import org.h2.test.TestBase;
import org.h2.test.TestDb;
import org.h2.util.DateTimeUtils;
import org.h2.util.JSR310Utils;
import org.h2.util.LegacyDateTimeUtils;
import org.h2.util.TimeZoneProvider;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 */
public class TestTimeStampWithTimeZone extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws SQLException {
        deleteDb(getTestName());
        test1();
        test2();
        test3();
        test4();
        test5();
        testOrder();
        testConversions();
        deleteDb(getTestName());
    }

    private void test1() throws SQLException {
        Connection conn = getConnection(getTestName());
        Statement stat = conn.createStatement();
        stat.execute("create table test(id identity, t1 timestamp(9) with time zone)");
        stat.execute("insert into test(t1) values('1970-01-01 12:00:00.00+00:15')");
        // verify NanosSinceMidnight is in local time and not UTC
        stat.execute("insert into test(t1) values('2016-09-24 00:00:00.000000001+00:01')");
        stat.execute("insert into test(t1) values('2016-09-24 00:00:00.000000001-00:01')");
        // verify year month day is in local time and not UTC
        stat.execute("insert into test(t1) values('2016-01-01 05:00:00.00+10:00')");
        stat.execute("insert into test(t1) values('2015-12-31 19:00:00.00-10:00')");
        ResultSet rs = stat.executeQuery("select t1 from test");
        rs.next();
        assertEquals("1970-01-01 12:00:00+00:15", rs.getString(1));
        OffsetDateTime ts = (OffsetDateTime) rs.getObject(1);
        assertEquals(1970, ts.getYear());
        assertEquals(1, ts.getMonthValue());
        assertEquals(1, ts.getDayOfMonth());
        assertEquals(15 * 60, ts.getOffset().getTotalSeconds());
        OffsetDateTime expected = OffsetDateTime.parse("1970-01-01T12:00+00:15");
        assertEquals(expected, ts);
        assertEquals("1970-01-01T12:00+00:15", rs.getObject(1, OffsetDateTime.class).toString());
        rs.next();
        ts = (OffsetDateTime) rs.getObject(1);
        assertEquals(2016, ts.getYear());
        assertEquals(9, ts.getMonthValue());
        assertEquals(24, ts.getDayOfMonth());
        assertEquals(1L, ts.toLocalTime().toNanoOfDay());
        assertEquals(60, ts.getOffset().getTotalSeconds());
        assertEquals("2016-09-24T00:00:00.000000001+00:01", rs.getObject(1, OffsetDateTime.class).toString());
        rs.next();
        ts = (OffsetDateTime) rs.getObject(1);
        assertEquals(2016, ts.getYear());
        assertEquals(9, ts.getMonthValue());
        assertEquals(24, ts.getDayOfMonth());
        assertEquals(1L, ts.toLocalTime().toNanoOfDay());
        assertEquals(-60, ts.getOffset().getTotalSeconds());
        assertEquals("2016-09-24T00:00:00.000000001-00:01", rs.getObject(1, OffsetDateTime.class).toString());
        rs.next();
        ts = (OffsetDateTime) rs.getObject(1);
        assertEquals(2016, ts.getYear());
        assertEquals(1, ts.getMonthValue());
        assertEquals(1, ts.getDayOfMonth());
        assertEquals("2016-01-01T05:00+10:00", rs.getObject(1, OffsetDateTime.class).toString());
        rs.next();
        ts = (OffsetDateTime) rs.getObject(1);
        assertEquals(2015, ts.getYear());
        assertEquals(12, ts.getMonthValue());
        assertEquals(31, ts.getDayOfMonth());
        assertEquals("2015-12-31T19:00-10:00", rs.getObject(1, OffsetDateTime.class).toString());

        ResultSetMetaData metaData = rs.getMetaData();
        int columnType = metaData.getColumnType(1);
        assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, columnType);
        assertEquals("java.time.OffsetDateTime", metaData.getColumnClassName(1));

        rs.close();

        rs = stat.executeQuery("select cast(t1 as varchar) from test");
        assertTrue(rs.next());
        assertEquals(expected, rs.getObject(1, OffsetDateTime.class));

        stat.close();
        conn.close();
    }

    private void test2() {
        ValueTimestampTimeZone a = ValueTimestampTimeZone.parse("1970-01-01 12:00:00.00+00:15", null);
        ValueTimestampTimeZone b = ValueTimestampTimeZone.parse("1970-01-01 12:00:01.00+01:15", null);
        int c = a.compareTo(b, null, null);
        assertEquals(1, c);
        c = b.compareTo(a, null, null);
        assertEquals(-1, c);
    }

    private void test3() {
        ValueTimestampTimeZone a = ValueTimestampTimeZone.parse("1970-01-02 00:00:02.00+01:15", null);
        ValueTimestampTimeZone b = ValueTimestampTimeZone.parse("1970-01-01 23:00:01.00+00:15", null);
        int c = a.compareTo(b, null, null);
        assertEquals(1, c);
        c = b.compareTo(a, null, null);
        assertEquals(-1, c);
    }

    private void test4() {
        ValueTimestampTimeZone a = ValueTimestampTimeZone.parse("1970-01-02 00:00:01.00+01:15", null);
        ValueTimestampTimeZone b = ValueTimestampTimeZone.parse("1970-01-01 23:00:01.00+00:15", null);
        int c = a.compareTo(b, null, null);
        assertEquals(0, c);
        c = b.compareTo(a, null, null);
        assertEquals(0, c);
    }

    private void test5() throws SQLException {
        Connection conn = getConnection(getTestName());
        Statement stat = conn.createStatement();
        stat.execute("create table test5(id identity, t1 timestamp with time zone)");
        stat.execute("insert into test5(t1) values('2016-09-24 00:00:00.000000001+00:01')");
        stat.execute("insert into test5(t1) values('2017-04-20 00:00:00.000000001+00:01')");

        PreparedStatement preparedStatement = conn.prepareStatement("select id"
                        + " from test5"
                        + " where (t1 < ?)");
        Value value = ValueTimestampTimeZone.parse("2016-12-24 00:00:00.000000001+00:01", null);
        preparedStatement.setObject(1, JSR310Utils.valueToOffsetDateTime(value, null));

        ResultSet rs = preparedStatement.executeQuery();

        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        rs.close();
        preparedStatement.close();
        stat.close();
        conn.close();
    }

    private void testOrder() throws SQLException {
        Connection conn = getConnection(getTestName());
        Statement stat = conn.createStatement();
        stat.execute("create table test_order(id identity, t1 timestamp with time zone)");
        stat.execute("insert into test_order(t1) values('1970-01-01 12:00:00.00+00:15')");
        stat.execute("insert into test_order(t1) values('1970-01-01 12:00:01.00+01:15')");
        ResultSet rs = stat.executeQuery("select t1 from test_order order by t1");
        rs.next();
        assertEquals("1970-01-01 12:00:01+01:15", rs.getString(1));
        conn.close();
    }

    private void testConversionsImpl(String timeStr, boolean testReverse, CastDataProvider provider) {
        ValueTimestamp ts = ValueTimestamp.parse(timeStr, null);
        ValueDate d = ts.convertToDate(provider);
        ValueTime t = (ValueTime) ts.convertTo(TypeInfo.TYPE_TIME, provider);
        ValueTimestampTimeZone tstz = ValueTimestampTimeZone.parse(timeStr, null);
        assertEquals(ts, tstz.convertTo(TypeInfo.TYPE_TIMESTAMP, provider));
        assertEquals(d, tstz.convertToDate(provider));
        assertEquals(t, tstz.convertTo(TypeInfo.TYPE_TIME, provider));
        assertEquals(LegacyDateTimeUtils.toTimestamp(provider, null, ts),
                LegacyDateTimeUtils.toTimestamp(provider, null, tstz));
        if (testReverse) {
            assertEquals(0, tstz.compareTo(ts.convertTo(TypeInfo.TYPE_TIMESTAMP_TZ, provider), null, null));
            assertEquals(d.convertTo(TypeInfo.TYPE_TIMESTAMP, provider)
                    .convertTo(TypeInfo.TYPE_TIMESTAMP_TZ, provider),
                    d.convertTo(TypeInfo.TYPE_TIMESTAMP_TZ, provider));
            assertEquals(t.convertTo(TypeInfo.TYPE_TIMESTAMP, provider)
                    .convertTo(TypeInfo.TYPE_TIMESTAMP_TZ, provider),
                    t.convertTo(TypeInfo.TYPE_TIMESTAMP_TZ, provider));
        }
    }

    private void testConversions() {
        TestDate.SimpleCastDataProvider provider = new TestDate.SimpleCastDataProvider();
        TimeZone current = TimeZone.getDefault();
        try {
            for (String id : TimeZone.getAvailableIDs()) {
                if (id.equals("GMT0")) {
                    continue;
                }
                TimeZone.setDefault(TimeZone.getTimeZone(id));
                provider.currentTimeZone = TimeZoneProvider.ofId(id);
                DateTimeUtils.resetCalendar();
                testConversionsImpl("2017-12-05 23:59:30.987654321-12:00", true, provider);
                testConversionsImpl("2000-01-02 10:20:30.123456789+07:30", true, provider);
                boolean testReverse = !"Africa/Monrovia".equals(id);
                testConversionsImpl("1960-04-06 12:13:14.777666555+12:00", testReverse, provider);
            }
        } finally {
            TimeZone.setDefault(current);
            DateTimeUtils.resetCalendar();
        }
    }

}
