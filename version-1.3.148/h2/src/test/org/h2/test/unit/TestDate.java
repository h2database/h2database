/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.TimeZone;

import org.h2.constant.ErrorCode;
import org.h2.test.TestBase;
import org.h2.util.DateTimeUtils;
import org.h2.value.Value;

/**
 * Tests the data parsing. The problem is that some dates are not allowed
 * because of the summer time change. Most countries change at 2 o'clock in the
 * morning to 3 o'clock, but some (for example Chile) change at midnight.
 * Non-lenient parsing would not work in this case.
 */
public class TestDate extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        testAllTimeZones();
        testCurrentTimeZone();
    }

    private void testCurrentTimeZone() {
        for (int year = 1970; year < 2050; year += 3) {
            for (int month = 1; month <= 12; month++) {
                for (int day = 1; day < 29; day++) {
                    for (int hour = 0; hour < 24; hour++) {
                        test(year, month, day, hour);
                    }
                }
            }
        }
    }

    private void test(int year, int month, int day, int hour) {
        DateTimeUtils.parseDateTime(year + "-" + month + "-" + day + " " + hour + ":00:00", Value.TIMESTAMP, ErrorCode.TIMESTAMP_CONSTANT_2);
    }

    private void testAllTimeZones() throws SQLException {
        Connection conn = getConnection("date");
        TimeZone defaultTimeZone = TimeZone.getDefault();
        PreparedStatement prep = conn.prepareStatement("CALL CAST(? AS DATE)");
        try {
            String[] ids = TimeZone.getAvailableIDs();
            for (int i = 0; i < ids.length; i++) {
                TimeZone.setDefault(TimeZone.getTimeZone(ids[i]));
                DateTimeUtils.resetCalendar();
                for (int d = 101; d < 129; d++) {
                    test(prep, d);
                }
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
            DateTimeUtils.resetCalendar();
        }
        conn.close();
        deleteDb("date");
    }

    private void test(PreparedStatement prep, int d) throws SQLException {
        String s = "2040-10-" + ("" + d).substring(1);
        // some dates don't work in some versions of Java
        //        java.sql.Date date = java.sql.Date.valueOf(s);
        //        long time = date.getTime();
        //        int plus = 0;
        //        while (true) {
        //            date = new java.sql.Date(time);
        //            String x = date.toString();
        //            if (x.equals(s)) {
        //                break;
        //            }
        //            time += 1000;
        //            plus += 1000;
        //        }
        prep.setString(1, s);
        ResultSet rs = prep.executeQuery();
        rs.next();
        String t = rs.getString(1);
        assertEquals(s, t);
    }

}
