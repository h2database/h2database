/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import org.h2.constant.SysProperties;
import org.h2.store.Data;
import org.h2.test.TestBase;
import org.h2.util.DateTimeUtils;
import org.h2.util.New;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

/**
 * Tests the date parsing. The problem is that some dates are not allowed
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
        // System.setProperty("h2.storeLocalTime", "true");
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        testValidDate();
        testValidTime();
        testCalculateLocalMillis();
        testTimeOperationsAcrossTimeZones();
        testDateTimeUtils();
    }

    private void testValidTime() {
        for (int h = -1; h < 28; h++) {
            for (int m = -1; m < 65; m++) {
                for (int s = -1; s < 65; s++) {
                    boolean valid = DateTimeUtils.isValidTime(h, m, s);
                    boolean expected = h >= 0 && h < 24 && m >= 0 && m < 60 &&
                            s >= 0 && s < 60;
                    assertEquals(expected, valid);
                }
            }
        }
    }

    private void testValidDate() {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        c.setLenient(false);
        for (int y = -2000; y < 3000; y++) {
            for (int m = -10; m <= 20; m++) {
                for (int d = -10; d <= 40; d++) {
                    boolean valid = DateTimeUtils.isValidDate(y, m, d);
                    if (m < 1 || m > 12) {
                        assertFalse(valid);
                    } else if (d < 1 || d > 31) {
                        assertFalse(valid);
                    } else {
                        if (y <= 0) {
                            c.set(Calendar.ERA, GregorianCalendar.BC);
                            c.set(Calendar.YEAR, 1 - y);
                        } else {
                            c.set(Calendar.ERA, GregorianCalendar.AD);
                            c.set(Calendar.YEAR, y);
                        }
                        c.set(Calendar.MONTH, m - 1);
                        c.set(Calendar.DAY_OF_MONTH, d);
                        boolean expected = true;
                        try {
                            c.getTimeInMillis();
                        } catch (Exception e) {
                            expected = false;
                        }
                        if (expected != valid) {
                            fail(y + "-" + m + "-" + d + " expected: " + expected + " got: " + valid);
                        }
                    }
                }
            }
        }
    }

    private void testCalculateLocalMillis() {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try {
            for (TimeZone tz : TestDate.getDistinctTimeZones()) {
                TimeZone.setDefault(tz);
                for (int y = 1900; y < 2039; y++) {
                    if (y == 1993) {
                        // timezone change in Kwajalein
                    } else if (y == 1995) {
                        // timezone change in Enderbury and Kiritimati
                    }
                    for (int m = 1; m <= 12; m++) {
                        for (int day = 1; day < 29; day++) {
                            if (y == 1582 && m == 10 && day >= 5 && day <= 14) {
                                continue;
                            }
                            testDate(y, m, day);
                        }
                    }
                }
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    static void testDate(int y, int m, int day) {
        long millis = DateTimeUtils.getMillis(TimeZone.getDefault(), y, m, day, 0, 0, 0, 0);
        String st = new java.sql.Date(millis).toString();
        int y2 = Integer.parseInt(st.substring(0, 4));
        int m2 = Integer.parseInt(st.substring(5, 7));
        int d2 = Integer.parseInt(st.substring(8, 10));
        if (y != y2 || m != m2 || day != d2) {
            String s = y + "-" + (m < 10 ? "0" + m : m) + "-" + (day < 10 ? "0" + day : day);
            System.out.println(s + "<>" + st + " " + TimeZone.getDefault().getID());
        }
    }

    private void testTimeOperationsAcrossTimeZones() {
        if (!SysProperties.STORE_LOCAL_TIME) {
            return;
        }
        TimeZone defaultTimeZone = TimeZone.getDefault();
        ArrayList<TimeZone> distinct = TestDate.getDistinctTimeZones();
        Data d = Data.create(null, 10240);
        try {
            for (TimeZone tz : distinct) {
                TimeZone.setDefault(tz);
                DateTimeUtils.resetCalendar();
                d.reset();
                for (int m = 1; m <= 12; m++) {
                    for (int h = 0; h <= 23; h++) {
                        if (h == 0 || h == 2 || h == 3) {
                            // those hours may not exist for all days in all
                            // timezones because of daylight saving
                            continue;
                        }
                        String s = "2000-" + (m < 10 ? "0" + m : m) + "-01 " + (h < 10 ? "0" + h : h) + ":00:00.0";
                        d.writeValue(ValueString.get(s));
                        d.writeValue(ValueTimestamp.get(Timestamp.valueOf(s)));
                    }
                }
                d.writeValue(ValueNull.INSTANCE);
                d.reset();
                for (TimeZone target : distinct) {
                    if ("Pacific/Kiritimati".equals(target)) {
                        // there is a problem with this time zone, but it seems
                        // unrelated to this database (possibly wrong timezone
                        // information?)
                        continue;
                    }
                    TimeZone.setDefault(target);
                    DateTimeUtils.resetCalendar();
                    while (true) {
                        Value v = d.readValue();
                        if (v == ValueNull.INSTANCE) {
                            break;
                        }
                        String a = v.getString();
                        String b = d.readValue().getString();
                        if (!a.equals(b)) {
                            assertEquals("source: " + tz.getID() + " target: " + target.getID(), a, b);
                        }
                    }
                }
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
            DateTimeUtils.resetCalendar();
        }
    }

    /**
     * Get the list of timezones with distinct rules.
     *
     * @return the list
     */
    public static ArrayList<TimeZone> getDistinctTimeZones() {
        ArrayList<TimeZone> distinct = New.arrayList();
        for (String id : TimeZone.getAvailableIDs()) {
            TimeZone t = TimeZone.getTimeZone(id);
            for (TimeZone d : distinct) {
                if (t.hasSameRules(d)) {
                    t = null;
                    break;
                }
            }
            if (t != null) {
                distinct.add(t);
            }
        }
        return distinct;
    }

    private void testDateTimeUtils() {
        ValueTimestamp ts1 = (ValueTimestamp) DateTimeUtils.parse("-999-08-07 13:14:15.16", Value.TIMESTAMP);
        ValueTimestamp ts2 = (ValueTimestamp) DateTimeUtils.parse("19999-08-07 13:14:15.16", Value.TIMESTAMP);
        ValueTime t1 = (ValueTime) ts1.convertTo(Value.TIME);
        ValueTime t2 = (ValueTime) ts2.convertTo(Value.TIME);
        ValueDate d1 = (ValueDate) ts1.convertTo(Value.DATE);
        ValueDate d2 = (ValueDate) ts2.convertTo(Value.DATE);
        assertEquals("-999-08-07 13:14:15.16", ts1.getString());
        assertEquals("-999-08-07", d1.getString());
        assertEquals("13:14:15.16", t1.getString());
        assertEquals("19999-08-07 13:14:15.16", ts2.getString());
        assertEquals("19999-08-07", d2.getString());
        assertEquals("13:14:15.16", t2.getString());
        java.sql.Timestamp ts1a = DateTimeUtils.convertTimestampToCalendar(ts1.getTimestamp(), Calendar.getInstance());
        java.sql.Timestamp ts2a = DateTimeUtils.convertTimestampToCalendar(ts2.getTimestamp(), Calendar.getInstance());
        assertEquals("-999-08-07 13:14:15.16", ValueTimestamp.get(ts1a).getString());
        assertEquals("19999-08-07 13:14:15.16", ValueTimestamp.get(ts2a).getString());
    }

}
