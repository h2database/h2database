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
        System.setProperty("h2.storeLocalTime", "true");
        TestBase.createCaller().init().test();
    }

    public void test() throws SQLException {
        testTimeOperationsAcrossTimeZones();
        testDateTimeUtils();
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
        java.sql.Timestamp ts1 = (Timestamp) DateTimeUtils.parseDateTime("-999-08-07 13:14:15.16", Value.TIMESTAMP);
        java.sql.Timestamp ts2 = (Timestamp) DateTimeUtils.parseDateTime("19999-08-07 13:14:15.16", Value.TIMESTAMP);
        java.sql.Time t1 = DateTimeUtils.cloneAndNormalizeTime(new java.sql.Time(ts1.getTime()));
        java.sql.Time t2 = DateTimeUtils.cloneAndNormalizeTime(new java.sql.Time(ts2.getTime()));
        java.sql.Date d1 = DateTimeUtils.cloneAndNormalizeDate(new java.sql.Date(ts1.getTime()));
        java.sql.Date d2 = DateTimeUtils.cloneAndNormalizeDate(new java.sql.Date(ts2.getTime()));
        assertEquals("-999-08-07 13:14:15.16", ValueTimestamp.get(ts1).getString());
        assertEquals("-999-08-07", ValueDate.get(d1).getString());
        assertEquals("13:14:15", ValueTime.get(t1).getString());
        assertEquals("19999-08-07 13:14:15.16", ValueTimestamp.get(ts2).getString());
        assertEquals("19999-08-07", ValueDate.get(d2).getString());
        assertEquals("13:14:15", ValueTime.get(t2).getString());
        Calendar cal = Calendar.getInstance();
        cal.setTime(t1);
        assertEquals(GregorianCalendar.AD, cal.get(Calendar.ERA));
        cal.setTime(t2);
        assertEquals(GregorianCalendar.AD, cal.get(Calendar.ERA));
        java.sql.Timestamp ts1a = DateTimeUtils.convertTimestampToCalendar(ts1, Calendar.getInstance());
        java.sql.Timestamp ts2a = DateTimeUtils.convertTimestampToCalendar(ts2, Calendar.getInstance());
        assertEquals("-999-08-07 13:14:15.16", ValueTimestamp.get(ts1a).getString());
        assertEquals("19999-08-07 13:14:15.16", ValueTimestamp.get(ts2a).getString());
    }

}
