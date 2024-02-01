/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.h2.api.ErrorCode;
import org.h2.api.JavaObjectSerializer;
import org.h2.engine.CastDataProvider;
import org.h2.engine.Mode;
import org.h2.test.TestBase;
import org.h2.util.DateTimeUtils;
import org.h2.util.LegacyDateTimeUtils;
import org.h2.util.TimeZoneProvider;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueDouble;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * Tests the date parsing. The problem is that some dates are not allowed
 * because of the summer time change. Most countries change at 2 o'clock in the
 * morning to 3 o'clock, but some (for example Chile) change at midnight.
 * Non-lenient parsing would not work in this case.
 */
public class TestDate extends TestBase {

    static class SimpleCastDataProvider implements CastDataProvider {

        TimeZoneProvider currentTimeZone = DateTimeUtils.getTimeZone();

        ValueTimestampTimeZone currentTimestamp = DateTimeUtils.currentTimestamp(currentTimeZone);

        @Override
        public Mode getMode() {
            return Mode.getRegular();
        }

        @Override
        public ValueTimestampTimeZone currentTimestamp() {
            return currentTimestamp;
        }

        @Override
        public TimeZoneProvider currentTimeZone() {
            return currentTimeZone;
        }

        @Override
        public JavaObjectSerializer getJavaObjectSerializer() {
            return null;
        }

        @Override
        public boolean zeroBasedEnums() {
            return false;
        }

    }

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
        testValueDate();
        testValueTime();
        testValueTimestamp();
        testValueTimestampWithTimezone();
        testValidDate();
        testAbsoluteDay();
        testCalculateLocalMillis();
        testDateTimeUtils();
    }

    private void testValueDate() {
        assertEquals("2000-01-01",
                LegacyDateTimeUtils.fromDate(null, null, Date.valueOf("2000-01-01")).getString());
        assertEquals("0000-00-00",
                ValueDate.fromDateValue(0).getString());
        assertEquals("9999-12-31",
                ValueDate.parse("9999-12-31").getString());
        assertEquals("-9999-12-31",
                ValueDate.parse("-9999-12-31").getString());
        ValueDate d1 = ValueDate.parse("2001-01-01");
        assertEquals("2001-01-01", LegacyDateTimeUtils.toDate(null,  null, d1).toString());
        assertEquals("DATE '2001-01-01'", d1.getTraceSQL());
        assertEquals("DATE '2001-01-01'", d1.toString());
        assertEquals(Value.DATE, d1.getValueType());
        long dv = d1.getDateValue();
        assertEquals((int) ((dv >>> 32) ^ dv), d1.hashCode());
        TypeInfo type = d1.getType();
        assertEquals(d1.getString().length(), type.getDisplaySize());
        assertEquals(ValueDate.PRECISION, type.getPrecision());
        ValueDate d1b = ValueDate.parse("2001-01-01");
        assertTrue(d1 == d1b);
        Value.clearCache();
        d1b = ValueDate.parse("2001-01-01");
        assertFalse(d1 == d1b);
        assertTrue(d1.equals(d1));
        assertTrue(d1.equals(d1b));
        assertTrue(d1b.equals(d1));
        assertEquals(0, d1.compareTo(d1b, null, null));
        assertEquals(0, d1b.compareTo(d1, null, null));
        ValueDate d2 = ValueDate.parse("2002-02-02");
        assertFalse(d1.equals(d2));
        assertFalse(d2.equals(d1));
        assertEquals(-1, d1.compareTo(d2, null, null));
        assertEquals(1, d2.compareTo(d1, null, null));
    }

    private void testValueTime() {
        assertEquals("10:20:30", LegacyDateTimeUtils.fromTime(null, null, Time.valueOf("10:20:30")).getString());
        assertEquals("00:00:00", ValueTime.fromNanos(0).getString());
        assertEquals("23:59:59", ValueTime.parse("23:59:59", null).getString());
        assertEquals("11:22:33.444555666", ValueTime.parse("11:22:33.444555666", null).getString());
        assertThrows(ErrorCode.INVALID_DATETIME_CONSTANT_2, () -> ValueTime.parse("-00:00:00.000000001", null));
        assertThrows(ErrorCode.INVALID_DATETIME_CONSTANT_2, () -> ValueTime.parse("24:00:00", null));
        ValueTime t1 = ValueTime.parse("11:11:11", null);
        assertEquals("11:11:11", LegacyDateTimeUtils.toTime(null,  null, t1).toString());
        assertEquals("TIME '11:11:11'", t1.getTraceSQL());
        assertEquals("TIME '11:11:11'", t1.toString());
        assertEquals("05:35:35.5", t1.multiply(ValueDouble.get(0.5)).getString());
        assertEquals("22:22:22", t1.divide(ValueDouble.get(0.5), TypeInfo.TYPE_TIME).getString());
        assertEquals(Value.TIME, t1.getValueType());
        long nanos = t1.getNanos();
        assertEquals((int) ((nanos >>> 32) ^ nanos), t1.hashCode());
        // Literals return maximum precision
        TypeInfo type = t1.getType();
        assertEquals(ValueTime.MAXIMUM_PRECISION, type.getDisplaySize());
        assertEquals(ValueTime.MAXIMUM_PRECISION, type.getPrecision());
        ValueTime t1b = ValueTime.parse("11:11:11", null);
        assertTrue(t1 == t1b);
        Value.clearCache();
        t1b = ValueTime.parse("11:11:11", null);
        assertFalse(t1 == t1b);
        assertTrue(t1.equals(t1));
        assertTrue(t1.equals(t1b));
        assertTrue(t1b.equals(t1));
        assertEquals(0, t1.compareTo(t1b, null, null));
        assertEquals(0, t1b.compareTo(t1, null, null));
        ValueTime t2 = ValueTime.parse("22:22:22", null);
        assertFalse(t1.equals(t2));
        assertFalse(t2.equals(t1));
        assertEquals(-1, t1.compareTo(t2, null, null));
        assertEquals(1, t2.compareTo(t1, null, null));
    }

    private void testValueTimestampWithTimezone() {
        for (int m = 1; m <= 12; m++) {
            for (int d = 1; d <= 28; d++) {
                for (int h = 0; h <= 23; h++) {
                    String s = "2011-" + (m < 10 ? "0" : "") + m +
                            "-" + (d < 10 ? "0" : "") + d + " " +
                            (h < 10 ? "0" : "") + h + ":00:00";
                    ValueTimestamp ts = ValueTimestamp.parse(s + "Z", null);
                    String s2 = ts.getString();
                    ValueTimestamp ts2 = ValueTimestamp.parse(s2, null);
                    assertEquals(ts.getString(), ts2.getString());
                }
            }
        }
    }

    @SuppressWarnings("unlikely-arg-type")
    private void testValueTimestamp() {
        assertEquals(
                "2001-02-03 04:05:06",
                LegacyDateTimeUtils.fromTimestamp(null, null, Timestamp.valueOf("2001-02-03 04:05:06")).getString());
        assertEquals(
                "2001-02-03 04:05:06.001002003",
                LegacyDateTimeUtils.fromTimestamp(null, null, Timestamp.valueOf("2001-02-03 04:05:06.001002003"))
                .getString());
        assertEquals(
                "0000-00-00 00:00:00", ValueTimestamp.fromDateValueAndNanos(0, 0).getString());
        assertEquals(
                "9999-12-31 23:59:59",
                ValueTimestamp.parse("9999-12-31 23:59:59", null).getString());

        ValueTimestamp t1 = ValueTimestamp.parse("2001-01-01 01:01:01.111", null);
        assertEquals("2001-01-01 01:01:01.111", LegacyDateTimeUtils.toTimestamp(null,  null, t1).toString());
        assertEquals("2001-01-01", LegacyDateTimeUtils.toDate(null,  null, t1).toString());
        assertEquals("01:01:01", LegacyDateTimeUtils.toTime(null,  null, t1).toString());
        assertEquals("TIMESTAMP '2001-01-01 01:01:01.111'", t1.getTraceSQL());
        assertEquals("TIMESTAMP '2001-01-01 01:01:01.111'", t1.toString());
        assertEquals(Value.TIMESTAMP, t1.getValueType());
        long dateValue = t1.getDateValue();
        long nanos = t1.getTimeNanos();
        assertEquals((int) ((dateValue >>> 32) ^ dateValue ^
                (nanos >>> 32) ^ nanos),
                t1.hashCode());
        // Literals return maximum precision
        TypeInfo type = t1.getType();
        assertEquals(ValueTimestamp.MAXIMUM_PRECISION, type.getDisplaySize());
        assertEquals(ValueTimestamp.MAXIMUM_PRECISION, type.getPrecision());
        assertEquals(9, type.getScale());
        ValueTimestamp t1b = ValueTimestamp.parse("2001-01-01 01:01:01.111", null);
        assertTrue(t1 == t1b);
        Value.clearCache();
        t1b = ValueTimestamp.parse("2001-01-01 01:01:01.111", null);
        assertFalse(t1 == t1b);
        assertTrue(t1.equals(t1));
        assertTrue(t1.equals(t1b));
        assertTrue(t1b.equals(t1));
        assertEquals(0, t1.compareTo(t1b, null, null));
        assertEquals(0, t1b.compareTo(t1, null, null));
        ValueTimestamp t2 = ValueTimestamp.parse("2002-02-02 02:02:02.222", null);
        assertFalse(t1.equals(t2));
        assertFalse(t2.equals(t1));
        assertEquals(-1, t1.compareTo(t2, null, null));
        assertEquals(1, t2.compareTo(t1, null, null));
        SimpleCastDataProvider provider = new SimpleCastDataProvider();
        t1 = ValueTimestamp.parse("2001-01-01 01:01:01.123456789", null);
        assertEquals("2001-01-01 01:01:01.123456789",
                t1.getString());
        assertEquals("2001-01-01 01:01:01.123456789",
                t1.castTo(TypeInfo.getTypeInfo(Value.TIMESTAMP, 0L, 9, null), provider).getString());
        assertEquals("2001-01-01 01:01:01.12345679",
                t1.castTo(TypeInfo.getTypeInfo(Value.TIMESTAMP, 0L, 8, null), provider).getString());
        assertEquals("2001-01-01 01:01:01.1234568",
                t1.castTo(TypeInfo.getTypeInfo(Value.TIMESTAMP, 0L, 7, null), provider).getString());
        assertEquals("2001-01-01 01:01:01.123457",
                t1.castTo(TypeInfo.getTypeInfo(Value.TIMESTAMP, 0L, 6, null), provider).getString());
        assertEquals("2001-01-01 01:01:01.12346",
                t1.castTo(TypeInfo.getTypeInfo(Value.TIMESTAMP, 0L, 5, null), provider).getString());
        assertEquals("2001-01-01 01:01:01.1235",
                t1.castTo(TypeInfo.getTypeInfo(Value.TIMESTAMP, 0L, 4, null), provider).getString());
        assertEquals("2001-01-01 01:01:01.123",
                t1.castTo(TypeInfo.getTypeInfo(Value.TIMESTAMP, 0L, 3, null), provider).getString());
        assertEquals("2001-01-01 01:01:01.12",
                t1.castTo(TypeInfo.getTypeInfo(Value.TIMESTAMP, 0L, 2, null), provider).getString());
        assertEquals("2001-01-01 01:01:01.1",
                t1.castTo(TypeInfo.getTypeInfo(Value.TIMESTAMP, 0L, 1, null), provider).getString());
        assertEquals("2001-01-01 01:01:01",
                t1.castTo(TypeInfo.getTypeInfo(Value.TIMESTAMP, 0L, 0, null), provider).getString());
        t1 = ValueTimestamp.parse("-2001-01-01 01:01:01.123456789", null);
        assertEquals("-2001-01-01 01:01:01.123457",
                t1.castTo(TypeInfo.getTypeInfo(Value.TIMESTAMP, 0L, 6, null), provider).getString());
        // classes do not match
        assertFalse(ValueTimestamp.parse("2001-01-01", null).
                equals(ValueDate.parse("2001-01-01")));

        provider.currentTimestamp = ValueTimestampTimeZone.fromDateValueAndNanos(DateTimeUtils.EPOCH_DATE_VALUE, 0,
                provider.currentTimeZone.getTimeZoneOffsetUTC(0L));
        assertEquals("2001-01-01 01:01:01",
                ValueTimestamp.parse("2001-01-01", null).add(
                ValueTime.parse("01:01:01", null).convertTo(TypeInfo.TYPE_TIMESTAMP, provider)).getString());
        assertEquals("1010-10-10 00:00:00",
                ValueTimestamp.parse("1010-10-10 10:10:10", null).subtract(
                ValueTime.parse("10:10:10", null).convertTo(TypeInfo.TYPE_TIMESTAMP, provider)).getString());
        assertEquals("-2001-01-01 01:01:01",
                ValueTimestamp.parse("-2001-01-01", null).add(
                ValueTime.parse("01:01:01", null).convertTo(TypeInfo.TYPE_TIMESTAMP, provider)).getString());
        assertEquals("-1010-10-10 00:00:00",
                ValueTimestamp.parse("-1010-10-10 10:10:10", null).subtract(
                ValueTime.parse("10:10:10", null).convertTo(TypeInfo.TYPE_TIMESTAMP, provider)).getString());

        assertEquals(0, DateTimeUtils.absoluteDayFromDateValue(
                ValueTimestamp.parse("1970-01-01", null).getDateValue()));
        assertEquals(0, ValueTimestamp.parse("1970-01-01", null).getTimeNanos());
        assertEquals(0, LegacyDateTimeUtils.toTimestamp(null, null,
                ValueTimestamp.parse("1970-01-01 00:00:00.000 UTC", null)).getTime());
        assertEquals(0, LegacyDateTimeUtils.toTimestamp(null, null,
                ValueTimestamp.parse("+1970-01-01T00:00:00.000Z", null)).getTime());
        assertEquals(0, LegacyDateTimeUtils.toTimestamp(null, null,
                ValueTimestamp.parse("1970-01-01T00:00:00.000+00:00", null)).getTime());
        assertEquals(0, LegacyDateTimeUtils.toTimestamp(null, null,
                ValueTimestamp.parse("1970-01-01T00:00:00.000-00:00", null)).getTime());
        assertThrows(ErrorCode.INVALID_DATETIME_CONSTANT_2,
                () -> ValueTimestamp.parse("1970-01-01 00:00:00.000 ABC", null));
        assertThrows(ErrorCode.INVALID_DATETIME_CONSTANT_2,
                () -> ValueTimestamp.parse("1970-01-01T00:00:00.000+ABC", null));
    }

    private void testAbsoluteDay() {
        long next = Long.MIN_VALUE;
        for (int y = -2000; y < 3000; y++) {
            for (int m = -3; m <= 14; m++) {
                for (int d = -2; d <= 35; d++) {
                    if (!DateTimeUtils.isValidDate(y, m, d)) {
                        continue;
                    }
                    long date = DateTimeUtils.dateValue(y, m, d);
                    long abs = DateTimeUtils.absoluteDayFromDateValue(date);
                    if (abs != next && next != Long.MIN_VALUE) {
                        assertEquals(abs, next);
                    }
                    if (m == 1 && d == 1) {
                        assertEquals(abs, DateTimeUtils.absoluteDayFromYear(y));
                    }
                    next = abs + 1;
                    long d2 = DateTimeUtils.dateValueFromAbsoluteDay(abs);
                    assertEquals(date, d2);
                    assertEquals(y, DateTimeUtils.yearFromDateValue(date));
                    assertEquals(m, DateTimeUtils.monthFromDateValue(date));
                    assertEquals(d, DateTimeUtils.dayFromDateValue(date));
                    long nextDateValue = DateTimeUtils.dateValueFromAbsoluteDay(next);
                    assertEquals(nextDateValue, DateTimeUtils.incrementDateValue(date));
                    assertEquals(date, DateTimeUtils.decrementDateValue(nextDateValue));
                }
            }
        }
    }

    private void testValidDate() {
        Calendar c = TestDateTimeUtils.createGregorianCalendar(LegacyDateTimeUtils.UTC);
        c.setLenient(false);
        for (int y = -2000; y < 3000; y++) {
            for (int m = -3; m <= 14; m++) {
                for (int d = -2; d <= 35; d++) {
                    boolean valid = DateTimeUtils.isValidDate(y, m, d);
                    if (m < 1 || m > 12) {
                        assertFalse(valid);
                    } else if (d < 1 || d > 31) {
                        assertFalse(valid);
                    } else if (d <= 27) {
                        assertTrue(valid);
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
                            fail(y + "-" + m + "-" + d +
                                    " expected: " + expected + " got: " + valid);
                        }
                    }
                }
            }
        }
    }

    private static void testCalculateLocalMillis() {
        TimeZone defaultTimeZone = TimeZone.getDefault();
        try {
            for (TimeZone tz : TestDate.getDistinctTimeZones()) {
                TimeZone.setDefault(tz);
                for (int y = 1900; y < 2039; y += 10) {
                    if (y == 1993) {
                        // timezone change in Kwajalein
                    } else if (y == 1995) {
                        // timezone change in Enderbury and Kiritimati
                    }
                    for (int m = 1; m <= 12; m++) {
                        if (m != 3 && m != 4 && m != 10 && m != 11) {
                            // only test daylight saving time transitions
                            continue;
                        }
                        for (int day = 1; day < 29; day++) {
                            testDate(y, m, day);
                        }
                    }
                }
            }
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    private static void testDate(int y, int m, int day) {
        long millis = LegacyDateTimeUtils.getMillis(null, TimeZone.getDefault(), DateTimeUtils.dateValue(y, m, day),
                0);
        String st = new java.sql.Date(millis).toString();
        int y2 = Integer.parseInt(st.substring(0, 4));
        int m2 = Integer.parseInt(st.substring(5, 7));
        int d2 = Integer.parseInt(st.substring(8, 10));
        if (y != y2 || m != m2 || day != d2) {
            String s = y + "-" + (m < 10 ? "0" + m : m) +
                    "-" + (day < 10 ? "0" + day : day);
            System.out.println(s + "<>" + st + " " + TimeZone.getDefault().getID());
        }
    }

    /**
     * Get the list of timezones with distinct rules.
     *
     * @return the list
     */
    public static ArrayList<TimeZone> getDistinctTimeZones() {
        ArrayList<TimeZone> distinct = new ArrayList<>();
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
        TimeZone old = TimeZone.getDefault();
        /*
         * java.util.TimeZone doesn't support LMT, so perform this test with
         * fixed time zone offset
         */
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+01"));
        DateTimeUtils.resetCalendar();
        try {
            ValueTimestamp ts1 = ValueTimestamp.parse("-999-08-07 13:14:15.16", null);
            ValueTimestamp ts2 = ValueTimestamp.parse("19999-08-07 13:14:15.16", null);
            ValueTime t1 = (ValueTime) ts1.convertTo(TypeInfo.TYPE_TIME);
            ValueTime t2 = (ValueTime) ts2.convertTo(TypeInfo.TYPE_TIME);
            ValueDate d1 = ts1.convertToDate(null);
            ValueDate d2 = ts2.convertToDate(null);
            assertEquals("-0999-08-07 13:14:15.16", ts1.getString());
            assertEquals("-0999-08-07", d1.getString());
            assertEquals("13:14:15.16", t1.getString());
            assertEquals("19999-08-07 13:14:15.16", ts2.getString());
            assertEquals("19999-08-07", d2.getString());
            assertEquals("13:14:15.16", t2.getString());
            TimeZone timeZone = TimeZone.getDefault();
            ValueTimestamp ts1a = LegacyDateTimeUtils.fromTimestamp(null, timeZone,
                    LegacyDateTimeUtils.toTimestamp(null,  null, ts1));
            ValueTimestamp ts2a = LegacyDateTimeUtils.fromTimestamp(null, timeZone,
                    LegacyDateTimeUtils.toTimestamp(null,  null, ts2));
            assertEquals("-0999-08-07 13:14:15.16", ts1a.getString());
            assertEquals("19999-08-07 13:14:15.16", ts2a.getString());
        } finally {
            TimeZone.setDefault(old);
            DateTimeUtils.resetCalendar();
        }
    }

}
