/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import static org.h2.util.DateTimeUtils.dateValue;

import org.h2.api.JavaObjectSerializer;
import org.h2.engine.CastDataProvider;
import org.h2.engine.Mode;
import org.h2.message.DbException;
import org.h2.test.TestBase;
import org.h2.util.DateTimeTemplate;
import org.h2.util.TimeZoneProvider;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimeTimeZone;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * Test cases for DateTimeTemplate.
 */
public class TestDateTimeTemplate extends TestBase {

    private static final class Provider implements CastDataProvider {

        private final ValueTimestampTimeZone currentTimestamp;

        Provider(int year, int month) {
            currentTimestamp = ValueTimestampTimeZone.fromDateValueAndNanos(dateValue(year, month, 15), 1234567890123L,
                    -12233);
        }

        @Override
        public ValueTimestampTimeZone currentTimestamp() {
            return currentTimestamp;
        }

        @Override
        public TimeZoneProvider currentTimeZone() {
            return null;
        }

        @Override
        public Mode getMode() {
            return null;
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
     * @param a
     *            ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().testFromMain();
    }

    @Override
    public void test() throws Exception {
        testDate();
        testTime();
        testTimeTz();
        testTimestamp();
        testTimestampTz();
        testInvalidCombinations();
        testInvalidDelimiters();
        testInvalidFields();
        testInvalidTemplates();
        testOutOfRange();
        testParseErrors();
    }

    private void testDate() {
        Provider provider = new Provider(2023, 4);

        ValueDate date = date(2022, 10, 12);
        assertEquals("2022-10-12", date, "YYYY-MM-DD", provider);
        assertEquals("022-10-12", date, "YYY-MM-DD", provider);
        assertEquals("22-10-12", date, "YY-MM-DD", provider);
        assertEquals("2-10-12", date, "Y-MM-DD", provider);
        assertEquals("2022-10-12", date, "RRRR-MM-DD", provider);
        assertEquals("22-10-12", date, "RR-MM-DD", provider);

        assertEquals("2022-12", date(2022, 4, 12), date, "YYYY-DD", provider);
        assertEquals("2022-10", date(2022, 10, 1), date, "YYYY-MM", provider);
        assertEquals("12-10", date(2023, 10, 12), date, "DD-MM", provider);

        assertEquals("22-10-12", date, "RR-MM-DD", provider);
        assertEquals("73-01-01", date(2073, 1, 1), "RR-MM-DD", provider);
        assertEquals("74-01-01", date(1974, 1, 1), date(2074, 1, 1), "RR-MM-DD", provider);
        assertEquals("73-01-01", date(2073, 1, 1), date(1973, 1, 1), "RR-MM-DD", provider);
        Provider altProvider = new Provider(2090, 1);
        assertEquals("40-01-01", date(2040, 1, 1), date(2040, 1, 1), "RR-MM-DD", altProvider);

        date = date(12345, 5, 7);
        assertEquals("12345-05-07", date, "YYYY-MM-DD", provider);
        assertEquals("345-05-07", date(2345, 5, 7), date, "YYY-MM-DD", provider);
        assertEquals("45-05-07", date(2045, 5, 7), date, "YY-MM-DD", provider);
        assertEquals("5-05-07", date(2025, 5, 7), date, "Y-MM-DD", provider);
        assertEquals("12345-05-07", date, "RRRR-MM-DD", provider);
        assertEquals("45-05-07", date(2045, 5, 7), date, "RR-MM-DD", provider);

        date = date(-12345, 5, 7);
        assertEquals("-12345-05-07", date, "YYYY-MM-DD", provider);
        assertEqualsAndFail("-345-05-07", date, "YYY-MM-DD", provider);
        assertEqualsAndFail("-45-05-07", date, "YY-MM-DD", provider);
        assertEqualsAndFail("-5-05-07", date, "Y-MM-DD", provider);
        assertEqualsAndFail("-12345-05-07", date, "RRRR-MM-DD", provider);
        assertEqualsAndFail("-45-05-07", date, "RR-MM-DD", provider);

        assertEquals("1900-061", date(1900, 3, 2), "YYYY-DDD", provider);
        assertEquals("1904-062", date(1904, 3, 2), "YYYY-DDD", provider);
        assertEquals("2000-062", date(2000, 3, 2), "YYYY-DDD", provider);
    }

    private void testTime() {
        Provider provider = new Provider(2023, 4);

        assertEquals("12 A.M.", time(0, 0, 0, 0), "HH A.M.", provider);
        assertEquals("01 A.M.", time(1, 0, 0, 0), "HH A.M.", provider);
        assertEquals("02 A.M.", time(2, 0, 0, 0), "HH A.M.", provider);
        assertEquals("03 A.M.", time(3, 0, 0, 0), "HH A.M.", provider);
        assertEquals("04 A.M.", time(4, 0, 0, 0), "HH A.M.", provider);
        assertEquals("05 A.M.", time(5, 0, 0, 0), "HH A.M.", provider);
        assertEquals("06 A.M.", time(6, 0, 0, 0), "HH A.M.", provider);
        assertEquals("07 A.M.", time(7, 0, 0, 0), "HH A.M.", provider);
        assertEquals("08 A.M.", time(8, 0, 0, 0), "HH A.M.", provider);
        assertEquals("09 A.M.", time(9, 0, 0, 0), "HH A.M.", provider);
        assertEquals("10 A.M.", time(10, 0, 0, 0), "HH A.M.", provider);
        assertEquals("11 A.M.", time(11, 0, 0, 0), "HH A.M.", provider);
        assertEquals("12 P.M.", time(12, 0, 0, 0), "HH A.M.", provider);
        assertEquals("01 P.M.", time(13, 0, 0, 0), "HH A.M.", provider);
        assertEquals("02 P.M.", time(14, 0, 0, 0), "HH A.M.", provider);
        assertEquals("03 P.M.", time(15, 0, 0, 0), "HH A.M.", provider);
        assertEquals("04 P.M.", time(16, 0, 0, 0), "HH A.M.", provider);
        assertEquals("05 P.M.", time(17, 0, 0, 0), "HH A.M.", provider);
        assertEquals("06 P.M.", time(18, 0, 0, 0), "HH A.M.", provider);
        assertEquals("07 P.M.", time(19, 0, 0, 0), "HH A.M.", provider);
        assertEquals("08 P.M.", time(20, 0, 0, 0), "HH A.M.", provider);
        assertEquals("09 P.M.", time(21, 0, 0, 0), "HH A.M.", provider);
        assertEquals("10 P.M.", time(22, 0, 0, 0), "HH A.M.", provider);
        assertEquals("11 P.M.", time(23, 0, 0, 0), "HH A.M.", provider);

        assertEquals("01:02:03.1", time(1, 2, 3, 100_000_000), time(1, 2, 3, 123_456_789), "HH24:MI:SS.FF1", provider);
        assertEquals("01:02:03.12", time(1, 2, 3, 120_000_000), time(1, 2, 3, 123_456_789), "HH24:MI:SS.FF2", //
                provider);
        assertEquals("01:02:03.123", time(1, 2, 3, 123_000_000), time(1, 2, 3, 123_456_789), "HH24:MI:SS.FF3",
                provider);
        assertEquals("01:02:03.1234", time(1, 2, 3, 123_400_000), time(1, 2, 3, 123_456_789), "HH24:MI:SS.FF4",
                provider);
        assertEquals("01:02:03.12345", time(1, 2, 3, 123_450_000), time(1, 2, 3, 123_456_789), "HH24:MI:SS.FF5",
                provider);
        assertEquals("01:02:03.123456", time(1, 2, 3, 123_456_000), time(1, 2, 3, 123_456_789), "HH24:MI:SS.FF6",
                provider);
        assertEquals("01:02:03.1234567", time(1, 2, 3, 123_456_700), time(1, 2, 3, 123_456_789), "HH24:MI:SS.FF7",
                provider);
        assertEquals("01:02:03.12345678", time(1, 2, 3, 123_456_780), time(1, 2, 3, 123_456_789), "HH24:MI:SS.FF8",
                provider);
        assertEquals("01:02:03.123456789", time(1, 2, 3, 123_456_789), "HH24:MI:SS.FF9", provider);

        assertEquals("02:03.123456789", time(0, 2, 3, 123_456_789), time(1, 2, 3, 123_456_789), "MI:SS.FF9", provider);
        assertEquals("01:03.123456789", time(1, 0, 3, 123_456_789), time(1, 2, 3, 123_456_789), "HH24:SS.FF9",
                provider);
        assertEquals("01:02.123456789", time(1, 2, 0, 123_456_789), time(1, 2, 3, 123_456_789), "HH24:MI.FF9",
                provider);
        assertEquals("01:02:03", time(1, 2, 3, 0), time(1, 2, 3, 123_456_789), "HH24:MI:SS", provider);

        assertEquals("37230.987654321", time(10, 20, 30, 987_654_321), "SSSSS.FF9", provider);
        assertEquals("37230987654321", time(10, 20, 30, 987_654_321), "SSSSSFF9", provider);
    }

    private void testTimeTz() {
        Provider provider = new Provider(2023, 4);
        assertEquals("01:02:03.123456789+10:23:45", timeTz(1, 2, 3, 123_456_789, 10, 23, 45),
                "HH24:MI:SS.FF9TZH:TZM:TZS", provider);
        assertEquals("01:02:03.123456789-10:23:45", timeTz(1, 2, 3, 123_456_789, -10, -23, -45),
                "HH24:MI:SS.FF9TZH:TZM:TZS", provider);
        assertEquals("01:02:03.123456789-00:23:45", timeTz(1, 2, 3, 123_456_789, 0, -23, -45),
                "HH24:MI:SS.FF9TZH:TZM:TZS", provider);
        assertEquals("01:02:03.123456789-10:23", timeTz(1, 2, 3, 123_456_789, -10, -23, 0),
                timeTz(1, 2, 3, 123_456_789, -10, -23, -45), "HH24:MI:SS.FF9TZH:TZM", provider);
        assertEquals("01:02:03.123456789-10", timeTz(1, 2, 3, 123_456_789, -10, 0, 0),
                timeTz(1, 2, 3, 123_456_789, -10, -23, -45), "HH24:MI:SS.FF9TZH", provider);
        assertEquals("01:02:03.123456789", timeTz(1, 2, 3, 123_456_789, 0, 0, 0),
                timeTz(1, 2, 3, 123_456_789, -10, -23, -45), "HH24:MI:SS.FF9", provider);
        assertEquals(timeTz(10, 20, 30, 0, 1, 30, 0), DateTimeTemplate.of("HH24:MI:SSTZH:TZM").parse("10:20:30 01:30",
                TypeInfo.getTypeInfo(Value.TIME_TZ), provider));
    }

    private void testTimestamp() {
        Provider provider = new Provider(2023, 4);
        assertEquals("2022-10-12 01:02:03.123456789", timestamp(2022, 10, 12, 1, 2, 3, 123_456_789),
                "YYYY-MM-DD HH24:MI:SS.FF9", provider);

    }

    private void testTimestampTz() {
        Provider provider = new Provider(2023, 4);
        assertEquals("2022-10-12 01:02:03.123456789+10:23:45",
                timestampTz(2022, 10, 12, 1, 2, 3, 123_456_789, 10, 23, 45), "YYYY-MM-DD HH24:MI:SS.FF9TZH:TZM:TZS",
                provider);
    }

    private void testInvalidCombinations() {
        // Fields of the same group may appear only once
        testInvalidCombination("Y YY");
        testInvalidCombination("YY RR");
        testInvalidCombination("MM MM");
        testInvalidCombination("DD DD");
        testInvalidCombination("DDD DDD");
        testInvalidCombination("HH HH12 A.M.");
        testInvalidCombination("HH24 HH24");
        testInvalidCombination("MI MI");
        testInvalidCombination("SS SS");
        testInvalidCombination("SSSSS SSSSS");
        testInvalidCombination("FF1 FF9");
        testInvalidCombination("A.M. P.M. HH");
        testInvalidCombination("TZH TZH");
        testInvalidCombination("TZM TZM");
        testInvalidCombination("TZS TZS");
        // Invalid combinations
        testInvalidCombination("DDD MM");
        testInvalidCombination("DDD DD");
        testInvalidCombination("HH");
        testInvalidCombination("A.M.");
        testInvalidCombination("A.M. HH HH24");
        testInvalidCombination("SSSSS HH");
        testInvalidCombination("SSSSS HH24");
        testInvalidCombination("SSSSS MI");
        testInvalidCombination("SSSSS SS");
        testInvalidCombination("TZS TZH");
        testInvalidCombination("TZM");
    }

    private void testInvalidCombination(String template) {
        assertFail(template);
    }

    private void testInvalidDelimiters() {
        String valid = "-./,';: ";
        DateTimeTemplate.of(valid);
        for (char ch = ' '; ch <= '@'; ch++) {
            if (valid.indexOf(ch) < 0) {
                testInvalidDelimiter(String.valueOf(ch));
            }
        }
        for (char ch = '['; ch <= '`'; ch++) {
            if (valid.indexOf(ch) < 0) {
                testInvalidDelimiter(String.valueOf(ch));
            }
        }
        for (char ch = '{'; ch <= 128; ch++) {
            if (valid.indexOf(ch) < 0) {
                testInvalidDelimiter(String.valueOf(ch));
            }
        }
    }

    private void testInvalidDelimiter(String template) {
        assertFail(template);
    }

    private void testInvalidFields() {
        long dateValue = dateValue(2000, 11, 15), timeNanos = ((14L * 60 + 23) * 60 + 45) * 1_000_000_000 + 123456789;
        int offsetSecons = -((3 * 60 + 37) * 60 + 12);
        ValueDate date = ValueDate.fromDateValue(dateValue);
        ValueTime time = ValueTime.fromNanos(timeNanos);
        ValueTimeTimeZone timeTz = ValueTimeTimeZone.fromNanos(timeNanos, offsetSecons);
        ValueTimestamp timestamp = ValueTimestamp.fromDateValueAndNanos(dateValue, timeNanos);
        testInvalidTimeFields(date);
        testInvalidTimeZoneField(date);
        testInvalidDateFields(time);
        testInvalidTimeZoneField(time);
        testInvalidDateFields(timeTz);
        testInvalidTimeZoneField(timestamp);
    }

    private void testInvalidDateFields(Value value) {
        testInvalidField(value, "23", "YY");
        testInvalidField(value, "23", "RR");
        testInvalidField(value, "10", "MM");
        testInvalidField(value, "15", "DD");
        testInvalidField(value, "100", "DDD");
    }

    private void testInvalidTimeFields(Value value) {
        testInvalidField(value, "12 P.M.", "HH A.M.");
        testInvalidField(value, "18", "HH24");
        testInvalidField(value, "23", "MI");
        testInvalidField(value, "55", "SS");
        testInvalidField(value, "12345", "SSSSS");
    }

    private void testInvalidTimeZoneField(Value value) {
        testInvalidField(value, "+10", "TZH");
        testInvalidField(value, "+10 30", "TZH TZM");
        testInvalidField(value, "+10 30 45", "TZH TZM TZS");
    }

    private void testInvalidField(Value value, String valueString, String template) {
        DateTimeTemplate t = DateTimeTemplate.of(template);
        try {
            t.format(value);
            fail("DbException expected for template \"" + template + "\" and value " + value.getTraceSQL());
        } catch (DbException e) {
            // Expected
        }
        try {
            t.parse(valueString, value.getType(), null);
            fail("DbException expected for template \"" + template + "\" and value " + value.getTraceSQL());
        } catch (DbException e) {
            // Expected
        }
    }

    private void testInvalidTemplates() {
        assertFail("FF ");
        assertFail("FFF");
        assertFail("R");
        assertFail("RRR");
    }

    private void testOutOfRange() {
        Provider provider = new Provider(2023, 4);
        testOutOfRange("YYYY-MM-DD", "2023-02-29", Value.DATE, provider);
        testOutOfRange("YYYY-MM-DD", "2023--1-20", Value.DATE, provider);
        testOutOfRange("YYYY-MM-DD", "2023-13-20", Value.DATE, provider);
        testOutOfRange("YYYY-MM-DD", "2023-01--1", Value.DATE, provider);
        testOutOfRange("YYYY-MM-DD", "2023-01-32", Value.DATE, provider);
        testOutOfRange("YYYY-DDD", "2023-000", Value.DATE, provider);
        testOutOfRange("YYYY-DDD", "2023-366", Value.DATE, provider);
        testOutOfRange("YYYY-DDD", "2024-367", Value.DATE, provider);

        testOutOfRange("Y", "10", Value.DATE, provider);
        testOutOfRange("YY", "100", Value.DATE, provider);
        testOutOfRange("YYY", "1000", Value.DATE, provider);
        testOutOfRange("RR", "100", Value.DATE, provider);

        testOutOfRange("A.M. HH12:MI:SS", "A.M. 13:00:00", Value.TIME, provider);
        testOutOfRange("HH24:MI:SS", "-1:00:00", Value.TIME, provider);
        testOutOfRange("HH24:MI:SS", "24:00:00", Value.TIME, provider);
        testOutOfRange("HH24:MI:SS", "23:-1:00", Value.TIME, provider);
        testOutOfRange("HH24:MI:SS", "23:60:00", Value.TIME, provider);
        testOutOfRange("HH24:MI:SS", "23:00:-1", Value.TIME, provider);
        testOutOfRange("HH24:MI:SS", "23:00:60", Value.TIME, provider);
        testOutOfRange("SSSSS", "-1", Value.TIME, provider);
        testOutOfRange("SSSSS", "86400", Value.TIME, provider);
        testOutOfRange("SSSSS", "9999999999", Value.TIME, provider);
        testOutOfRange("SSSSS", "9999999999", Value.TIME, provider);

        testOutOfRange("HH24:MI:SSTZH:TZM:TZS", "10:20:30+19:00:00", Value.TIME_TZ, provider);
        testOutOfRange("HH24:MI:SSTZH:TZM:TZS", "10:20:30+18:00:01", Value.TIME_TZ, provider);
        testOutOfRange("HH24:MI:SSTZH:TZM:TZS", "10:20:30+10:60:00", Value.TIME_TZ, provider);
        testOutOfRange("HH24:MI:SSTZH:TZM:TZS", "10:20:30+10:00:60", Value.TIME_TZ, provider);
    }

    private void testOutOfRange(String template, String valueString, int valueType, CastDataProvider provider) {
        DateTimeTemplate t = DateTimeTemplate.of(template);
        try {
            t.parse(valueString, TypeInfo.getTypeInfo(valueType), provider);
            fail("DbException expected for template \"" + template + "\" and string \"" + valueString + '"');
        } catch (DbException e) {
            // Expected
        }
    }

    private void testParseErrors() {
        Provider provider = new Provider(2023, 4);
        testParseError("SSSSS", "", Value.TIME, provider);
        testParseError("YYYYSSSSS", "2023", Value.TIMESTAMP, provider);
        testParseError("FF1", "", Value.TIME, provider);
        testParseError("FF1", "A", Value.TIME, provider);
        testParseError("SSFF9", "10", Value.TIME, provider);
        testParseError("SSFF1", "10!", Value.TIME, provider);
        testParseError("SSFF1", "10A", Value.TIME, provider);
        testParseError("YYYY:", "1999", Value.DATE, provider);
        testParseError("YYYY:", "1999;", Value.DATE, provider);

    }

    private void testParseError(String template, String valueString, int valueType, CastDataProvider provider) {
        DateTimeTemplate t = DateTimeTemplate.of(template);
        try {
            t.parse(valueString, TypeInfo.getTypeInfo(valueType), provider);
            fail("DbException expected for template \"" + template + "\" and string \"" + valueString + '"');
        } catch (DbException e) {
            // Expected
        }
    }

    private void assertEquals(String expected, Value value, String template, CastDataProvider provider) {
        DateTimeTemplate t = DateTimeTemplate.of(template);
        assertEquals(expected, t.format(value));
        assertEquals(value, t.parse(expected, value.getType(), provider));
    }

    private void assertEquals(String expectedString, Value expectedValue, Value value, String template,
            CastDataProvider provider) {
        DateTimeTemplate t = DateTimeTemplate.of(template);
        assertEquals(expectedString, t.format(value));
        assertEquals(expectedValue, t.parse(expectedString, value.getType(), provider));
    }

    private void assertEqualsAndFail(String expectedString, Value value, String template, CastDataProvider provider) {
        DateTimeTemplate t = DateTimeTemplate.of(template);
        assertEquals(expectedString, t.format(value));
        try {
            t.parse(expectedString, value.getType(), provider);
            fail("DbException expected for template \"" + template + "\" and string \"" + expectedString + '"');
        } catch (DbException e) {
            // Expected
        }
    }

    private void assertFail(String template) {
        try {
            DateTimeTemplate.of(template);
            fail("DbException expected for template \"" + template + '"');
        } catch (DbException e) {
            // Expected
        }
    }

    private static ValueDate date(int year, int month, int day) {
        return ValueDate.fromDateValue(dateValue(year, month, day));
    }

    private static ValueTime time(int hour, int minute, int second, int nanos) {
        return ValueTime.fromNanos(((hour * 60L + minute) * 60 + second) * 1_000_000_000 + nanos);
    }

    private static ValueTimeTimeZone timeTz(int hour, int minute, int second, int nanos, int tzHour, int tzMinute,
            int tzSeconds) {
        return ValueTimeTimeZone.fromNanos(((hour * 60L + minute) * 60 + second) * 1_000_000_000 + nanos,
                (tzHour * 60 + tzMinute) * 60 + tzSeconds);
    }

    private static ValueTimestamp timestamp(int year, int month, int day, int hour, int minute, int second, //
            int nanos) {
        return ValueTimestamp.fromDateValueAndNanos(dateValue(year, month, day),
                ((hour * 60L + minute) * 60 + second) * 1_000_000_000 + nanos);
    }

    private static ValueTimestampTimeZone timestampTz(int year, int month, int day, int hour, int minute, int second,
            int nanos, int tzHour, int tzMinute, int tzSeconds) {
        return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue(year, month, day),
                ((hour * 60L + minute) * 60 + second) * 1_000_000_000 + nanos,
                (tzHour * 60 + tzMinute) * 60 + tzSeconds);
    }

}
