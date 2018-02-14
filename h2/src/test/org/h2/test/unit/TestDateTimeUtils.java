/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import static org.h2.util.DateTimeUtils.dateValue;
import java.util.Calendar;
import java.util.GregorianCalendar;
import org.h2.test.TestBase;
import org.h2.util.DateTimeUtils;

/**
 * Unit tests for the DateTimeUtils class
 */
public class TestDateTimeUtils extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testParseTimeNanosDB2Format();
        testDayOfWeek();
        testWeekOfYear();
        testDateValueFromDenormalizedDate();
    }

    private void testParseTimeNanosDB2Format() {
        assertEquals(3723004000000L, DateTimeUtils.parseTimeNanos("01:02:03.004", 0, 12, true));
        assertEquals(3723004000000L, DateTimeUtils.parseTimeNanos("01.02.03.004", 0, 12, true));

        assertEquals(3723000000000L, DateTimeUtils.parseTimeNanos("01:02:03", 0, 8, true));
        assertEquals(3723000000000L, DateTimeUtils.parseTimeNanos("01.02.03", 0, 8, true));
    }

    /**
     * Test for {@link DateTimeUtils#getSundayDayOfWeek(long)} and
     * {@link DateTimeUtils#getIsoDayOfWeek(long)}.
     */
    private void testDayOfWeek() {
        GregorianCalendar gc = DateTimeUtils.createGregorianCalendar(DateTimeUtils.UTC);
        for (int i = -1_000_000; i <= 1_000_000; i++) {
            gc.clear();
            gc.setTimeInMillis(i * 86400000L);
            int year = gc.get(Calendar.YEAR);
            if (gc.get(Calendar.ERA) == GregorianCalendar.BC) {
                year = 1 - year;
            }
            long expectedDateValue = dateValue(year, gc.get(Calendar.MONTH) + 1,
                    gc.get(Calendar.DAY_OF_MONTH));
            long dateValue = DateTimeUtils.dateValueFromAbsoluteDay(i);
            assertEquals(expectedDateValue, dateValue);
            assertEquals(i, DateTimeUtils.absoluteDayFromDateValue(dateValue));
            int dow = gc.get(Calendar.DAY_OF_WEEK);
            assertEquals(dow, DateTimeUtils.getSundayDayOfWeek(dateValue));
            int isoDow = (dow + 5) % 7 + 1;
            assertEquals(isoDow, DateTimeUtils.getIsoDayOfWeek(dateValue));
            assertEquals(gc.get(Calendar.WEEK_OF_YEAR),
                    DateTimeUtils.getWeekOfYear(dateValue, gc.getFirstDayOfWeek() - 1,
                    gc.getMinimalDaysInFirstWeek()));
        }
    }

    /**
     * Test for {@link DateTimeUtils#getDayOfYear(long)},
     * {@link DateTimeUtils#getWeekOfYear(long, int, int)} and
     * {@link DateTimeUtils#getWeekYear(long, int, int)}.
     */
    private void testWeekOfYear() {
        GregorianCalendar gc = new GregorianCalendar(DateTimeUtils.UTC);
        for (int firstDay = 1; firstDay <= 7; firstDay++) {
            gc.setFirstDayOfWeek(firstDay);
            for (int minimalDays = 1; minimalDays <= 7; minimalDays++) {
                gc.setMinimalDaysInFirstWeek(minimalDays);
                for (int i = 0; i < 150_000; i++) {
                    long dateValue = DateTimeUtils.dateValueFromAbsoluteDay(i);
                    gc.clear();
                    gc.setTimeInMillis(i * 86400000L);
                    assertEquals(gc.get(Calendar.DAY_OF_YEAR), DateTimeUtils.getDayOfYear(dateValue));
                    assertEquals(gc.get(Calendar.WEEK_OF_YEAR),
                            DateTimeUtils.getWeekOfYear(dateValue, firstDay - 1, minimalDays));
                    assertEquals(gc.getWeekYear(), DateTimeUtils.getWeekYear(dateValue, firstDay - 1, minimalDays));
                }
            }
        }
    }

    /**
     * Test for {@link DateTimeUtils#dateValueFromDenormalizedDate(long, long, int)}.
     */
    private void testDateValueFromDenormalizedDate() {
        assertEquals(dateValue(2017, 1, 1), DateTimeUtils.dateValueFromDenormalizedDate(2018, -11, 0));
        assertEquals(dateValue(2001, 2, 28), DateTimeUtils.dateValueFromDenormalizedDate(2000, 14, 29));
        assertEquals(dateValue(1999, 8, 1), DateTimeUtils.dateValueFromDenormalizedDate(2000, -4, -100));
        assertEquals(dateValue(2100, 12, 31), DateTimeUtils.dateValueFromDenormalizedDate(2100, 12, 2000));
        assertEquals(dateValue(-100, 2, 29), DateTimeUtils.dateValueFromDenormalizedDate(-100, 2, 30));
    }

}
