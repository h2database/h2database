/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import org.h2.api.Interval;
import org.h2.test.TestBase;

/**
 * Test cases for Interval.
 */
public class TestInterval extends TestBase {

    /**
     * Run just this test.
     *
     * @param a
     *            ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        Interval i;
        i = Interval.ofYears(100);
        assertEquals(100, i.getYears());
        assertEquals("INTERVAL '100' YEAR", i.toString());
        i = Interval.ofMonths(100);
        assertEquals(100, i.getMonths());
        assertEquals("INTERVAL '100' MONTH", i.toString());
        i = Interval.ofDays(100);
        assertEquals(100, i.getDays());
        assertEquals("INTERVAL '100' DAY", i.toString());
        i = Interval.ofHours(100);
        assertEquals(100, i.getHours());
        assertEquals("INTERVAL '100' HOUR", i.toString());
        i = Interval.ofMinutes(100);
        assertEquals(100, i.getMinutes());
        assertEquals("INTERVAL '100' MINUTE", i.toString());
        i = Interval.ofNanos(100_123_456_789L);
        assertEquals(100_123_456_789L, i.getNanos());
        assertEquals("INTERVAL '100.123456789' SECOND", i.toString());
    }

}
