/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

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
        // System.setProperty("h2.storeLocalTime", "true");
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        testParseTimeNanosDB2Format();
    }

    private void testParseTimeNanosDB2Format() {
        assertEquals(3723004000000L, DateTimeUtils.parseTimeNanos("01:02:03.004", 0, 12, true));
        assertEquals(3723004000000L, DateTimeUtils.parseTimeNanos("01.02.03.004", 0, 12, true));

        assertEquals(3723000000000L, DateTimeUtils.parseTimeNanos("01:02:03", 0, 8, true));
        assertEquals(3723000000000L, DateTimeUtils.parseTimeNanos("01.02.03", 0, 8, true));
    }
}
