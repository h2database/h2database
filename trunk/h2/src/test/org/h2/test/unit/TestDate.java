/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.test.TestBase;
import org.h2.util.DateTimeUtils;
import org.h2.value.Value;

/**
 * Tests the data parsing.
 * The problem is that some dates are not allowed because of the summer time change.
 * Most countries change at 2 o'clock in the morning to 3 o'clock, but some
 * (for example Chile) change at midnight. Non-lenient parsing wouldn't work in this case.
 */
public class TestDate extends TestBase {

    public void test() throws Exception {
        for (int year = 1970; year < 2070; year++) {
            for (int month = 1; month <= 12; month++) {
                for (int day = 1; day < 29; day++) {
                    for (int hour = 0; hour < 24; hour++) {
                        test(year, month, day, hour);
                    }
                }
            }
        }
    }

    private void test(int year, int month, int day, int hour) throws SQLException {
        DateTimeUtils.parseDateTime(year + "-" + month + "-" + day + " " + hour + ":00:00", Value.TIMESTAMP, ErrorCode.TIMESTAMP_CONSTANT_2);
    }


}
