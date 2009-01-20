/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: Robert Rathsack (firstName dot lastName at gmx dot de)
 */
package org.h2.util;

import java.util.Date;
import java.util.Calendar;

/**
 * <p>
 * Calculate day of week, week of year and year according to the ISO 8601
 * specification. See also http://en.wikipedia.org/wiki/ISO_8601
 * </p><p>
 * The specification defines that the week starts at Monday. The first week of
 * the year is defined as the week which contains at least 4 days of the new
 * year. Therefore if January 1st is on Thursday (or earlier) it belongs to the
 * first week, otherwise to the last week of the previous year. Hence January
 * 4th always belongs to the first week while the December 28th always belongs
 * to the last week. The year of a date reflects to this corresponding week
 * definition.
 * </p>
 */
public class DateTimeIso8601Utils {

    /**
     * Return the day of week. Week starts at Monday.
     *
     * @param date the date object which day of week should be calculated
     * @return the day of the week, Monday as 1 to Sunday as 7
     */
    public static int getIsoDayOfWeek(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(date.getTime());
        int val = cal.get(Calendar.DAY_OF_WEEK) - 1;
        return val == 0 ? 7 : val;
    }

    /**
     * Returns the week of the year. The spec defines the first week of the year
     * as this week which contains at least 4 days. The week starts at Monday.
     * Therefore December 29th - 31th could belong to the next year and January
     * 1st - 3th could belong to the previous year.
     *
     * @param date the date object which week of year should be calculated
     * @return the week of the year
     */
    public static int getIsoWeek(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(date.getTime());
        c.setFirstDayOfWeek(Calendar.MONDAY);
        c.setMinimalDaysInFirstWeek(4);
        return c.get(Calendar.WEEK_OF_YEAR);
    }

    /**
     * Returns the year according to the ISO week definition.
     *
     * @param date the date object which year should be calculated
     * @return the year
     */
    public static int getIsoYear(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(date.getTime());
        cal.setFirstDayOfWeek(Calendar.MONDAY);
        cal.setMinimalDaysInFirstWeek(4);
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH);
        int week = cal.get(Calendar.WEEK_OF_YEAR);
        if (month == 0 && week > 51) {
            year--;
        } else if (month == 11 && week == 1) {
            year++;
        }
        return year;
    }

}
