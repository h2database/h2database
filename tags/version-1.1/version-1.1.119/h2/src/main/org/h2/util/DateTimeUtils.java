/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.h2.message.Message;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

/**
 * This utility class contains time conversion functions.
 */
public class DateTimeUtils {

    private static final int DEFAULT_YEAR = 1970;
    private static final int DEFAULT_MONTH = 1;
    private static final int DEFAULT_DAY = 1;
    private static final int DEFAULT_HOUR = 0;

    private static Calendar cachedCalendar = Calendar.getInstance();

    private DateTimeUtils() {
        // utility class
    }

    /**
     * Reset the calendar, for example after changing the default timezone.
     */
    public static void resetCalendar() {
        cachedCalendar = null;
    }

    private static Calendar getCalendar() {
        if (cachedCalendar == null) {
            cachedCalendar = Calendar.getInstance();
        }
        return cachedCalendar;
    }

    /**
     * Convert the timestamp to the specified time zone.
     *
     * @param x the timestamp
     * @param calendar the calendar
     * @return the timestamp using the correct time zone
     */
    public static Timestamp convertTimestampToCalendar(Timestamp x, Calendar calendar) throws SQLException {
        if (x != null) {
            Timestamp y = new Timestamp(getLocalTime(x, calendar));
            // fix the nano seconds
            y.setNanos(x.getNanos());
            x = y;
        }
        return x;
    }

    /**
     * Clone a time object and reset the day to 1970-01-01.
     *
     * @param value the time value
     * @return the time value without the date component
     */
    public static Time cloneAndNormalizeTime(Time value) {
        Calendar cal = getCalendar();
        long time;
        synchronized (cal) {
            cal.setTime(value);
            // month is 0 based
            cal.set(DEFAULT_YEAR, DEFAULT_MONTH - 1, DEFAULT_DAY);
            time = cal.getTime().getTime();
        }
        return new Time(time);
    }

    /**
     * Clone a date object and reset the hour, minutes, seconds, and
     * milliseconds to zero.
     *
     * @param value the date value
     * @return the date value at midnight
     */
    public static Date cloneAndNormalizeDate(Date value) {
        Calendar cal = getCalendar();
        long time;
        synchronized (cal) {
            cal.setTime(value);
            // if we don't enable lenient processing, dates between
            // 1916-06-03 and 1920-03-21,
            // 1940-06-15, 1947-03-16, and
            // 1966-05-22 to 1979-05-27 don't work
            // (central european timezone CET)
            cal.setLenient(true);
            cal.set(Calendar.MILLISECOND, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.HOUR_OF_DAY, DEFAULT_HOUR);
            time = cal.getTime().getTime();
        }
        return new Date(time);
    }

    /**
     * Convert the date from the specified time zone to UTC.
     *
     * @param x the date
     * @param source the calendar
     * @return the date in UTC
     */
    public static Value convertDateToUniversal(Date x, Calendar source) throws SQLException {
        return ValueDate.get(new Date(getUniversalTime(source, x)));
    }

    /**
     * Convert the time from the specified time zone to UTC.
     *
     * @param x the time
     * @param source the calendar
     * @return the time in UTC
     */
    public static Value convertTimeToUniversal(Time x, Calendar source) throws SQLException {
        return ValueTime.get(new Time(getUniversalTime(source, x)));
    }

    /**
     * Convert the timestamp from the specified time zone to UTC.
     *
     * @param x the time
     * @param source the calendar
     * @return the timestamp in UTC
     */
    public static Value convertTimestampToUniversal(Timestamp x, Calendar source) throws SQLException {
        Timestamp y = new Timestamp(getUniversalTime(source, x));
        // fix the nano seconds
        y.setNanos(x.getNanos());
        return ValueTimestamp.getNoCopy(y);
    }

    /**
     * Convert the date value to UTC using the given calendar.
     *
     * @param source the source calendar
     * @param x the date
     * @return the UTC number of milliseconds.
     */
    private static long getUniversalTime(Calendar source, java.util.Date x) throws SQLException {
        if (source == null) {
            throw Message.getInvalidValueException("calendar", null);
        }
        source = (Calendar) source.clone();
        Calendar universal = getCalendar();
        synchronized (universal) {
            source.setTime(x);
            convertTime(source, universal);
            return universal.getTime().getTime();
        }
    }

    private static long getLocalTime(java.util.Date x, Calendar target) throws SQLException {
        if (target == null) {
            throw Message.getInvalidValueException("calendar", null);
        }
        target = (Calendar) target.clone();
        Calendar local = Calendar.getInstance();
        synchronized (local) {
            local.setTime(x);
            convertTime(local, target);
        }
        return target.getTime().getTime();
    }

    private static void convertTime(Calendar from, Calendar to) {
        to.set(Calendar.YEAR, from.get(Calendar.YEAR));
        to.set(Calendar.MONTH, from.get(Calendar.MONTH));
        to.set(Calendar.DAY_OF_MONTH, from.get(Calendar.DAY_OF_MONTH));
        to.set(Calendar.HOUR_OF_DAY, from.get(Calendar.HOUR_OF_DAY));
        to.set(Calendar.MINUTE, from.get(Calendar.MINUTE));
        to.set(Calendar.SECOND, from.get(Calendar.SECOND));
        to.set(Calendar.MILLISECOND, from.get(Calendar.MILLISECOND));
    }

    /**
     * Convert the date to the specified time zone.
     *
     * @param x the date
     * @param calendar the calendar
     * @return the date using the correct time zone
     */
    public static Date convertDateToCalendar(Date x, Calendar calendar) throws SQLException {
        return x == null ? null : new Date(getLocalTime(x, calendar));
    }

    /**
     * Convert the time to the specified time zone.
     *
     * @param x the time
     * @param calendar the calendar
     * @return the time using the correct time zone
     */
    public static Time convertTimeToCalendar(Time x, Calendar calendar) throws SQLException {
        return x == null ? null : new Time(getLocalTime(x, calendar));
    }

    /**
     * Parse a date, time or timestamp value. This method supports the format
     * +/-year-month-day hour:minute:seconds.fractional and an optional timezone
     * part.
     *
     * @param original the original string
     * @param type the value type (Value.TIME, TIMESTAMP, or DATE)
     * @param errorCode the error code to use if an error occurs
     * @return the date object
     */
    public static java.util.Date parseDateTime(String original, int type, int errorCode) throws SQLException {
        String s = original;
        if (s == null) {
            return null;
        }
        try {
            int timeStart = 0;
            TimeZone tz = null;
            if (type == Value.TIME) {
                timeStart = 0;
            } else {
                timeStart = s.indexOf(' ') + 1;
                if (timeStart <= 0) {
                    // ISO 8601 compatibility
                    timeStart = s.indexOf('T') + 1;
                }
            }

            int year = DEFAULT_YEAR, month = DEFAULT_MONTH, day = DEFAULT_DAY;
            if (type != Value.TIME) {
                if (s.startsWith("+")) {
                    // +year
                    s = s.substring(1);
                }
                // start at position 1 to support -year
                int s1 = s.indexOf('-', 1);
                int s2 = s.indexOf('-', s1 + 1);
                if (s1 <= 0 || s2 <= s1) {
                    throw Message.getSQLException(errorCode, s, "format yyyy-mm-dd");
                }
                year = Integer.parseInt(s.substring(0, s1));
                month = Integer.parseInt(s.substring(s1 + 1, s2));
                int end = timeStart == 0 ? s.length() : timeStart - 1;
                day = Integer.parseInt(s.substring(s2 + 1, end));
            }
            int hour = DEFAULT_HOUR, minute = 0, second = 0, nano = 0;
            int s1 = s.indexOf(':', timeStart);
            if (type == Value.TIME || (type == Value.TIMESTAMP && s1 >= 0)) {
                int s2 = s.indexOf(':', s1 + 1);
                int s3 = s.indexOf('.', s2 + 1);
                if (s1 <= 0 || s2 <= s1) {
                    throw Message.getSQLException(errorCode, s, "format hh:mm:ss");
                }

                if (s.endsWith("Z")) {
                    s = s.substring(0, s.length() - 1);
                    tz = TimeZone.getTimeZone("UTC");
                } else {
                    int timeZoneStart = s.indexOf('+', s2 + 1);
                    if (timeZoneStart < 0) {
                        timeZoneStart = s.indexOf('-', s2 + 1);
                    }
                    if (timeZoneStart >= 0) {
                        String tzName = "GMT" + s.substring(timeZoneStart);
                        tz = TimeZone.getTimeZone(tzName);
                        if (!tz.getID().startsWith(tzName)) {
                            throw Message.getSQLException(errorCode, new String[] { s, tz.getID() + " <>" + tzName });
                        }
                        s = s.substring(0, timeZoneStart).trim();
                    }
                }

                hour = Integer.parseInt(s.substring(timeStart, s1));
                minute = Integer.parseInt(s.substring(s1 + 1, s2));
                if (s3 < 0) {
                    second = Integer.parseInt(s.substring(s2 + 1));
                } else {
                    second = Integer.parseInt(s.substring(s2 + 1, s3));
                    String n = (s + "000000000").substring(s3 + 1, s3 + 10);
                    nano = Integer.parseInt(n);
                }
            }
            long time;
            try {
                time = getTime(false, tz, year, month, day, hour, minute, second, type != Value.TIMESTAMP, nano);
            } catch (IllegalArgumentException e) {
                // special case: if the time simply doesn't exist because of
                // daylight saving time changes, use the lenient version
                String message = e.toString();
                if (message.indexOf("HOUR_OF_DAY") > 0) {
                    if (hour < 0 || hour > 23) {
                        throw e;
                    }
                    time = getTime(true, tz, year, month, day, hour, minute, second, type != Value.TIMESTAMP, nano);
                } else if (message.indexOf("DAY_OF_MONTH") > 0) {
                    int maxDay;
                    if (month == 2) {
                        maxDay = new GregorianCalendar().isLeapYear(year) ? 29 : 28;
                    } else {
                        maxDay = 30 + ((month + (month > 7 ? 1 : 0)) & 1);
                    }
                    if (day < 1 || day > maxDay) {
                        throw e;
                    }
                    // DAY_OF_MONTH is thrown for years > 2037
                    // using the timezone Brasilia and others,
                    // for example for 2042-10-12 00:00:00.
                    hour += 6;
                    time = getTime(true, tz, year, month, day, hour, minute, second, type != Value.TIMESTAMP, nano);
                } else {
                    throw e;
                }
            }
            switch (type) {
            case Value.DATE:
                return new java.sql.Date(time);
            case Value.TIME:
                return new java.sql.Time(time);
            case Value.TIMESTAMP: {
                Timestamp ts = new Timestamp(time);
                ts.setNanos(nano);
                return ts;
            }
            default:
                throw Message.throwInternalError("type:" + type);
            }
        } catch (IllegalArgumentException e) {
            throw Message.getSQLException(errorCode, e, original, e.toString());
        }
    }

    private static long getTime(boolean lenient, TimeZone tz, int year, int month, int day, int hour, int minute, int second, boolean setMillis, int nano) {
        Calendar c;
        if (tz == null) {
            c = getCalendar();
        } else {
            c = Calendar.getInstance(tz);
        }
        synchronized (c) {
            c.setLenient(lenient);
            if (year <= 0) {
                c.set(Calendar.ERA, GregorianCalendar.BC);
                c.set(Calendar.YEAR, 1 - year);
            } else {
                c.set(Calendar.ERA, GregorianCalendar.AD);
                c.set(Calendar.YEAR, year);
            }
            // january is 0
            c.set(Calendar.MONTH, month - 1);
            c.set(Calendar.DAY_OF_MONTH, day);
            c.set(Calendar.HOUR_OF_DAY, hour);
            c.set(Calendar.MINUTE, minute);
            c.set(Calendar.SECOND, second);
            if (setMillis) {
                c.set(Calendar.MILLISECOND, nano / 1000000);
            }
            return c.getTime().getTime();
        }
    }

    /**
     * Get the specified field of a date, however with years normalized to
     * positive or negative, and month starting with 1.
     *
     * @param d the date
     * @param field the field type
     * @return the value
     */
    public static int getDatePart(java.util.Date d, int field) {
        Calendar c = getCalendar();
        int value;
        synchronized (c) {
            c.setTime(d);
            value = c.get(field);
        }
        if (field == Calendar.MONTH) {
            value++;
        } else if (field == Calendar.YEAR) {
            if (c.get(Calendar.ERA) == GregorianCalendar.BC) {
                value = 1 - value;
            }
        }
        return value;
    }

    /**
     * Get the number of milliseconds since 1970-01-01 in the local timezone.
     *
     * @param d the date
     * @return the milliseconds
     */
    public static long getTimeLocal(java.util.Date d) {
        Calendar c = getCalendar();
        synchronized (c) {
            c.setTime(d);
            return c.getTime().getTime() + c.get(Calendar.ZONE_OFFSET);
        }
    }

    /**
     * Convert the number of milliseconds since 1970-01-01 in the local timezone
     * to GMT.
     *
     * @param millis the number of milliseconds in the local timezone
     * @return the number of milliseconds in GMT
     */
    public static long getTimeGMT(long millis) {
        Date d = new Date(millis);
        Calendar c = getCalendar();
        synchronized (c) {
            c.setTime(d);
            return c.getTime().getTime() - c.get(Calendar.ZONE_OFFSET);
        }
    }

}
