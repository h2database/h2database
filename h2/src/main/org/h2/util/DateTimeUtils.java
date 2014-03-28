/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 * Iso8601:
 * Initial Developer: Robert Rathsack (firstName dot lastName at gmx dot de)
 */
package org.h2.util;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueNull;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

/**
 * This utility class contains time conversion functions.
 * <p>
 * Date value: a bit field with bits for the year, month, and day.
 * Absolute day: the day number (0 means 1970-01-01).
 */
public class DateTimeUtils {

    /**
     * The number of milliseconds per day.
     */
    public static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000L;

    private static final long NANOS_PER_DAY = MILLIS_PER_DAY * 1000000;

    private static final int SHIFT_YEAR = 9;
    private static final int SHIFT_MONTH = 5;

    private static final int[] NORMAL_DAYS_PER_MONTH = { 0, 31, 28, 31, 30, 31,
            30, 31, 31, 30, 31, 30, 31 };

    /**
     * Offsets of month within a year, starting with March, April,...
     */
    private static final int[] DAYS_OFFSET = { 0, 31, 61, 92, 122, 153, 184,
            214, 245, 275, 306, 337, 366 };

    private static int zoneOffset;
    private static Calendar cachedCalendar;

    private DateTimeUtils() {
        // utility class
    }

    static {
        getCalendar();
    }

    /**
     * Reset the calendar, for example after changing the default timezone.
     */
    public static void resetCalendar() {
        cachedCalendar = null;
        getCalendar();
    }

    private static Calendar getCalendar() {
        if (cachedCalendar == null) {
            cachedCalendar = Calendar.getInstance();
            zoneOffset = cachedCalendar.get(Calendar.ZONE_OFFSET);
        }
        return cachedCalendar;
    }

    /**
     * Convert the date to the specified time zone.
     *
     * @param value the date (might be ValueNull)
     * @param calendar the calendar
     * @return the date using the correct time zone
     */
    public static Date convertDate(Value value, Calendar calendar) {
        if (value == ValueNull.INSTANCE) {
            return null;
        }
        ValueDate d = (ValueDate) value.convertTo(Value.DATE);
        Calendar cal = (Calendar) calendar.clone();
        cal.clear();
        cal.setLenient(true);
        long dateValue = d.getDateValue();
        setCalendarFields(cal, yearFromDateValue(dateValue),
                monthFromDateValue(dateValue),
                dayFromDateValue(dateValue),
                0, 0, 0, 0);
        long ms = cal.getTimeInMillis();
        return new Date(ms);
    }

    /**
     * Convert the time to the specified time zone.
     *
     * @param value the time (might be ValueNull)
     * @param calendar the calendar
     * @return the time using the correct time zone
     */
    public static Time convertTime(Value value, Calendar calendar) {
        if (value == ValueNull.INSTANCE) {
            return null;
        }
        ValueTime t = (ValueTime) value.convertTo(Value.TIME);
        Calendar cal = (Calendar) calendar.clone();
        cal.clear();
        cal.setLenient(true);
        long nanos = t.getNanos();
        long millis = nanos / 1000000;
        nanos -= millis * 1000000;
        long s = millis / 1000;
        millis -= s * 1000;
        long m = s / 60;
        s -= m * 60;
        long h = m / 60;
        m -= h * 60;
        setCalendarFields(cal, 1970, 1, 1,
                (int) h, (int) m, (int) s, (int) millis);
        long ms = cal.getTimeInMillis();
        return new Time(ms);
    }

    /**
     * Convert the timestamp to the specified time zone.
     *
     * @param value the timestamp (might be ValueNull)
     * @param calendar the calendar
     * @return the timestamp using the correct time zone
     */
    public static Timestamp convertTimestamp(Value value, Calendar calendar) {
        if (value == ValueNull.INSTANCE) {
            return null;
        }
        ValueTimestamp ts = (ValueTimestamp) value.convertTo(Value.TIMESTAMP);
        Calendar cal = (Calendar) calendar.clone();
        cal.clear();
        cal.setLenient(true);
        long dateValue = ts.getDateValue();
        long nanos = ts.getNanos();
        long millis = nanos / 1000000;
        nanos -= millis * 1000000;
        long s = millis / 1000;
        millis -= s * 1000;
        long m = s / 60;
        s -= m * 60;
        long h = m / 60;
        m -= h * 60;
        setCalendarFields(cal, yearFromDateValue(dateValue),
                monthFromDateValue(dateValue),
                dayFromDateValue(dateValue),
                (int) h, (int) m, (int) s, (int) millis);
        long ms = cal.getTimeInMillis();
        Timestamp x = new Timestamp(ms);
        x.setNanos((int) (nanos + millis * 1000000));
        return x;
    }

    /**
     * Convert the date using the specified calendar.
     *
     * @param x the date
     * @param calendar the calendar
     * @return the date
     */
    public static ValueDate convertDate(Date x, Calendar calendar) {
        if (calendar == null) {
            throw DbException.getInvalidValueException("calendar", null);
        }
        Calendar cal = (Calendar) calendar.clone();
        cal.setTimeInMillis(x.getTime());
        long dateValue = dateValueFromCalendar(cal);
        return ValueDate.fromDateValue(dateValue);
    }

    /**
     * Convert the time using the specified calendar.
     *
     * @param x the time
     * @param calendar the calendar
     * @return the time
     */
    public static ValueTime convertTime(Time x, Calendar calendar) {
        if (calendar == null) {
            throw DbException.getInvalidValueException("calendar", null);
        }
        Calendar cal = (Calendar) calendar.clone();
        cal.setTimeInMillis(x.getTime());
        long nanos = nanosFromCalendar(cal);
        return ValueTime.fromNanos(nanos);
    }

    /**
     * Convert a date to the specified time zone.
     *
     * @param x the date to convert
     * @param target the calendar with the target timezone
     * @return the milliseconds in UTC
     */
    public static long convertToLocal(java.util.Date x, Calendar target) {
        if (target == null) {
            throw DbException.getInvalidValueException("calendar", null);
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
        to.set(Calendar.ERA, from.get(Calendar.ERA));
        to.set(Calendar.YEAR, from.get(Calendar.YEAR));
        to.set(Calendar.MONTH, from.get(Calendar.MONTH));
        to.set(Calendar.DAY_OF_MONTH, from.get(Calendar.DAY_OF_MONTH));
        to.set(Calendar.HOUR_OF_DAY, from.get(Calendar.HOUR_OF_DAY));
        to.set(Calendar.MINUTE, from.get(Calendar.MINUTE));
        to.set(Calendar.SECOND, from.get(Calendar.SECOND));
        to.set(Calendar.MILLISECOND, from.get(Calendar.MILLISECOND));
    }

    /**
     * Convert the timestamp using the specified calendar.
     *
     * @param x the time
     * @param calendar the calendar
     * @return the timestamp
     */
    public static ValueTimestamp convertTimestamp(Timestamp x, Calendar calendar) {
        if (calendar == null) {
            throw DbException.getInvalidValueException("calendar", null);
        }
        Calendar cal = (Calendar) calendar.clone();
        cal.setTimeInMillis(x.getTime());
        long dateValue = dateValueFromCalendar(cal);
        long nanos = nanosFromCalendar(cal);
        nanos += x.getNanos() % 1000000;
        return ValueTimestamp.fromDateValueAndNanos(dateValue, nanos);
    }

    /**
     * Parse a date string. The format is: [+|-]year-month-day
     *
     * @param s the string to parse
     * @param start the parse index start
     * @param end the parse index end
     * @return the date value
     * @throws IllegalArgumentException if there is a problem
     */
    public static long parseDateValue(String s, int start, int end) {
        if (s.charAt(start) == '+') {
            // +year
            start++;
        }
        // start at position 1 to support "-year"
        int s1 = s.indexOf('-', start + 1);
        int s2 = s.indexOf('-', s1 + 1);
        if (s1 <= 0 || s2 <= s1) {
            throw new IllegalArgumentException(s);
        }
        int year = Integer.parseInt(s.substring(start, s1));
        int month = Integer.parseInt(s.substring(s1 + 1, s2));
        int day = Integer.parseInt(s.substring(s2 + 1, end));
        if (!isValidDate(year, month, day)) {
            throw new IllegalArgumentException(year + "-" + month + "-" + day);
        }
        return dateValue(year, month, day);
    }

    /**
     * Parse a time string. The format is: [-]hour:minute:second[.nanos]
     *
     * @param s the string to parse
     * @param start the parse index start
     * @param end the parse index end
     * @param timeOfDay whether the result need to be within 0 (inclusive) and 1
     *            day (exclusive)
     * @return the time in nanoseconds
     * @throws IllegalArgumentException if there is a problem
     */
    public static long parseTimeNanos(String s, int start, int end,
            boolean timeOfDay) {
        int hour = 0, minute = 0, second = 0;
        long nanos = 0;
        int s1 = s.indexOf(':', start);
        int s2 = s.indexOf(':', s1 + 1);
        int s3 = s.indexOf('.', s2 + 1);
        if (s1 <= 0 || s2 <= s1) {
            throw new IllegalArgumentException(s);
        }
        boolean negative;
        hour = Integer.parseInt(s.substring(start, s1));
        if (hour < 0) {
            if (timeOfDay) {
                throw new IllegalArgumentException(s);
            }
            negative = true;
            hour = -hour;
        } else {
            negative = false;
        }
        minute = Integer.parseInt(s.substring(s1 + 1, s2));
        if (s3 < 0) {
            second = Integer.parseInt(s.substring(s2 + 1, end));
        } else {
            second = Integer.parseInt(s.substring(s2 + 1, s3));
            String n = (s.substring(s3 + 1, end) + "000000000").substring(0, 9);
            nanos = Integer.parseInt(n);
        }
        if (hour >= 2000000 || minute < 0 ||
                minute >= 60 || second < 0 || second >= 60) {
            throw new IllegalArgumentException(s);
        }
        if (timeOfDay && hour >= 24) {
            throw new IllegalArgumentException(s);
        }
        nanos += ((((hour * 60L) + minute) * 60) + second) * 1000000000;
        return negative ? -nanos : nanos;
    }

    /**
     * Calculate the milliseconds since 1970-01-01 (UTC) for the given date and
     * time (in the specified timezone).
     *
     * @param tz the timezone of the parameters
     * @param year the absolute year (positive or negative)
     * @param month the month (1-12)
     * @param day the day (1-31)
     * @param hour the hour (0-23)
     * @param minute the minutes (0-59)
     * @param second the number of seconds (0-59)
     * @param millis the number of milliseconds
     * @return the number of milliseconds (UTC)
     */
    public static long getMillis(TimeZone tz, int year, int month, int day,
            int hour, int minute, int second, int millis) {
        try {
            return getTimeTry(false, tz, year, month, day, hour, minute, second, millis);
        } catch (IllegalArgumentException e) {
            // special case: if the time simply doesn't exist because of
            // daylight saving time changes, use the lenient version
            String message = e.toString();
            if (message.indexOf("HOUR_OF_DAY") > 0) {
                if (hour < 0 || hour > 23) {
                    throw e;
                }
                return getTimeTry(true, tz, year, month, day, hour, minute,
                        second, millis);
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
                return getTimeTry(true, tz, year, month, day, hour, minute,
                        second, millis);
            } else {
                return getTimeTry(true, tz, year, month, day, hour, minute,
                        second, millis);
            }
        }
    }

    private static long getTimeTry(boolean lenient, TimeZone tz,
            int year, int month, int day, int hour, int minute, int second,
            int millis) {
        Calendar c;
        if (tz == null) {
            c = getCalendar();
        } else {
            c = Calendar.getInstance(tz);
        }
        synchronized (c) {
            c.clear();
            c.setLenient(lenient);
            setCalendarFields(c, year, month, day, hour, minute, second, millis);
            return c.getTime().getTime();
        }
    }

    private static void setCalendarFields(Calendar cal, int year, int month,
            int day, int hour, int minute, int second, int millis) {
        if (year <= 0) {
            cal.set(Calendar.ERA, GregorianCalendar.BC);
            cal.set(Calendar.YEAR, 1 - year);
        } else {
            cal.set(Calendar.ERA, GregorianCalendar.AD);
            cal.set(Calendar.YEAR, year);
        }
        // january is 0
        cal.set(Calendar.MONTH, month - 1);
        cal.set(Calendar.DAY_OF_MONTH, day);
        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, minute);
        cal.set(Calendar.SECOND, second);
        cal.set(Calendar.MILLISECOND, millis);
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
        synchronized (c) {
            c.setTime(d);
            if (field == Calendar.YEAR) {
                return getYear(c);
            }
            int value = c.get(field);
            if (field == Calendar.MONTH) {
                return value + 1;
            }
            return value;
        }
    }

    /**
     * Get the year (positive or negative) from a calendar.
     *
     * @param calendar the calendar
     * @return the year
     */
    private static int getYear(Calendar calendar) {
        int year = calendar.get(Calendar.YEAR);
        if (calendar.get(Calendar.ERA) == GregorianCalendar.BC) {
            year = 1 - year;
        }
        return year;
    }

    /**
     * Get the number of milliseconds since 1970-01-01 in the local timezone,
     * but without daylight saving time into account.
     *
     * @param d the date
     * @return the milliseconds
     */
    public static long getTimeLocalWithoutDst(java.util.Date d) {
        return d.getTime() + zoneOffset;
    }

    /**
     * Convert the number of milliseconds since 1970-01-01 in the local timezone
     * to UTC, but without daylight saving time into account.
     *
     * @param millis the number of milliseconds in the local timezone
     * @return the number of milliseconds in UTC
     */
    public static long getTimeUTCWithoutDst(long millis) {
        return millis - zoneOffset;
    }

    /**
     * Return the day of week according to the ISO 8601 specification. Week
     * starts at Monday. See also http://en.wikipedia.org/wiki/ISO_8601
     *
     * @author Robert Rathsack
     *
     * @param date the date object which day of week should be calculated
     * @return the day of the week, Monday as 1 to Sunday as 7
     */
    public static int getIsoDayOfWeek(java.util.Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(date.getTime());
        int val = cal.get(Calendar.DAY_OF_WEEK) - 1;
        return val == 0 ? 7 : val;
    }

    /**
     * Returns the week of the year according to the ISO 8601 specification. The
     * spec defines the first week of the year as the week which contains at
     * least 4 days of the new year. The week starts at Monday. Therefore
     * December 29th - 31th could belong to the next year and January 1st - 3th
     * could belong to the previous year. If January 1st is on Thursday (or
     * earlier) it belongs to the first week, otherwise to the last week of the
     * previous year. Hence January 4th always belongs to the first week while
     * the December 28th always belongs to the last week.
     *
     * @author Robert Rathsack
     * @param date the date object which week of year should be calculated
     * @return the week of the year
     */
    public static int getIsoWeek(java.util.Date date) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(date.getTime());
        c.setFirstDayOfWeek(Calendar.MONDAY);
        c.setMinimalDaysInFirstWeek(4);
        return c.get(Calendar.WEEK_OF_YEAR);
    }

    /**
     * Returns the year according to the ISO week definition.
     *
     * @author Robert Rathsack
     *
     * @param date the date object which year should be calculated
     * @return the year
     */
    public static int getIsoYear(java.util.Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(date.getTime());
        cal.setFirstDayOfWeek(Calendar.MONDAY);
        cal.setMinimalDaysInFirstWeek(4);
        int year = getYear(cal);
        int month = cal.get(Calendar.MONTH);
        int week = cal.get(Calendar.WEEK_OF_YEAR);
        if (month == 0 && week > 51) {
            year--;
        } else if (month == 11 && week == 1) {
            year++;
        }
        return year;
    }

    /**
     * Formats a date using a format string.
     *
     * @param date the date to format
     * @param format the format string
     * @param locale the locale
     * @param timeZone the timezone
     * @return the formatted date
     */
    public static String formatDateTime(java.util.Date date, String format,
            String locale, String timeZone) {
        SimpleDateFormat dateFormat = getDateFormat(format, locale, timeZone);
        synchronized (dateFormat) {
            return dateFormat.format(date);
        }
    }

    /**
     * Parses a date using a format string.
     *
     * @param date the date to parse
     * @param format the parsing format
     * @param locale the locale
     * @param timeZone the timeZone
     * @return the parsed date
     */
    public static java.util.Date parseDateTime(String date, String format,
            String locale, String timeZone) {
        SimpleDateFormat dateFormat = getDateFormat(format, locale, timeZone);
        try {
            synchronized (dateFormat) {
                return dateFormat.parse(date);
            }
        } catch (Exception e) {
            // ParseException
            throw DbException.get(ErrorCode.PARSE_ERROR_1, e, date);
        }
    }

    private static SimpleDateFormat getDateFormat(String format, String locale,
            String timeZone) {
        try {
            // currently, a new instance is create for each call
            // however, could cache the last few instances
            SimpleDateFormat df;
            if (locale == null) {
                df = new SimpleDateFormat(format);
            } else {
                Locale l = new Locale(locale);
                df = new SimpleDateFormat(format, l);
            }
            if (timeZone != null) {
                df.setTimeZone(TimeZone.getTimeZone(timeZone));
            }
            return df;
        } catch (Exception e) {
            throw DbException.get(ErrorCode.PARSE_ERROR_1, e,
                    format + "/" + locale + "/" + timeZone);
        }
    }

    /**
     * Verify if the specified date is valid.
     *
     * @param year the year
     * @param month the month (January is 1)
     * @param day the day (1 is the first of the month)
     * @return true if it is valid
     */
    public static boolean isValidDate(int year, int month, int day) {
        if (month < 1 || month > 12 || day < 1) {
            return false;
        }
        if (year > 1582) {
            // Gregorian calendar
            if (month != 2) {
                return day <= NORMAL_DAYS_PER_MONTH[month];
            }
            // February
            if ((year & 3) != 0) {
                return day <= 28;
            }
            return day <= ((year % 100 != 0) || (year % 400 == 0) ? 29 : 28);
        } else if (year == 1582 && month == 10) {
            // special case: days 1582-10-05 .. 1582-10-14 don't exist
            return day <= 31 && (day < 5 || day > 14);
        }
        if (month != 2 && day <= NORMAL_DAYS_PER_MONTH[month]) {
            return true;
        }
        return day <= ((year & 3) != 0 ? 28 : 29);
    }

    /**
     * Convert a date value to a date, using the default timezone.
     *
     * @param dateValue the date value
     * @return the date
     */
    public static Date convertDateValueToDate(long dateValue) {
        long millis = getMillis(TimeZone.getDefault(),
                yearFromDateValue(dateValue),
                monthFromDateValue(dateValue),
                dayFromDateValue(dateValue), 0, 0, 0, 0);
        return new Date(millis);
    }

    /**
     * Convert a date value / time value to a timestamp, using the default
     * timezone.
     *
     * @param dateValue the date value
     * @param nanos the nanoseconds since midnight
     * @return the timestamp
     */
    public static Timestamp convertDateValueToTimestamp(long dateValue,
            long nanos) {
        long millis = nanos / 1000000;
        nanos -= millis * 1000000;
        long s = millis / 1000;
        millis -= s * 1000;
        long m = s / 60;
        s -= m * 60;
        long h = m / 60;
        m -= h * 60;
        long ms = getMillis(TimeZone.getDefault(),
                yearFromDateValue(dateValue),
                monthFromDateValue(dateValue),
                dayFromDateValue(dateValue),
                (int) h, (int) m, (int) s, 0);
        Timestamp ts = new Timestamp(ms);
        ts.setNanos((int) (nanos + millis * 1000000));
        return ts;
    }

    /**
     * Convert a time value to a time, using the default
     * timezone.
     *
     * @param nanos the nanoseconds since midnight
     * @return the time
     */
    public static Time convertNanoToTime(long nanos) {
        long millis = nanos / 1000000;
        long s = millis / 1000;
        millis -= s * 1000;
        long m = s / 60;
        s -= m * 60;
        long h = m / 60;
        m -= h * 60;
        long ms = getMillis(TimeZone.getDefault(),
                1970, 1, 1, (int) (h % 24), (int) m, (int) s, (int) millis);
        return new Time(ms);
    }

    /**
     * Get the year from a date value.
     *
     * @param x the date value
     * @return the year
     */
    public static int yearFromDateValue(long x) {
        return (int) (x >>> SHIFT_YEAR);
    }

    /**
     * Get the month from a date value.
     *
     * @param x the date value
     * @return the month (1..12)
     */
    public static int monthFromDateValue(long x) {
        return (int) (x >>> SHIFT_MONTH) & 15;
    }

    /**
     * Get the day of month from a date value.
     *
     * @param x the date value
     * @return the day (1..31)
     */
    public static int dayFromDateValue(long x) {
        return (int) (x & 31);
    }

    /**
     * Get the date value from a given date.
     *
     * @param year the year
     * @param month the month (1..12)
     * @param day the day (1..31)
     * @return the date value
     */
    public static long dateValue(long year, int month, int day) {
        return (year << SHIFT_YEAR) | (month << SHIFT_MONTH) | day;
    }

    /**
     * Calculate the date value (in the default timezone) from a given time in
     * milliseconds in UTC.
     *
     * @param ms the milliseconds
     * @return the date value
     */
    public static long dateValueFromDate(long ms) {
        Calendar cal = getCalendar();
        synchronized (cal) {
            cal.clear();
            cal.setTimeInMillis(ms);
            return dateValueFromCalendar(cal);
        }
    }

    /**
     * Calculate the date value from a given calendar.
     *
     * @param cal the calendar
     * @return the date value
     */
    private static long dateValueFromCalendar(Calendar cal) {
        int year, month, day;
        year = getYear(cal);
        month = cal.get(Calendar.MONTH) + 1;
        day = cal.get(Calendar.DAY_OF_MONTH);
        return ((long) year << SHIFT_YEAR) | (month << SHIFT_MONTH) | day;
    }

    /**
     * Calculate the nanoseconds since midnight (in the default timezone) from a
     * given time in milliseconds in UTC.
     *
     * @param ms the milliseconds
     * @return the nanoseconds
     */
    public static long nanosFromDate(long ms) {
        Calendar cal = getCalendar();
        synchronized (cal) {
            cal.clear();
            cal.setTimeInMillis(ms);
            return nanosFromCalendar(cal);
        }
    }

    /**
     * Calculate the nanoseconds since midnight from a given calendar.
     *
     * @param cal the calendar
     * @return the nanoseconds
     */
    private static long nanosFromCalendar(Calendar cal) {
        int h = cal.get(Calendar.HOUR_OF_DAY);
        int m = cal.get(Calendar.MINUTE);
        int s = cal.get(Calendar.SECOND);
        int millis = cal.get(Calendar.MILLISECOND);
        return ((((((h * 60L) + m) * 60) + s) * 1000) + millis) * 1000000;
    }

    /**
     * Calculate the normalized timestamp.
     *
     * @param absoluteDay the absolute day
     * @param nanos the nanoseconds (may be negative or larger than one day)
     * @return the timestamp
     */
    public static ValueTimestamp normalizeTimestamp(long absoluteDay, long nanos) {
        if (nanos > NANOS_PER_DAY || nanos < 0) {
            long d;
            if (nanos > NANOS_PER_DAY) {
                d = nanos / NANOS_PER_DAY;
            } else {
                d = (nanos - NANOS_PER_DAY + 1) / NANOS_PER_DAY;
            }
            nanos -= d * NANOS_PER_DAY;
            absoluteDay += d;
        }
        return ValueTimestamp.fromDateValueAndNanos(
                dateValueFromAbsoluteDay(absoluteDay), nanos);
    }

    /**
     * Calculate the absolute day from a date value.
     *
     * @param dateValue the date value
     * @return the absolute day
     */
    public static long absoluteDayFromDateValue(long dateValue) {
        long y = yearFromDateValue(dateValue);
        int m = monthFromDateValue(dateValue);
        int d = dayFromDateValue(dateValue);
        if (m <= 2) {
            y--;
            m += 12;
        }
        long a = ((y * 2922L) >> 3) + DAYS_OFFSET[m - 3] + d - 719484;
        if (y <= 1582 && ((y < 1582) || (m * 100 + d < 1005))) {
            // Julian calendar (cutover at 1582-10-04 / 1582-10-15)
            a += 13;
        } else if (y < 1901 || y > 2099) {
            // Gregorian calendar (slow mode)
            a += (y / 400) - (y / 100) + 15;
        }
        return a;
    }

    /**
     * Calculate the date value from an absolute day.
     *
     * @param absoluteDay the absolute day
     * @return the date value
     */
    public static long dateValueFromAbsoluteDay(long absoluteDay) {
        long d = absoluteDay + 719468;
        long y100 = 0, offset;
        if (d > 578040) {
            // Gregorian calendar
            long y400 = d / 146097;
            d -= y400 * 146097;
            y100 = d / 36524;
            d -= y100 * 36524;
            offset = y400 * 400 + y100 * 100;
        } else {
            // Julian calendar
            d += 292200000002L;
            offset = -800000000;
        }
        long y4 = d / 1461;
        d -= y4 * 1461;
        long y = d / 365;
        d -= y * 365;
        if (d == 0 && (y == 4 || y100 == 4)) {
            y--;
            d += 365;
        }
        y += offset + y4 * 4;
        // month of a day
        int m = ((int) d * 2 + 1) * 5 / 306;
        d -= DAYS_OFFSET[m] - 1;
        if (m >= 10) {
            y++;
            m -= 12;
        }
        return dateValue(y, m + 3, (int) d);
    }

}
