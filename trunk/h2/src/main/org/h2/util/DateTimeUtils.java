/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
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
import org.h2.constant.ErrorCode;
import org.h2.message.DbException;
import org.h2.value.DataType;
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

    private static final int SHIFT_YEAR = 9;
    private static final int SHIFT_MONTH = 5;

    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000L;
    private static final long NANOS_PER_DAY = MILLIS_PER_DAY * 1000000;

    private static final int[] NORMAL_DAYS_PER_MONTH = {
        0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };

    /**
     * Offsets of month within a year, starting with March, April,...
     */
    private static final int[] DAYS_OFFSET =
        { 0, 31, 61, 92, 122, 153, 184, 214, 245, 275, 306, 337, 366 };

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
     * Convert the timestamp to the specified time zone.
     *
     * @param x the timestamp
     * @param calendar the calendar
     * @return the timestamp using the correct time zone
     */
    public static Timestamp convertTimestampToCalendar(Timestamp x, Calendar calendar) {
        if (x != null) {
            Timestamp y = new Timestamp(getLocalTime(x, calendar));
            // fix the nano seconds
            y.setNanos(x.getNanos());
            x = y;
        }
        return x;
    }

    /**
     * Convert the date from the specified time zone to UTC.
     *
     * @param x the date
     * @param source the calendar
     * @return the date in UTC
     */
    public static Value convertDateToUTC(Date x, Calendar source) {
        return ValueDate.get(new Date(getUniversalTime(source, x)));
    }

    /**
     * Convert the time from the specified time zone to UTC.
     *
     * @param x the time
     * @param source the calendar
     * @return the time in UTC
     */
    public static Value convertTimeToUTC(Time x, Calendar source) {
        return ValueTime.get(new Time(getUniversalTime(source, x)));
    }

    /**
     * Convert the timestamp from the specified time zone to UTC.
     *
     * @param x the time
     * @param source the calendar
     * @return the timestamp in UTC
     */
    public static Value convertTimestampToUniversal(Timestamp x, Calendar source) {
        Timestamp y = new Timestamp(getUniversalTime(source, x));
        // fix the nano seconds
        y.setNanos(x.getNanos());
        return ValueTimestamp.get(y);
    }

    /**
     * Convert the date value to UTC using the given calendar.
     *
     * @param source the source calendar
     * @param x the date
     * @return the UTC number of milliseconds.
     */
    private static long getUniversalTime(Calendar source, java.util.Date x) {
        if (source == null) {
            throw DbException.getInvalidValueException("calendar", null);
        }
        source = (Calendar) source.clone();
        Calendar universal = getCalendar();
        synchronized (universal) {
            source.setTime(x);
            convertTime(source, universal);
            return universal.getTime().getTime();
        }
    }

    private static long getLocalTime(java.util.Date x, Calendar target) {
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
     * Convert the date to the specified time zone.
     *
     * @param x the date
     * @param calendar the calendar
     * @return the date using the correct time zone
     */
    public static Date convertDateToCalendar(Date x, Calendar calendar) {
        return x == null ? null : new Date(getLocalTime(x, calendar));
    }

    /**
     * Convert the time to the specified time zone.
     *
     * @param x the time
     * @param calendar the calendar
     * @return the time using the correct time zone
     */
    public static Time convertTimeToCalendar(Time x, Calendar calendar) {
        return x == null ? null : new Time(getLocalTime(x, calendar));
    }

    /**
     * Parse a date, time or timestamp value. This method supports the format
     * +/-year-month-day hour:minute:seconds.fractional and an optional timezone
     * part.
     *
     * @param original the original string
     * @param type the value type (Value.TIME, TIMESTAMP, or DATE)
     * @return the date object
     */
    public static Value parse(String original, int type) {
        String s = original;
        if (s == null) {
            return null;
        }
        try {
            int timeStart;
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
                    throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2,
                            DataType.getDataType(type).name, s);
                }
                year = Integer.parseInt(s.substring(0, s1));
                month = Integer.parseInt(s.substring(s1 + 1, s2));
                int end = timeStart == 0 ? s.length() : timeStart - 1;
                day = Integer.parseInt(s.substring(s2 + 1, end));
            }
            int hour = DEFAULT_HOUR, minute = 0, second = 0;
            long  millis = 0, nanos = 0;
            int s1 = s.indexOf(':', timeStart);
            if (type == Value.TIME || (type == Value.TIMESTAMP && s1 >= 0)) {
                int s2 = s.indexOf(':', s1 + 1);
                int s3 = s.indexOf('.', s2 + 1);
                if (s1 <= 0 || s2 <= s1) {
                    throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2,
                            DataType.getDataType(type).name, original);
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
                            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2,
                                    DataType.getDataType(type).name,
                                    original + " (" + tz.getID() + " <>" + tzName + ")");
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
                    nanos = Integer.parseInt(n);
                    millis = nanos / 1000000;
                    nanos -= millis * 1000000;
                }
            }
            if (!isValidDate(year, month, day)) {
                throw new IllegalArgumentException(year + "-" + month + "-" + day);
            }
            if (!isValidTime(hour, minute, second)) {
                throw new IllegalArgumentException(hour + ":" + minute + ":" + second);
            }
            long dateValue;
            if (tz == null) {
                dateValue = dateValue(year, month, day);
            } else {
                long ms = getMillis(tz, year, month, day, hour, minute, second, (int) millis);
                ms = DateTimeUtils.getLocalTime(new Date(ms),
                        Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                dateValue = dateValueFromDate(ms);
                // TODO verify this always works
                hour =  minute =  second = 0;
                millis = ms - absoluteDayFromDateValue(dateValue) * MILLIS_PER_DAY;
            }
            if (type == Value.DATE) {
                return ValueDate.get(dateValue);
            } else if (type == Value.TIMESTAMP) {
                nanos += (((((hour * 60L) + minute) * 60) + second) * 1000 + millis) * 1000000;
                return ValueTimestamp.get(dateValue, nanos);
            } else {
                throw DbException.throwInternalError("type:" + type);
            }
        } catch (IllegalArgumentException e) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2,
                    e, DataType.getDataType(type).name, original);
        }
    }

    public static long parseTime(String s) {
        int hour = 0, minute = 0, second = 0, nanos = 0;
        int s1 = s.indexOf(':');
        int s2 = s.indexOf(':', s1 + 1);
        int s3 = s.indexOf('.', s2 + 1);
        if (s1 <= 0 || s2 <= s1) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2,
                    "TIME", s);
        }
        try {
            hour = Integer.parseInt(s.substring(0, s1));
            minute = Integer.parseInt(s.substring(s1 + 1, s2));
            if (s3 < 0) {
                second = Integer.parseInt(s.substring(s2 + 1));
            } else {
                second = Integer.parseInt(s.substring(s2 + 1, s3));
                String n = (s + "000000000").substring(s3 + 1, s3 + 10);
                nanos = Integer.parseInt(n);
            }
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2,
                    "TIME", s);
        }
        if (minute < 0 || minute >= 60 || second < 0 || second >= 60) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2,
                    "TIME", s);
        }
        return ((((hour * 60L) + minute) * 60) + second) * 1000000000 + nanos;
    }

    /**
     * Calculate the milliseconds for the given date and time in the specified timezone.
     *
     * @param tz the timezone
     * @param year the absolute year (positive or negative)
     * @param month the month (1-12)
     * @param day the day (1-31)
     * @param hour the hour (0-23)
     * @param minute the minutes (0-59)
     * @param second the number of seconds (0-59)
     * @param millis the number of milliseconds
     * @return the number of milliseconds
     */
    public static long getMillis(TimeZone tz, int year, int month, int day, int hour, int minute, int second, int millis) {
        int todoInternal;
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
                return getTimeTry(true, tz, year, month, day, hour, minute, second, millis);
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
                return getTimeTry(true, tz, year, month, day, hour, minute, second, millis);
            } else {
                return getTimeTry(true, tz, year, month, day, hour, minute, second, millis);
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
            c.set(Calendar.MILLISECOND, millis);
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
     * Get the number of milliseconds since 1970-01-01 in the local timezone, but
     * without daylight saving time into account.
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
     *
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
    public static String formatDateTime(java.util.Date date, String format, String locale, String timeZone) {
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
    public static java.util.Date parseDateTime(String date, String format, String locale, String timeZone) {
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

    private static SimpleDateFormat getDateFormat(String format, String locale, String timeZone) {
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
            throw DbException.get(ErrorCode.PARSE_ERROR_1, e, format + "/" + locale + "/" + timeZone);
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
     * Verify if the specified time is valid.
     *
     * @param hour the hour
     * @param minute the minute
     * @param second the second
     * @return true if it is valid
     */
    public static boolean isValidTime(int hour, int minute, int second) {
        return hour >= 0 && hour < 24 &&
            minute >= 0 && minute < 60 &&
            second >= 0 && second < 60;
    }

    public static Date convertDateValueToDate(long dateValue) {
        long millis = getMillis(TimeZone.getDefault(),
                yearFromDateValue(dateValue),
                monthFromDateValue(dateValue),
                dayFromDateValue(dateValue), 0, 0, 0, 0);
        return new Date(millis);
    }

    public static Timestamp convertDateValueToTimestamp(long dateValue, long nanos) {
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

    private static int yearFromDateValue(long x) {
        return (int) (x >>> SHIFT_YEAR);
    }

    private static int monthFromDateValue(long x) {
        return (int) (x >>> SHIFT_MONTH) & 15;
    }

    public static int dayFromDateValue(long x) {
        return (int) (x & 31);
    }

    public static long dateValue(long year, int month, int day) {
        return (year << SHIFT_YEAR) | (month << SHIFT_MONTH) | day;
    }

    public static long dateValueFromDate(long ms) {
        Calendar cal = getCalendar();
        synchronized (cal) {
            cal.clear();
            cal.setTimeInMillis(ms);
            int year, month, day;
            year = getYear(cal);
            month = cal.get(Calendar.MONTH) + 1;
            day = cal.get(Calendar.DAY_OF_MONTH);
            return ((long) year << SHIFT_YEAR) | (month << SHIFT_MONTH) | day;
        }
    }

    public static long nanosFromDate(long ms) {
        Calendar cal = getCalendar();
        synchronized (cal) {
            cal.clear();
            cal.setTimeInMillis(ms);
            int h = cal.get(Calendar.HOUR_OF_DAY);
            int m = cal.get(Calendar.MINUTE);
            int s = cal.get(Calendar.SECOND);
            int millis = cal.get(Calendar.MILLISECOND);
            return ((((((h * 60L) + m) * 60) + s) * 1000) + millis) * 1000000;
        }
    }

    public static ValueTimestamp normalize(long absoluteDay, long nanos) {
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
        return ValueTimestamp.get(dateValueFromAbsoluteDay(absoluteDay), nanos);
    }

    public static long absoluteDayFromDateValue(long dateValue) {
        long y = yearFromDateValue(dateValue);
        int m = monthFromDateValue(dateValue);
        int d = dayFromDateValue(dateValue);
        if (m <= 2) {
            y--;
            m += 12;
        }
        long a = ((y * 2922L) >>> 3) + DAYS_OFFSET[m - 3] + d - 719484;
        if (y <= 1582 && ((y < 1582) || (m * 100 + d < 1005))) {
            // Julian calendar (cutover at 1582-10-04 / 1582-10-15)
            a += 13;
        } else if (y < 1901 || y > 2099) {
            // Gregorian calendar (slow mode)
            a += (y / 400) - (y / 100) + 15;
        }
        return a;
    }

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

    public static void appendDate(StringBuilder buff, long dateValue) {
        int y = DateTimeUtils.yearFromDateValue(dateValue);
        int m = DateTimeUtils.monthFromDateValue(dateValue);
        int d = DateTimeUtils.dayFromDateValue(dateValue);
        if (y > 0 && y < 10000) {
            StringUtils.appendZeroPadded(buff, 4, y);
        } else {
            buff.append(y);
        }
        buff.append('-');
        StringUtils.appendZeroPadded(buff, 2, m);
        buff.append('-');
        StringUtils.appendZeroPadded(buff, 2, d);
    }

    public static void appendTime(StringBuilder buff, long n, boolean alwaysAddMillis) {
        long ms = n / 1000000;
        n -= ms * 1000000;
        long s = ms / 1000;
        ms -= s * 1000;
        long m = s / 60;
        s -= m * 60;
        long h = m / 60;
        m -= h * 60;
        if (h < 0) {
            buff.append(h);
        } else {
            StringUtils.appendZeroPadded(buff, 2, h);
        }
        buff.append(':');
        StringUtils.appendZeroPadded(buff, 2, m);
        buff.append(':');
        StringUtils.appendZeroPadded(buff, 2, s);
        if (ms > 0 || n > 0) {
            buff.append('.');
            int start = buff.length();
            StringUtils.appendZeroPadded(buff, 3, ms);
            if (n > 0) {
                StringUtils.appendZeroPadded(buff, 6, n);
            }
            for (int i = buff.length() - 1; i > start; i--) {
                if (buff.charAt(i) != '0') {
                    break;
                }
                buff.deleteCharAt(i);
            }
        } else if (alwaysAddMillis) {
            buff.append(".0");
        }
    }

}
