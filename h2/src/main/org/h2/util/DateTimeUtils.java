/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 * Iso8601: Initial Developer: Robert Rathsack (firstName dot lastName at gmx
 * dot de)
 */
package org.h2.util;

import java.time.Instant;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimeTimeZone;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * This utility class contains time conversion functions.
 * <p>
 * Date value: a bit field with bits for the year, month, and day. Absolute day:
 * the day number (0 means 1970-01-01).
 */
public class DateTimeUtils {

    /**
     * The number of milliseconds per day.
     */
    public static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000L;

    /**
     * The number of seconds per day.
     */
    public static final long SECONDS_PER_DAY = 24 * 60 * 60;

    /**
     * The number of nanoseconds per second.
     */
    public static final long NANOS_PER_SECOND = 1_000_000_000;

    /**
     * The number of nanoseconds per minute.
     */
    public static final long NANOS_PER_MINUTE = 60 * NANOS_PER_SECOND;

    /**
     * The number of nanoseconds per hour.
     */
    public static final long NANOS_PER_HOUR = 60 * NANOS_PER_MINUTE;

    /**
     * The number of nanoseconds per day.
     */
    public static final long NANOS_PER_DAY = MILLIS_PER_DAY * 1_000_000;

    /**
     * The offset of year bits in date values.
     */
    public static final int SHIFT_YEAR = 9;

    /**
     * The offset of month bits in date values.
     */
    public static final int SHIFT_MONTH = 5;

    /**
     * Date value for 1970-01-01.
     */
    public static final int EPOCH_DATE_VALUE = (1970 << SHIFT_YEAR) + (1 << SHIFT_MONTH) + 1;

    /**
     * Minimum possible date value.
     */
    public static final long MIN_DATE_VALUE = (-1_000_000_000L << SHIFT_YEAR) + (1 << SHIFT_MONTH) + 1;

    /**
     * Maximum possible date value.
     */
    public static final long MAX_DATE_VALUE = (1_000_000_000L << SHIFT_YEAR) + (12 << SHIFT_MONTH) + 31;

    private static final int[] NORMAL_DAYS_PER_MONTH = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

    /**
     * Multipliers for {@link #convertScale(long, int, long)} and
     * {@link #appendNanos(StringBuilder, int)}.
     */
    static final int[] FRACTIONAL_SECONDS_TABLE = { 1_000_000_000, 100_000_000,
            10_000_000, 1_000_000, 100_000, 10_000, 1_000, 100, 10, 1 };

    private static volatile TimeZoneProvider LOCAL;

    private DateTimeUtils() {
        // utility class
    }

    /**
     * Reset the cached calendar for default timezone, for example after
     * changing the default timezone.
     */
    public static void resetCalendar() {
        LOCAL = null;
    }

    /**
     * Get the time zone provider for the default time zone.
     *
     * @return the time zone provider for the default time zone
     */
    public static TimeZoneProvider getTimeZone() {
        TimeZoneProvider local = LOCAL;
        if (local == null) {
            LOCAL = local = TimeZoneProvider.getDefault();
        }
        return local;
    }

    /**
     * Returns current timestamp.
     *
     * @param timeZone
     *            the time zone
     * @return current timestamp
     */
    public static ValueTimestampTimeZone currentTimestamp(TimeZoneProvider timeZone) {
        return currentTimestamp(timeZone, Instant.now());
    }

    /**
     * Returns current timestamp using the specified instant for its value.
     *
     * @param timeZone
     *            the time zone
     * @param now
     *            timestamp source, must be greater than or equal to
     *            1970-01-01T00:00:00Z
     * @return current timestamp
     */
    public static ValueTimestampTimeZone currentTimestamp(TimeZoneProvider timeZone, Instant now) {
        /*
         * This code intentionally does not support properly dates before UNIX
         * epoch because such support is not required for current dates.
         */
        long second = now.getEpochSecond();
        int offset = timeZone.getTimeZoneOffsetUTC(second);
        second += offset;
        return ValueTimestampTimeZone.fromDateValueAndNanos(dateValueFromAbsoluteDay(second / SECONDS_PER_DAY),
                second % SECONDS_PER_DAY * 1_000_000_000 + now.getNano(), offset);
    }

    /**
     * Parse a date string. The format is: [+|-]year-month-day
     * or [+|-]yyyyMMdd.
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
        int yEnd = s.indexOf('-', start + 1);
        int mStart, mEnd, dStart;
        if (yEnd > 0) {
            // Standard [+|-]year-month-day format
            mStart = yEnd + 1;
            mEnd = s.indexOf('-', mStart);
            if (mEnd <= mStart) {
                throw new IllegalArgumentException(s);
            }
            dStart = mEnd + 1;
        } else {
            // Additional [+|-]yyyyMMdd format for compatibility
            mEnd = dStart = end - 2;
            yEnd = mStart = mEnd - 2;
            // Accept only 3 or more digits in year for now
            if (yEnd < start + 3) {
                throw new IllegalArgumentException(s);
            }
        }
        int year = Integer.parseInt(s.substring(start, yEnd));
        int month = StringUtils.parseUInt31(s, mStart, mEnd);
        int day = StringUtils.parseUInt31(s, dStart, end);
        if (!isValidDate(year, month, day)) {
            throw new IllegalArgumentException(year + "-" + month + "-" + day);
        }
        return dateValue(year, month, day);
    }

    /**
     * Parse a time string. The format is: hour:minute[:second[.nanos]],
     * hhmm[ss[.nanos]], or hour.minute.second[.nanos].
     *
     * @param s the string to parse
     * @param start the parse index start
     * @param end the parse index end
     * @return the time in nanoseconds
     * @throws IllegalArgumentException if there is a problem
     */
    public static long parseTimeNanos(String s, int start, int end) {
        int hour, minute, second, nanos;
        int hEnd = s.indexOf(':', start);
        int mStart, mEnd, sStart, sEnd;
        if (hEnd > 0) {
            mStart = hEnd + 1;
            mEnd = s.indexOf(':', mStart);
            if (mEnd >= mStart) {
                // Standard hour:minute:second[.nanos] format
                sStart = mEnd + 1;
                sEnd = s.indexOf('.', sStart);
            } else {
                // Additional hour:minute format for compatibility
                mEnd = end;
                sStart = sEnd = -1;
            }
        } else {
            int t = s.indexOf('.', start);
            if (t < 0) {
                // Additional hhmm[ss] format for compatibility
                hEnd = mStart = start + 2;
                mEnd = mStart + 2;
                int len = end - start;
                if (len == 6) {
                    sStart = mEnd;
                    sEnd = -1;
                } else if (len == 4) {
                    sStart = sEnd = -1;
                } else {
                    throw new IllegalArgumentException(s);
                }
            } else if (t >= start + 6) {
                // Additional hhmmss.nanos format for compatibility
                if (t - start != 6) {
                    throw new IllegalArgumentException(s);
                }
                hEnd = mStart = start + 2;
                mEnd = sStart = mStart + 2;
                sEnd = t;
            } else {
                // Additional hour.minute.second[.nanos] IBM DB2 time format
                hEnd = t;
                mStart = hEnd + 1;
                mEnd = s.indexOf('.', mStart);
                if (mEnd <= mStart) {
                    throw new IllegalArgumentException(s);
                }
                sStart = mEnd + 1;
                sEnd = s.indexOf('.', sStart);
            }
        }
        hour = StringUtils.parseUInt31(s, start, hEnd);
        if (hour >= 24) {
            throw new IllegalArgumentException(s);
        }
        minute = StringUtils.parseUInt31(s, mStart, mEnd);
        if (sStart > 0) {
            if (sEnd < 0) {
                second = StringUtils.parseUInt31(s, sStart, end);
                nanos = 0;
            } else {
                second = StringUtils.parseUInt31(s, sStart, sEnd);
                nanos = parseNanos(s, sEnd + 1, end);
            }
        } else {
            second = nanos = 0;
        }
        if (minute >= 60 || second >= 60) {
            throw new IllegalArgumentException(s);
        }
        return ((((hour * 60L) + minute) * 60) + second) * NANOS_PER_SECOND + nanos;
    }

    /**
     * Parse nanoseconds.
     *
     * @param s String to parse.
     * @param start Begin position at the string to read.
     * @param end End position at the string to read.
     * @return Parsed nanoseconds.
     */
    static int parseNanos(String s, int start, int end) {
        if (start >= end) {
            throw new IllegalArgumentException(s);
        }
        int nanos = 0, mul = 100_000_000;
        do {
            char c = s.charAt(start);
            if (c < '0' || c > '9') {
                throw new IllegalArgumentException(s);
            }
            nanos += mul * (c - '0');
            // mul can become 0, but continue loop anyway to ensure that all
            // remaining digits are valid
            mul /= 10;
        } while (++start < end);
        return nanos;
    }

    /**
     * Parses timestamp value from the specified string.
     *
     * @param s
     *            string to parse
     * @param provider
     *            the cast information provider, may be {@code null} for
     *            Standard-compliant literals
     * @param withTimeZone
     *            if {@code true} return {@link ValueTimestampTimeZone} instead of
     *            {@link ValueTimestamp}
     * @return parsed timestamp
     */
    public static Value parseTimestamp(String s, CastDataProvider provider, boolean withTimeZone) {
        int dateEnd = s.indexOf(' ');
        if (dateEnd < 0) {
            // ISO 8601 compatibility
            dateEnd = s.indexOf('T');
            if (dateEnd < 0 && provider != null && provider.getMode().allowDB2TimestampFormat) {
                // DB2 also allows dash between date and time
                dateEnd = s.indexOf('-', s.indexOf('-', s.indexOf('-') + 1) + 1);
            }
        }
        int timeStart;
        if (dateEnd < 0) {
            dateEnd = s.length();
            timeStart = -1;
        } else {
            timeStart = dateEnd + 1;
        }
        long dateValue = parseDateValue(s, 0, dateEnd);
        long nanos;
        TimeZoneProvider tz = null;
        if (timeStart < 0) {
            nanos = 0;
        } else {
            dateEnd++;
            int timeEnd;
            if (s.endsWith("Z")) {
                tz = TimeZoneProvider.UTC;
                timeEnd = s.length() - 1;
            } else {
                int timeZoneStart = s.indexOf('+', dateEnd);
                if (timeZoneStart < 0) {
                    timeZoneStart = s.indexOf('-', dateEnd);
                }
                if (timeZoneStart >= 0) {
                    // Allow [timeZoneName] part after time zone offset
                    int offsetEnd = s.indexOf('[', timeZoneStart + 1);
                    if (offsetEnd < 0) {
                        offsetEnd = s.length();
                    }
                    tz = TimeZoneProvider.ofId(s.substring(timeZoneStart, offsetEnd));
                    if (s.charAt(timeZoneStart - 1) == ' ') {
                        timeZoneStart--;
                    }
                    timeEnd = timeZoneStart;
                } else {
                    timeZoneStart = s.indexOf(' ', dateEnd);
                    if (timeZoneStart > 0) {
                        tz = TimeZoneProvider.ofId(s.substring(timeZoneStart + 1));
                        timeEnd = timeZoneStart;
                    } else {
                        timeEnd = s.length();
                    }
                }
            }
            nanos = parseTimeNanos(s, dateEnd, timeEnd);
        }
        if (withTimeZone) {
            int tzSeconds;
            if (tz == null) {
                tz = provider != null ? provider.currentTimeZone() : DateTimeUtils.getTimeZone();
            }
            if (tz != TimeZoneProvider.UTC) {
                tzSeconds = tz.getTimeZoneOffsetUTC(tz.getEpochSecondsFromLocal(dateValue, nanos));
            } else {
                tzSeconds = 0;
            }
            return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, nanos, tzSeconds);
        } else if (tz != null) {
            long seconds = tz.getEpochSecondsFromLocal(dateValue, nanos);
            seconds += (provider != null ? provider.currentTimeZone() : DateTimeUtils.getTimeZone())
                    .getTimeZoneOffsetUTC(seconds);
            dateValue = dateValueFromLocalSeconds(seconds);
            nanos = nanos % 1_000_000_000 + nanosFromLocalSeconds(seconds);
        }
        return ValueTimestamp.fromDateValueAndNanos(dateValue, nanos);
    }

    /**
     * Parses time value from the specified string.
     *
     * @param s
     *            string to parse
     * @param provider
     *            the cast information provider, or {@code null}
     * @param withTimeZone
     *            if {@code true} return {@link ValueTimeTimeZone} instead of
     *            {@link ValueTime}
     * @return parsed time
     */
    public static Value parseTime(String s, CastDataProvider provider, boolean withTimeZone) {
        int timeEnd;
        TimeZoneProvider tz = null;
        if (s.endsWith("Z")) {
            tz = TimeZoneProvider.UTC;
            timeEnd = s.length() - 1;
        } else {
            int timeZoneStart = s.indexOf('+', 1);
            if (timeZoneStart < 0) {
                timeZoneStart = s.indexOf('-', 1);
            }
            if (timeZoneStart >= 0) {
                tz = TimeZoneProvider.ofId(s.substring(timeZoneStart));
                if (s.charAt(timeZoneStart - 1) == ' ') {
                    timeZoneStart--;
                }
                timeEnd = timeZoneStart;
            } else {
                timeZoneStart = s.indexOf(' ', 1);
                if (timeZoneStart > 0) {
                    tz = TimeZoneProvider.ofId(s.substring(timeZoneStart + 1));
                    timeEnd = timeZoneStart;
                } else {
                    timeEnd = s.length();
                }
            }
            if (tz != null && !tz.hasFixedOffset()) {
                throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2, "TIME WITH TIME ZONE", s);
            }
        }
        long nanos = parseTimeNanos(s, 0, timeEnd);
        if (withTimeZone) {
            return ValueTimeTimeZone.fromNanos(nanos,
                    tz != null ? tz.getTimeZoneOffsetUTC(0L)
                            : (provider != null ? provider.currentTimestamp() : currentTimestamp(getTimeZone()))
                                    .getTimeZoneOffsetSeconds());
        }
        if (tz != null) {
            nanos = normalizeNanosOfDay(
                    nanos + ((provider != null ? provider.currentTimestamp() : currentTimestamp(getTimeZone()))
                            .getTimeZoneOffsetSeconds() - tz.getTimeZoneOffsetUTC(0L)) * NANOS_PER_SECOND);
        }
        return ValueTime.fromNanos(nanos);
    }

    /**
     * Calculates the seconds since epoch for the specified date value,
     * nanoseconds since midnight, and time zone offset.
     * @param dateValue
     *            date value
     * @param timeNanos
     *            nanoseconds since midnight
     * @param offsetSeconds
     *            time zone offset in seconds
     * @return seconds since epoch in UTC
     */
    public static long getEpochSeconds(long dateValue, long timeNanos, int offsetSeconds) {
        return absoluteDayFromDateValue(dateValue) * SECONDS_PER_DAY + timeNanos / NANOS_PER_SECOND - offsetSeconds;
    }

    /**
     * Extracts date value and nanos of day from the specified value.
     *
     * @param value
     *            value to extract fields from
     * @param provider
     *            the cast information provider
     * @return array with date value and nanos of day
     */
    public static long[] dateAndTimeFromValue(Value value, CastDataProvider provider) {
        long dateValue = EPOCH_DATE_VALUE;
        long timeNanos = 0;
        if (value instanceof ValueTimestamp) {
            ValueTimestamp v = (ValueTimestamp) value;
            dateValue = v.getDateValue();
            timeNanos = v.getTimeNanos();
        } else if (value instanceof ValueDate) {
            dateValue = ((ValueDate) value).getDateValue();
        } else if (value instanceof ValueTime) {
            timeNanos = ((ValueTime) value).getNanos();
        } else if (value instanceof ValueTimestampTimeZone) {
            ValueTimestampTimeZone v = (ValueTimestampTimeZone) value;
            dateValue = v.getDateValue();
            timeNanos = v.getTimeNanos();
        } else if (value instanceof ValueTimeTimeZone) {
            timeNanos = ((ValueTimeTimeZone) value).getNanos();
        } else {
            ValueTimestamp v = (ValueTimestamp) value.convertTo(TypeInfo.TYPE_TIMESTAMP, provider);
            dateValue = v.getDateValue();
            timeNanos = v.getTimeNanos();
        }
        return new long[] {dateValue, timeNanos};
    }

    /**
     * Creates a new date-time value with the same type as original value. If
     * original value is a ValueTimestampTimeZone or ValueTimeTimeZone, returned
     * value will have the same time zone offset as original value.
     *
     * @param original
     *            original value
     * @param dateValue
     *            date value for the returned value
     * @param timeNanos
     *            nanos of day for the returned value
     * @return new value with specified date value and nanos of day
     */
    public static Value dateTimeToValue(Value original, long dateValue, long timeNanos) {
        switch (original.getValueType()) {
        case Value.DATE:
            return ValueDate.fromDateValue(dateValue);
        case Value.TIME:
            return ValueTime.fromNanos(timeNanos);
        case Value.TIME_TZ:
            return ValueTimeTimeZone.fromNanos(timeNanos, ((ValueTimeTimeZone) original).getTimeZoneOffsetSeconds());
        case Value.TIMESTAMP:
        default:
            return ValueTimestamp.fromDateValueAndNanos(dateValue, timeNanos);
        case Value.TIMESTAMP_TZ:
            return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, timeNanos,
                    ((ValueTimestampTimeZone) original).getTimeZoneOffsetSeconds());
        }
    }

    /**
     * Returns day of week.
     *
     * @param dateValue
     *            the date value
     * @param firstDayOfWeek
     *            first day of week, Monday as 1, Sunday as 7 or 0
     * @return day of week
     * @see #getIsoDayOfWeek(long)
     */
    public static int getDayOfWeek(long dateValue, int firstDayOfWeek) {
        return getDayOfWeekFromAbsolute(absoluteDayFromDateValue(dateValue), firstDayOfWeek);
    }

    /**
     * Get the day of the week from the absolute day value.
     *
     * @param absoluteValue the absolute day
     * @param firstDayOfWeek the first day of the week
     * @return the day of week
     */
    public static int getDayOfWeekFromAbsolute(long absoluteValue, int firstDayOfWeek) {
        return absoluteValue >= 0 ? (int) ((absoluteValue - firstDayOfWeek + 11) % 7) + 1
                : (int) ((absoluteValue - firstDayOfWeek - 2) % 7) + 7;
    }

    /**
     * Returns number of day in year.
     *
     * @param dateValue
     *            the date value
     * @return number of day in year
     */
    public static int getDayOfYear(long dateValue) {
        int m = monthFromDateValue(dateValue);
        int a = (367 * m - 362) / 12 + dayFromDateValue(dateValue);
        if (m > 2) {
            a--;
            long y = yearFromDateValue(dateValue);
            if ((y & 3) != 0 || (y % 100 == 0 && y % 400 != 0)) {
                a--;
            }
        }
        return a;
    }

    /**
     * Returns ISO day of week.
     *
     * @param dateValue
     *            the date value
     * @return ISO day of week, Monday as 1 to Sunday as 7
     * @see #getSundayDayOfWeek(long)
     */
    public static int getIsoDayOfWeek(long dateValue) {
        return getDayOfWeek(dateValue, 1);
    }

    /**
     * Returns ISO number of week in year.
     *
     * @param dateValue
     *            the date value
     * @return number of week in year
     * @see #getIsoWeekYear(long)
     * @see #getWeekOfYear(long, int, int)
     */
    public static int getIsoWeekOfYear(long dateValue) {
        return getWeekOfYear(dateValue, 1, 4);
    }

    /**
     * Returns ISO week year.
     *
     * @param dateValue
     *            the date value
     * @return ISO week year
     * @see #getIsoWeekOfYear(long)
     * @see #getWeekYear(long, int, int)
     */
    public static int getIsoWeekYear(long dateValue) {
        return getWeekYear(dateValue, 1, 4);
    }

    /**
     * Returns day of week with Sunday as 1.
     *
     * @param dateValue
     *            the date value
     * @return day of week, Sunday as 1 to Monday as 7
     * @see #getIsoDayOfWeek(long)
     */
    public static int getSundayDayOfWeek(long dateValue) {
        return getDayOfWeek(dateValue, 0);
    }

    /**
     * Returns number of week in year.
     *
     * @param dateValue
     *            the date value
     * @param firstDayOfWeek
     *            first day of week, Monday as 1, Sunday as 7 or 0
     * @param minimalDaysInFirstWeek
     *            minimal days in first week of year
     * @return number of week in year
     * @see #getIsoWeekOfYear(long)
     */
    public static int getWeekOfYear(long dateValue, int firstDayOfWeek, int minimalDaysInFirstWeek) {
        long abs = absoluteDayFromDateValue(dateValue);
        int year = yearFromDateValue(dateValue);
        long base = getWeekYearAbsoluteStart(year, firstDayOfWeek, minimalDaysInFirstWeek);
        if (abs - base < 0) {
            base = getWeekYearAbsoluteStart(year - 1, firstDayOfWeek, minimalDaysInFirstWeek);
        } else if (monthFromDateValue(dateValue) == 12 && 24 + minimalDaysInFirstWeek < dayFromDateValue(dateValue)) {
            if (abs >= getWeekYearAbsoluteStart(year + 1, firstDayOfWeek, minimalDaysInFirstWeek)) {
                return 1;
            }
        }
        return (int) ((abs - base) / 7) + 1;
    }

    /**
     * Get absolute day of the first day in the week year.
     *
     * @param weekYear
     *            the week year
     * @param firstDayOfWeek
     *            first day of week, Monday as 1, Sunday as 7 or 0
     * @param minimalDaysInFirstWeek
     *            minimal days in first week of year
     * @return absolute day of the first day in the week year
     */
    public static long getWeekYearAbsoluteStart(int weekYear, int firstDayOfWeek, int minimalDaysInFirstWeek) {
        long first = absoluteDayFromYear(weekYear);
        int daysInFirstWeek = 8 - getDayOfWeekFromAbsolute(first, firstDayOfWeek);
        long base = first + daysInFirstWeek;
        if (daysInFirstWeek >= minimalDaysInFirstWeek) {
            base -= 7;
        }
        return base;
    }

    /**
     * Returns week year.
     *
     * @param dateValue
     *            the date value
     * @param firstDayOfWeek
     *            first day of week, Monday as 1, Sunday as 7 or 0
     * @param minimalDaysInFirstWeek
     *            minimal days in first week of year
     * @return week year
     * @see #getIsoWeekYear(long)
     */
    public static int getWeekYear(long dateValue, int firstDayOfWeek, int minimalDaysInFirstWeek) {
        long abs = absoluteDayFromDateValue(dateValue);
        int year = yearFromDateValue(dateValue);
        long base = getWeekYearAbsoluteStart(year, firstDayOfWeek, minimalDaysInFirstWeek);
        if (abs < base) {
            return year - 1;
        } else if (monthFromDateValue(dateValue) == 12 && 24 + minimalDaysInFirstWeek < dayFromDateValue(dateValue)) {
            if (abs >= getWeekYearAbsoluteStart(year + 1, firstDayOfWeek, minimalDaysInFirstWeek)) {
                return year + 1;
            }
        }
        return year;
    }

    /**
     * Returns number of days in month.
     *
     * @param year the year
     * @param month the month
     * @return number of days in the specified month
     */
    public static int getDaysInMonth(int year, int month) {
        if (month != 2) {
            return NORMAL_DAYS_PER_MONTH[month];
        }
        return isLeapYear(year) ? 29 : 28;
    }

    static boolean isLeapYear(int year) {
        return (year & 3) == 0 && (year % 100 != 0 || year % 400 == 0);
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
        return month >= 1 && month <= 12 && day >= 1 && day <= getDaysInMonth(year, month);
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
     * Get the date value from a given denormalized date with possible out of range
     * values of month and/or day. Used after addition or subtraction month or years
     * to (from) it to get a valid date.
     *
     * @param year
     *            the year
     * @param month
     *            the month, if out of range month and year will be normalized
     * @param day
     *            the day of the month, if out of range it will be saturated
     * @return the date value
     */
    public static long dateValueFromDenormalizedDate(long year, long month, int day) {
        long mm1 = month - 1;
        long yd = mm1 / 12;
        if (mm1 < 0 && yd * 12 != mm1) {
            yd--;
        }
        int y = (int) (year + yd);
        int m = (int) (month - yd * 12);
        if (day < 1) {
            day = 1;
        } else {
            int max = getDaysInMonth(y, m);
            if (day > max) {
                day = max;
            }
        }
        return dateValue(y, m, day);
    }

    /**
     * Convert a local seconds to an encoded date.
     *
     * @param localSeconds the seconds since 1970-01-01
     * @return the date value
     */
    public static long dateValueFromLocalSeconds(long localSeconds) {
        long absoluteDay = localSeconds / SECONDS_PER_DAY;
        // Round toward negative infinity
        if (localSeconds < 0 && (absoluteDay * SECONDS_PER_DAY != localSeconds)) {
            absoluteDay--;
        }
        return dateValueFromAbsoluteDay(absoluteDay);
    }

    /**
     * Convert a time in seconds in local time to the nanoseconds since midnight.
     *
     * @param localSeconds the seconds since 1970-01-01
     * @return the nanoseconds
     */
    public static long nanosFromLocalSeconds(long localSeconds) {
        localSeconds %= SECONDS_PER_DAY;
        if (localSeconds < 0) {
            localSeconds += SECONDS_PER_DAY;
        }
        return localSeconds * NANOS_PER_SECOND;
    }

    /**
     * Calculate the normalized nanos of day.
     *
     * @param nanos the nanoseconds (may be negative or larger than one day)
     * @return the nanos of day within a day
     */
    public static long normalizeNanosOfDay(long nanos) {
        nanos %= NANOS_PER_DAY;
        if (nanos < 0) {
            nanos += NANOS_PER_DAY;
        }
        return nanos;
    }

    /**
     * Calculate the absolute day for a January, 1 of the specified year.
     *
     * @param year
     *            the year
     * @return the absolute day
     */
    public static long absoluteDayFromYear(long year) {
        long a = 365 * year - 719_528;
        if (year >= 0) {
            a += (year + 3) / 4 - (year + 99) / 100 + (year + 399) / 400;
        } else {
            a -= year / -4 - year / -100 + year / -400;
        }
        return a;
    }

    /**
     * Calculate the absolute day from an encoded date value.
     *
     * @param dateValue the date value
     * @return the absolute day
     */
    public static long absoluteDayFromDateValue(long dateValue) {
        return absoluteDay(yearFromDateValue(dateValue), monthFromDateValue(dateValue), dayFromDateValue(dateValue));
    }

    /**
     * Calculate the absolute day.
     *
     * @param y year
     * @param m month
     * @param d day
     * @return the absolute day
     */
    static long absoluteDay(long y, int m, int d) {
        long a = absoluteDayFromYear(y) + (367 * m - 362) / 12 + d - 1;
        if (m > 2) {
            a--;
            if ((y & 3) != 0 || (y % 100 == 0 && y % 400 != 0)) {
                a--;
            }
        }
        return a;
    }

    /**
     * Calculate the encoded date value from an absolute day.
     *
     * @param absoluteDay the absolute day
     * @return the date value
     */
    public static long dateValueFromAbsoluteDay(long absoluteDay) {
        long d = absoluteDay + 719_468;
        long a = 0;
        if (d < 0) {
            a = (d + 1) / 146_097 - 1;
            d -= a * 146_097;
            a *= 400;
        }
        long y = (400 * d + 591) / 146_097;
        int day = (int) (d - (365 * y + y / 4 - y / 100 + y / 400));
        if (day < 0) {
            y--;
            day = (int) (d - (365 * y + y / 4 - y / 100 + y / 400));
        }
        y += a;
        int m = (day * 5 + 2) / 153;
        day -= (m * 306 + 5) / 10 - 1;
        if (m >= 10) {
            y++;
            m -= 12;
        }
        return dateValue(y, m + 3, day);
    }

    /**
     * Return the next date value.
     *
     * @param dateValue
     *            the date value
     * @return the next date value
     */
    public static long incrementDateValue(long dateValue) {
        int day = dayFromDateValue(dateValue);
        if (day < 28) {
            return dateValue + 1;
        }
        int year = yearFromDateValue(dateValue);
        int month = monthFromDateValue(dateValue);
        if (day < getDaysInMonth(year, month)) {
            return dateValue + 1;
        }
        if (month < 12) {
            month++;
        } else {
            month = 1;
            year++;
        }
        return dateValue(year, month, 1);
    }

    /**
     * Return the previous date value.
     *
     * @param dateValue
     *            the date value
     * @return the previous date value
     */
    public static long decrementDateValue(long dateValue) {
        if (dayFromDateValue(dateValue) > 1) {
            return dateValue - 1;
        }
        int year = yearFromDateValue(dateValue);
        int month = monthFromDateValue(dateValue);
        if (month > 1) {
            month--;
        } else {
            month = 12;
            year--;
        }
        return dateValue(year, month, getDaysInMonth(year, month));
    }

    /**
     * Append a date to the string builder.
     *
     * @param builder the target string builder
     * @param dateValue the date value
     * @return the specified string builder
     */
    public static StringBuilder appendDate(StringBuilder builder, long dateValue) {
        int y = yearFromDateValue(dateValue);
        if (y < 1_000 && y > -1_000) {
            if (y < 0) {
                builder.append('-');
                y = -y;
            }
            StringUtils.appendZeroPadded(builder, 4, y);
        } else {
            builder.append(y);
        }
        StringUtils.appendTwoDigits(builder.append('-'), monthFromDateValue(dateValue)).append('-');
        return StringUtils.appendTwoDigits(builder, dayFromDateValue(dateValue));
    }

    /**
     * Append a time to the string builder.
     *
     * @param builder the target string builder
     * @param nanos the time in nanoseconds
     * @return the specified string builder
     */
    public static StringBuilder appendTime(StringBuilder builder, long nanos) {
        if (nanos < 0) {
            builder.append('-');
            nanos = -nanos;
        }
        /*
         * nanos now either in range from 0 to Long.MAX_VALUE or equals to
         * Long.MIN_VALUE. We need to divide nanos by 1,000,000,000 with
         * unsigned division to get correct result. The simplest way to do this
         * with such constraints is to divide -nanos by -1,000,000,000.
         */
        long s = -nanos / -1_000_000_000;
        nanos -= s * 1_000_000_000;
        int m = (int) (s / 60);
        s -= m * 60;
        int h = m / 60;
        m -= h * 60;
        StringUtils.appendTwoDigits(builder, h).append(':');
        StringUtils.appendTwoDigits(builder, m).append(':');
        StringUtils.appendTwoDigits(builder, (int) s);
        return appendNanos(builder, (int) nanos);
    }

    /**
     * Append nanoseconds of time, if any.
     *
     * @param builder string builder to append to
     * @param nanos nanoseconds of second
     * @return the specified string builder
     */
    static StringBuilder appendNanos(StringBuilder builder, int nanos) {
        if (nanos > 0) {
            builder.append('.');
            for (int i = 1; nanos < FRACTIONAL_SECONDS_TABLE[i]; i++) {
                builder.append('0');
            }
            if (nanos % 1_000 == 0) {
                nanos /= 1_000;
                if (nanos % 1_000 == 0) {
                    nanos /= 1_000;
                }
            }
            if (nanos % 10 == 0) {
                nanos /= 10;
                if (nanos % 10 == 0) {
                    nanos /= 10;
                }
            }
            builder.append(nanos);
        }
        return builder;
    }

    /**
     * Append a time zone to the string builder.
     *
     * @param builder the target string builder
     * @param tz the time zone offset in seconds
     * @return the specified string builder
     */
    public static StringBuilder appendTimeZone(StringBuilder builder, int tz) {
        if (tz < 0) {
            builder.append('-');
            tz = -tz;
        } else {
            builder.append('+');
        }
        int rem = tz / 3_600;
        StringUtils.appendTwoDigits(builder, rem);
        tz -= rem * 3_600;
        if (tz != 0) {
            rem = tz / 60;
            StringUtils.appendTwoDigits(builder.append(':'), rem);
            tz -= rem * 60;
            if (tz != 0) {
                StringUtils.appendTwoDigits(builder.append(':'), tz);
            }
        }
        return builder;
    }

    /**
     * Generates time zone name for the specified offset in seconds.
     *
     * @param offsetSeconds
     *            time zone offset in seconds
     * @return time zone name
     */
    public static String timeZoneNameFromOffsetSeconds(int offsetSeconds) {
        if (offsetSeconds == 0) {
            return "UTC";
        }
        StringBuilder b = new StringBuilder(12);
        b.append("GMT");
        if (offsetSeconds < 0) {
            b.append('-');
            offsetSeconds = -offsetSeconds;
        } else {
            b.append('+');
        }
        StringUtils.appendTwoDigits(b, offsetSeconds / 3_600).append(':');
        offsetSeconds %= 3_600;
        StringUtils.appendTwoDigits(b, offsetSeconds / 60);
        offsetSeconds %= 60;
        if (offsetSeconds != 0) {
            b.append(':');
            StringUtils.appendTwoDigits(b, offsetSeconds);
        }
        return b.toString();
    }


    /**
     * Converts scale of nanoseconds.
     *
     * @param nanosOfDay nanoseconds of day
     * @param scale fractional seconds precision
     * @param range the allowed range of values (0..range-1)
     * @return scaled value
     */
    public static long convertScale(long nanosOfDay, int scale, long range) {
        if (scale >= 9) {
            return nanosOfDay;
        }
        int m = FRACTIONAL_SECONDS_TABLE[scale];
        long mod = nanosOfDay % m;
        if (mod >= m >>> 1) {
            nanosOfDay += m;
        }
        long r = nanosOfDay - mod;
        if (r >= range) {
            r = range - m;
        }
        return r;
    }

    /**
     * Moves timestamp with time zone to a new time zone.
     *
     * @param dateValue the date value
     * @param timeNanos the nanoseconds since midnight
     * @param oldOffset old offset
     * @param newOffset new offset
     * @return timestamp with time zone with new offset
     */
    public static ValueTimestampTimeZone timestampTimeZoneAtOffset(long dateValue, long timeNanos, int oldOffset,
            int newOffset) {
        timeNanos += (newOffset - oldOffset) * DateTimeUtils.NANOS_PER_SECOND;
        // Value can be 18+18 hours before or after the limit
        if (timeNanos < 0) {
            timeNanos += DateTimeUtils.NANOS_PER_DAY;
            dateValue = DateTimeUtils.decrementDateValue(dateValue);
            if (timeNanos < 0) {
                timeNanos += DateTimeUtils.NANOS_PER_DAY;
                dateValue = DateTimeUtils.decrementDateValue(dateValue);
            }
        } else if (timeNanos >= DateTimeUtils.NANOS_PER_DAY) {
            timeNanos -= DateTimeUtils.NANOS_PER_DAY;
            dateValue = DateTimeUtils.incrementDateValue(dateValue);
            if (timeNanos >= DateTimeUtils.NANOS_PER_DAY) {
                timeNanos -= DateTimeUtils.NANOS_PER_DAY;
                dateValue = DateTimeUtils.incrementDateValue(dateValue);
            }
        }
        return ValueTimestampTimeZone.fromDateValueAndNanos(dateValue, timeNanos, newOffset);
    }

}
