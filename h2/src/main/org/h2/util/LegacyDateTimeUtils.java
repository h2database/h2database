/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import static org.h2.util.DateTimeUtils.MILLIS_PER_DAY;
import static org.h2.util.DateTimeUtils.NANOS_PER_SECOND;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.h2.engine.CastDataProvider;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueNull;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;

/**
 * Date and time utilities for {@link Date}, {@link Time}, and {@link Timestamp}
 * classes.
 */
public final class LegacyDateTimeUtils {

    /**
     * Gregorian change date for a {@link java.util.GregorianCalendar} that
     * represents a proleptic Gregorian calendar.
     */
    public static final Date PROLEPTIC_GREGORIAN_CHANGE = new Date(Long.MIN_VALUE);

    /**
     * UTC time zone.
     */
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private LegacyDateTimeUtils() {
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param provider
     *            the cast information provider
     * @param timeZone
     *            time zone, or {@code null} for default
     * @param date
     *            the date
     * @return the value
     */
    public static ValueDate fromDate(CastDataProvider provider, TimeZone timeZone, Date date) {
        long ms = date.getTime();
        return ValueDate.fromDateValue(dateValueFromLocalMillis(
                ms + (timeZone == null ? getTimeZoneOffsetMillis(provider, ms) : timeZone.getOffset(ms))));
    }

    /**
     * Get or create a time value for the given time.
     *
     * @param provider
     *            the cast information provider
     * @param timeZone
     *            time zone, or {@code null} for default
     * @param time
     *            the time
     * @return the value
     */
    public static ValueTime fromTime(CastDataProvider provider, TimeZone timeZone, Time time) {
        long ms = time.getTime();
        return ValueTime.fromNanos(nanosFromLocalMillis(
                ms + (timeZone == null ? getTimeZoneOffsetMillis(provider, ms) : timeZone.getOffset(ms))));
    }

    /**
     * Get or create a timestamp value for the given timestamp.
     *
     * @param provider
     *            the cast information provider
     * @param timeZone
     *            time zone, or {@code null} for default
     * @param timestamp
     *            the timestamp
     * @return the value
     */
    public static ValueTimestamp fromTimestamp(CastDataProvider provider, TimeZone timeZone, Timestamp timestamp) {
        long ms = timestamp.getTime();
        return timestampFromLocalMillis(
                ms + (timeZone == null ? getTimeZoneOffsetMillis(provider, ms) : timeZone.getOffset(ms)),
                timestamp.getNanos() % 1_000_000);
    }

    /**
     * Get or create a timestamp value for the given date/time in millis.
     *
     * @param provider
     *            the cast information provider
     * @param ms
     *            the milliseconds
     * @param nanos
     *            the nanoseconds
     * @return the value
     */
    public static ValueTimestamp fromTimestamp(CastDataProvider provider, long ms, int nanos) {
        return timestampFromLocalMillis(ms + getTimeZoneOffsetMillis(provider, ms), nanos);
    }

    private static ValueTimestamp timestampFromLocalMillis(long ms, int nanos) {
        long dateValue = dateValueFromLocalMillis(ms);
        long timeNanos = nanos + nanosFromLocalMillis(ms);
        return ValueTimestamp.fromDateValueAndNanos(dateValue, timeNanos);
    }

    /**
     * Convert a local datetime in millis to an encoded date.
     *
     * @param ms
     *            the milliseconds
     * @return the date value
     */
    public static long dateValueFromLocalMillis(long ms) {
        long absoluteDay = ms / MILLIS_PER_DAY;
        // Round toward negative infinity
        if (ms < 0 && (absoluteDay * MILLIS_PER_DAY != ms)) {
            absoluteDay--;
        }
        return DateTimeUtils.dateValueFromAbsoluteDay(absoluteDay);
    }

    /**
     * Convert a time in milliseconds in local time to the nanoseconds since
     * midnight.
     *
     * @param ms
     *            the milliseconds
     * @return the nanoseconds
     */
    public static long nanosFromLocalMillis(long ms) {
        ms %= MILLIS_PER_DAY;
        if (ms < 0) {
            ms += MILLIS_PER_DAY;
        }
        return ms * 1_000_000;
    }

    /**
     * Get the date value converted to the specified time zone.
     *
     * @param provider the cast information provider
     * @param timeZone the target time zone
     * @param value the value to convert
     * @return the date
     */
    public static Date toDate(CastDataProvider provider, TimeZone timeZone, Value value) {
        return value != ValueNull.INSTANCE
                ? new Date(getMillis(provider, timeZone, value.convertToDate(provider).getDateValue(), 0)) : null;
    }

    /**
     * Get the time value converted to the specified time zone.
     *
     * @param provider the cast information provider
     * @param timeZone the target time zone
     * @param value the value to convert
     * @return the time
     */
    public static Time toTime(CastDataProvider provider, TimeZone timeZone, Value value) {
        switch (value.getValueType()) {
        case Value.NULL:
            return null;
        default:
            value = value.convertTo(TypeInfo.TYPE_TIME, provider);
            //$FALL-THROUGH$
        case Value.TIME:
            return new Time(
                    getMillis(provider, timeZone, DateTimeUtils.EPOCH_DATE_VALUE, ((ValueTime) value).getNanos()));
        }
    }

    /**
     * Get the timestamp value converted to the specified time zone.
     *
     * @param provider the cast information provider
     * @param timeZone the target time zone
     * @param value the value to convert
     * @return the timestamp
     */
    public static Timestamp toTimestamp(CastDataProvider provider, TimeZone timeZone, Value value) {
        switch (value.getValueType()) {
        case Value.NULL:
            return null;
        default:
            value = value.convertTo(TypeInfo.TYPE_TIMESTAMP, provider);
            //$FALL-THROUGH$
        case Value.TIMESTAMP: {
            ValueTimestamp v = (ValueTimestamp) value;
            long timeNanos = v.getTimeNanos();
            Timestamp ts = new Timestamp(getMillis(provider, timeZone, v.getDateValue(), timeNanos));
            ts.setNanos((int) (timeNanos % NANOS_PER_SECOND));
            return ts;
        }
        case Value.TIMESTAMP_TZ: {
            ValueTimestampTimeZone v = (ValueTimestampTimeZone) value;
            long timeNanos = v.getTimeNanos();
            Timestamp ts = new Timestamp(DateTimeUtils.absoluteDayFromDateValue(v.getDateValue()) * MILLIS_PER_DAY
                    + timeNanos / 1_000_000 - v.getTimeZoneOffsetSeconds() * 1_000);
            ts.setNanos((int) (timeNanos % NANOS_PER_SECOND));
            return ts;
        }
        }
    }

    /**
     * Calculate the milliseconds since 1970-01-01 (UTC) for the given date and
     * time (in the specified timezone).
     *
     * @param provider the cast information provider
     * @param tz the timezone of the parameters, or null for the default
     *            timezone
     * @param dateValue date value
     * @param timeNanos nanoseconds since midnight
     * @return the number of milliseconds (UTC)
     */
    public static long getMillis(CastDataProvider provider, TimeZone tz, long dateValue, long timeNanos) {
        return (tz == null ? provider != null ? provider.currentTimeZone() : DateTimeUtils.getTimeZone()
                : TimeZoneProvider.ofId(tz.getID())).getEpochSecondsFromLocal(dateValue, timeNanos) * 1_000
                + timeNanos / 1_000_000 % 1_000;
    }

    /**
     * Returns local time zone offset for a specified timestamp.
     *
     * @param provider the cast information provider
     * @param ms milliseconds since Epoch in UTC
     * @return local time zone offset
     */
    public static int getTimeZoneOffsetMillis(CastDataProvider provider, long ms) {
        long seconds = ms / 1_000;
        // Round toward negative infinity
        if (ms < 0 && (seconds * 1_000 != ms)) {
            seconds--;
        }
        return (provider != null ? provider.currentTimeZone() : DateTimeUtils.getTimeZone())
                .getTimeZoneOffsetUTC(seconds) * 1_000;
    }

    /**
     * Convert a legacy Java object to a value.
     *
     * @param session
     *            the session
     * @param x
     *            the value
     * @return the value, or {@code null} if not supported
     */
    public static Value legacyObjectToValue(CastDataProvider session, Object x) {
        if (x instanceof Date) {
            return fromDate(session, null, (Date) x);
        } else if (x instanceof Time) {
            return fromTime(session, null, (Time) x);
        } else if (x instanceof Timestamp) {
            return fromTimestamp(session, null, (Timestamp) x);
        } else if (x instanceof java.util.Date) {
            return fromTimestamp(session, ((java.util.Date) x).getTime(), 0);
        } else if (x instanceof Calendar) {
            Calendar gc = (Calendar) x;
            long ms = gc.getTimeInMillis();
            return timestampFromLocalMillis(ms + gc.getTimeZone().getOffset(ms), 0);
        } else {
            return null;
        }
    }

    /**
     * Converts the specified value to an object of the specified legacy type.
     *
     * @param <T> the type
     * @param type the class
     * @param value the value
     * @param provider the cast information provider
     * @return an instance of the specified class, or {@code null} if not supported
     */
    @SuppressWarnings("unchecked")
    public static <T> T valueToLegacyType(Class<T> type, Value value, CastDataProvider provider) {
        if (type == Date.class) {
            return (T) toDate(provider, null, value);
        } else if (type == Time.class) {
            return (T) toTime(provider, null, value);
        } else if (type == Timestamp.class) {
            return (T) toTimestamp(provider, null, value);
        } else if (type == java.util.Date.class) {
            return (T) new java.util.Date(toTimestamp(provider, null, value).getTime());
        } else if (type == Calendar.class) {
            GregorianCalendar calendar = new GregorianCalendar();
            calendar.setGregorianChange(PROLEPTIC_GREGORIAN_CHANGE);
            calendar.setTime(toTimestamp(provider, calendar.getTimeZone(), value));
            return (T) calendar;
        } else {
            return null;
        }
    }

    /**
     * Get the type information for the given legacy Java class.
     *
     * @param clazz
     *            the Java class
     * @return the value type, or {@code null} if not supported
     */
    public static TypeInfo legacyClassToType(Class<?> clazz) {
        if (Date.class.isAssignableFrom(clazz)) {
            return TypeInfo.TYPE_DATE;
        } else if (Time.class.isAssignableFrom(clazz)) {
            return TypeInfo.TYPE_TIME;
        } else if (java.util.Date.class.isAssignableFrom(clazz) || Calendar.class.isAssignableFrom(clazz)) {
            return TypeInfo.TYPE_TIMESTAMP;
        } else{
            return null;
        }
    }

}
