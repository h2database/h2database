/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;

/**
 * Implementation of the TIMESTAMP WITH TIME ZONE data type.
 */
public final class ValueTimestampTimeZone extends Value {

    /**
     * The default precision and display size of the textual representation of a timestamp.
     * Example: 2001-01-01 23:59:59.123456+10:00
     */
    public static final int DEFAULT_PRECISION = 32;

    /**
     * The maximum precision and display size of the textual representation of a timestamp.
     * Example: 2001-01-01 23:59:59.123456789+10:00
     */
    public static final int MAXIMUM_PRECISION = 35;

    /**
     * A bit field with bits for the year, month, and day (see DateTimeUtils for
     * encoding)
     */
    private final long dateValue;
    /**
     * The nanoseconds since midnight.
     */
    private final long timeNanos;
    /**
     * Time zone offset from UTC in seconds, range of -18 hours to +18 hours. This
     * range is compatible with OffsetDateTime from JSR-310.
     */
    private final int timeZoneOffsetSeconds;

    private ValueTimestampTimeZone(long dateValue, long timeNanos, int timeZoneOffsetSeconds) {
        if (dateValue < DateTimeUtils.MIN_DATE_VALUE || dateValue > DateTimeUtils.MAX_DATE_VALUE) {
            throw new IllegalArgumentException("dateValue out of range " + dateValue);
        }
        if (timeNanos < 0 || timeNanos >= DateTimeUtils.NANOS_PER_DAY) {
            throw new IllegalArgumentException(
                    "timeNanos out of range " + timeNanos);
        }
        /*
         * Some current and historic time zones have offsets larger than 12 hours.
         * JSR-310 determines 18 hours as maximum possible offset in both directions, so
         * we use this limit too for compatibility.
         */
        if (timeZoneOffsetSeconds < (-18 * 60 * 60)
                || timeZoneOffsetSeconds > (18 * 60 * 60)) {
            throw new IllegalArgumentException(
                    "timeZoneOffsetSeconds out of range " + timeZoneOffsetSeconds);
        }
        this.dateValue = dateValue;
        this.timeNanos = timeNanos;
        this.timeZoneOffsetSeconds = timeZoneOffsetSeconds;
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param dateValue the date value, a bit field with bits for the year,
     *            month, and day
     * @param timeNanos the nanoseconds since midnight
     * @param timeZoneOffsetSeconds the timezone offset in seconds
     * @return the value
     */
    public static ValueTimestampTimeZone fromDateValueAndNanos(long dateValue, long timeNanos,
            int timeZoneOffsetSeconds) {
        return (ValueTimestampTimeZone) Value.cache(new ValueTimestampTimeZone(
                dateValue, timeNanos, timeZoneOffsetSeconds));
    }

    /**
     * Parse a string to a ValueTimestamp. This method supports the format
     * +/-year-month-day hour:minute:seconds.fractional and an optional timezone
     * part.
     *
     * @param s the string to parse
     * @param provider
     *            the cast information provider, may be {@code null} for
     *            literals with time zone
     * @return the date
     */
    public static ValueTimestampTimeZone parse(String s, CastDataProvider provider) {
        try {
            return (ValueTimestampTimeZone) DateTimeUtils.parseTimestamp(s, provider, true);
        } catch (Exception e) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2, e, "TIMESTAMP WITH TIME ZONE", s);
        }
    }

    /**
     * A bit field with bits for the year, month, and day (see DateTimeUtils for
     * encoding).
     *
     * @return the data value
     */
    public long getDateValue() {
        return dateValue;
    }

    /**
     * The nanoseconds since midnight.
     *
     * @return the nanoseconds
     */
    public long getTimeNanos() {
        return timeNanos;
    }

    /**
     * The time zone offset in seconds.
     *
     * @return the offset
     */
    public int getTimeZoneOffsetSeconds() {
        return timeZoneOffsetSeconds;
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_TIMESTAMP_TZ;
    }

    @Override
    public int getValueType() {
        return TIMESTAMP_TZ;
    }

    @Override
    public int getMemory() {
        // Java 11 with -XX:-UseCompressedOops
        return 40;
    }

    @Override
    public String getString() {
        return toString(new StringBuilder(MAXIMUM_PRECISION), false).toString();
    }

    /**
     * Returns value as string in ISO format.
     *
     * @return value as string in ISO format
     */
    public String getISOString() {
        return toString(new StringBuilder(MAXIMUM_PRECISION), true).toString();
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return toString(builder.append("TIMESTAMP WITH TIME ZONE '"), false).append('\'');
    }

    private StringBuilder toString(StringBuilder builder, boolean iso) {
        DateTimeUtils.appendDate(builder, dateValue).append(iso ? 'T' : ' ');
        DateTimeUtils.appendTime(builder, timeNanos);
        return DateTimeUtils.appendTimeZone(builder, timeZoneOffsetSeconds);
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        ValueTimestampTimeZone t = (ValueTimestampTimeZone) o;
        // Maximum time zone offset is +/-18 hours so difference in days between local
        // and UTC cannot be more than one day
        long dateValueA = dateValue;
        long timeA = timeNanos - timeZoneOffsetSeconds * DateTimeUtils.NANOS_PER_SECOND;
        if (timeA < 0) {
            timeA += DateTimeUtils.NANOS_PER_DAY;
            dateValueA = DateTimeUtils.decrementDateValue(dateValueA);
        } else if (timeA >= DateTimeUtils.NANOS_PER_DAY) {
            timeA -= DateTimeUtils.NANOS_PER_DAY;
            dateValueA = DateTimeUtils.incrementDateValue(dateValueA);
        }
        long dateValueB = t.dateValue;
        long timeB = t.timeNanos - t.timeZoneOffsetSeconds * DateTimeUtils.NANOS_PER_SECOND;
        if (timeB < 0) {
            timeB += DateTimeUtils.NANOS_PER_DAY;
            dateValueB = DateTimeUtils.decrementDateValue(dateValueB);
        } else if (timeB >= DateTimeUtils.NANOS_PER_DAY) {
            timeB -= DateTimeUtils.NANOS_PER_DAY;
            dateValueB = DateTimeUtils.incrementDateValue(dateValueB);
        }
        int cmp = Long.compare(dateValueA, dateValueB);
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(timeA, timeB);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof ValueTimestampTimeZone)) {
            return false;
        }
        ValueTimestampTimeZone x = (ValueTimestampTimeZone) other;
        return dateValue == x.dateValue && timeNanos == x.timeNanos
                && timeZoneOffsetSeconds == x.timeZoneOffsetSeconds;
    }

    @Override
    public int hashCode() {
        return (int) (dateValue ^ (dateValue >>> 32) ^ timeNanos
                ^ (timeNanos >>> 32) ^ timeZoneOffsetSeconds);
    }

}
