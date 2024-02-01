/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;

/**
 * Implementation of the TIMESTAMP data type.
 */
public final class ValueTimestamp extends Value {

    /**
     * The default precision and display size of the textual representation of a timestamp.
     * Example: 2001-01-01 23:59:59.123456
     */
    public static final int DEFAULT_PRECISION = 26;

    /**
     * The maximum precision and display size of the textual representation of a timestamp.
     * Example: 2001-01-01 23:59:59.123456789
     */
    public static final int MAXIMUM_PRECISION = 29;

    /**
     * The default scale for timestamps.
     */
    public static final int DEFAULT_SCALE = 6;

    /**
     * The maximum scale for timestamps.
     */
    public static final int MAXIMUM_SCALE = 9;

    /**
     * A bit field with bits for the year, month, and day (see DateTimeUtils for
     * encoding)
     */
    private final long dateValue;
    /**
     * The nanoseconds since midnight.
     */
    private final long timeNanos;

    private ValueTimestamp(long dateValue, long timeNanos) {
        if (dateValue < DateTimeUtils.MIN_DATE_VALUE || dateValue > DateTimeUtils.MAX_DATE_VALUE) {
            throw new IllegalArgumentException("dateValue out of range " + dateValue);
        }
        if (timeNanos < 0 || timeNanos >= DateTimeUtils.NANOS_PER_DAY) {
            throw new IllegalArgumentException("timeNanos out of range " + timeNanos);
        }
        this.dateValue = dateValue;
        this.timeNanos = timeNanos;
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param dateValue the date value, a bit field with bits for the year,
     *            month, and day
     * @param timeNanos the nanoseconds since midnight
     * @return the value
     */
    public static ValueTimestamp fromDateValueAndNanos(long dateValue, long timeNanos) {
        return (ValueTimestamp) Value.cache(new ValueTimestamp(dateValue, timeNanos));
    }

    /**
     * Parse a string to a ValueTimestamp, using the given {@link CastDataProvider}.
     * This method supports the format +/-year-month-day[ -]hour[:.]minute[:.]seconds.fractional
     * and an optional timezone part.
     *
     * @param s the string to parse
     * @param provider
     *            the cast information provider, may be {@code null} for
     *            literals without time zone
     * @return the date
     */
    public static ValueTimestamp parse(String s, CastDataProvider provider) {
        try {
            return (ValueTimestamp) DateTimeUtils.parseTimestamp(s, provider, false);
        } catch (Exception e) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2, e, "TIMESTAMP", s);
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

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_TIMESTAMP;
    }

    @Override
    public int getValueType() {
        return TIMESTAMP;
    }

    @Override
    public int getMemory() {
        return 32;
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
        return toString(builder.append("TIMESTAMP '"), false).append('\'');
    }

    private StringBuilder toString(StringBuilder builder, boolean iso) {
        DateTimeUtils.appendDate(builder, dateValue).append(iso ? 'T' : ' ');
        return DateTimeUtils.appendTime(builder, timeNanos);
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        ValueTimestamp t = (ValueTimestamp) o;
        int c = Long.compare(dateValue, t.dateValue);
        if (c != 0) {
            return c;
        }
        return Long.compare(timeNanos, t.timeNanos);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof ValueTimestamp)) {
            return false;
        }
        ValueTimestamp x = (ValueTimestamp) other;
        return dateValue == x.dateValue && timeNanos == x.timeNanos;
    }

    @Override
    public int hashCode() {
        return (int) (dateValue ^ (dateValue >>> 32) ^ timeNanos ^ (timeNanos >>> 32));
    }

    @Override
    public Value add(Value v) {
        ValueTimestamp t = (ValueTimestamp) v;
        long absoluteDay = DateTimeUtils.absoluteDayFromDateValue(dateValue)
                + DateTimeUtils.absoluteDayFromDateValue(t.dateValue);
        long nanos = timeNanos + t.timeNanos;
        if (nanos >= DateTimeUtils.NANOS_PER_DAY) {
            nanos -= DateTimeUtils.NANOS_PER_DAY;
            absoluteDay++;
        }
        return ValueTimestamp.fromDateValueAndNanos(DateTimeUtils.dateValueFromAbsoluteDay(absoluteDay), nanos);
    }

    @Override
    public Value subtract(Value v) {
        ValueTimestamp t = (ValueTimestamp) v;
        long absoluteDay = DateTimeUtils.absoluteDayFromDateValue(dateValue)
                - DateTimeUtils.absoluteDayFromDateValue(t.dateValue);
        long nanos = timeNanos - t.timeNanos;
        if (nanos < 0) {
            nanos += DateTimeUtils.NANOS_PER_DAY;
            absoluteDay--;
        }
        return ValueTimestamp.fromDateValueAndNanos(DateTimeUtils.dateValueFromAbsoluteDay(absoluteDay), nanos);
    }

}
