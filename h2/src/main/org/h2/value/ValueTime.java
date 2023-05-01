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

import static org.h2.util.DateTimeUtils.NANOS_PER_HOUR;

/**
 * Implementation of the TIME data type.
 */
public final class ValueTime extends Value {

    /**
     * The default precision and display size of the textual representation of a time.
     * Example: 10:00:00
     */
    public static final int DEFAULT_PRECISION = 8;

    /**
     * The maximum precision and display size of the textual representation of a time.
     * Example: 10:00:00.123456789
     */
    public static final int MAXIMUM_PRECISION = 18;

    /**
     * The default scale for time.
     */
    public static final int DEFAULT_SCALE = 0;

    /**
     * The maximum scale for time.
     */
    public static final int MAXIMUM_SCALE = 9;

    private static final ValueTime[] STATIC_CACHE;

    /**
     * Nanoseconds since midnight
     */
    private final long nanos;

    static {
        ValueTime[] cache = new ValueTime[24];
        for (int hour = 0; hour < 24; hour++) {
            cache[hour] = new ValueTime(hour * NANOS_PER_HOUR);
        }
        STATIC_CACHE = cache;
    }

    /**
     * @param nanos nanoseconds since midnight
     */
    private ValueTime(long nanos) {
        this.nanos = nanos;
    }

    /**
     * Get or create a time value.
     *
     * @param nanos the nanoseconds since midnight
     * @return the value
     */
    public static ValueTime fromNanos(long nanos) {
        if (nanos < 0L || nanos >= DateTimeUtils.NANOS_PER_DAY) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2, "TIME",
                    DateTimeUtils.appendTime(new StringBuilder(), nanos).toString());
        }
        if (nanos % NANOS_PER_HOUR == 0L) {
            return STATIC_CACHE[(int) (nanos / NANOS_PER_HOUR)];
        }
        return (ValueTime) Value.cache(new ValueTime(nanos));
    }

    /**
     * Parse a string to a ValueTime.
     *
     * @param s the string to parse
     * @param provider
     *            the cast information provider, may be {@code null} for
     *            literals without time zone
     * @return the time
     */
    public static ValueTime parse(String s, CastDataProvider provider) {
        try {
            return (ValueTime) DateTimeUtils.parseTime(s, provider, false);
        } catch (Exception e) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2,
                    e, "TIME", s);
        }
    }

    /**
     * @return nanoseconds since midnight
     */
    public long getNanos() {
        return nanos;
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_TIME;
    }

    @Override
    public int getValueType() {
        return TIME;
    }

    @Override
    public String getString() {
        return DateTimeUtils.appendTime(new StringBuilder(MAXIMUM_PRECISION), nanos).toString();
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return DateTimeUtils.appendTime(builder.append("TIME '"), nanos).append('\'');
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        return Long.compare(nanos, ((ValueTime) o).nanos);
    }

    @Override
    public boolean equals(Object other) {
        return this == other || other instanceof ValueTime && nanos == ((ValueTime) other).nanos;
    }

    @Override
    public int hashCode() {
        return (int) (nanos ^ (nanos >>> 32));
    }

    @Override
    public Value add(Value v) {
        ValueTime t = (ValueTime) v;
        return ValueTime.fromNanos(nanos + t.getNanos());
    }

    @Override
    public Value subtract(Value v) {
        ValueTime t = (ValueTime) v;
        return ValueTime.fromNanos(nanos - t.getNanos());
    }

    @Override
    public Value multiply(Value v) {
        return ValueTime.fromNanos((long) (nanos * v.getDouble()));
    }

    @Override
    public Value divide(Value v, TypeInfo quotientType) {
        return ValueTime.fromNanos((long) (nanos / v.getDouble()));
    }

}
