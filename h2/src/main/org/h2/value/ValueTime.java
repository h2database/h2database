/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.util.TimeZone;
import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;
import org.h2.util.JSR310;
import org.h2.util.JSR310Utils;

/**
 * Implementation of the TIME data type.
 */
public class ValueTime extends Value {

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
    static final int DEFAULT_SCALE = 0;

    /**
     * The maximum scale for time.
     */
    public static final int MAXIMUM_SCALE = 9;

    /**
     * Nanoseconds since midnight
     */
    private final long nanos;

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
            StringBuilder builder = new StringBuilder();
            DateTimeUtils.appendTime(builder, nanos);
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2,
                    "TIME", builder.toString());
        }
        return (ValueTime) Value.cache(new ValueTime(nanos));
    }

    /**
     * Get or create a time value for the given time.
     *
     * @param timeZone time zone, or {@code null} for default
     * @param time the time
     * @return the value
     */
    public static ValueTime get(TimeZone timeZone, Time time) {
        long ms = time.getTime();
        return fromNanos(DateTimeUtils.nanosFromLocalMillis(
                ms + (timeZone == null ? DateTimeUtils.getTimeZoneOffsetMillis(ms) : timeZone.getOffset(ms))));
    }

    /**
     * Parse a string to a ValueTime.
     *
     * @param s the string to parse
     * @return the time
     */
    public static ValueTime parse(String s) {
        try {
            return fromNanos(DateTimeUtils.parseTimeNanos(s, 0, s.length()));
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
    public Time getTime(TimeZone timeZone) {
        return new Time(DateTimeUtils.getMillis(timeZone, DateTimeUtils.EPOCH_DATE_VALUE, nanos));
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
        StringBuilder buff = new StringBuilder(MAXIMUM_PRECISION);
        DateTimeUtils.appendTime(buff, nanos);
        return buff.toString();
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        builder.append("TIME '");
        DateTimeUtils.appendTime(builder, nanos);
        return builder.append('\'');
    }

    @Override
    public boolean checkPrecision(long precision) {
        // TIME data type does not have precision parameter
        return true;
    }

    @Override
    public Value convertScale(boolean onlyToSmallerScale, int targetScale) {
        if (targetScale >= MAXIMUM_SCALE) {
            return this;
        }
        if (targetScale < 0) {
            throw DbException.getInvalidValueException("scale", targetScale);
        }
        long n = nanos;
        long n2 = DateTimeUtils.convertScale(n, targetScale, DateTimeUtils.NANOS_PER_DAY);
        if (n2 == n) {
            return this;
        }
        return fromNanos(n2);
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        return Long.compare(nanos, ((ValueTime) o).nanos);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof ValueTime && nanos == (((ValueTime) other).nanos);
    }

    @Override
    public int hashCode() {
        return (int) (nanos ^ (nanos >>> 32));
    }

    @Override
    public Object getObject() {
        return getTime(null);
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        if (JSR310.PRESENT) {
            try {
                prep.setObject(parameterIndex, JSR310Utils.valueToLocalTime(this), Types.TIME);
                return;
            } catch (SQLException ignore) {
                // Nothing to do
            }
        }
        prep.setTime(parameterIndex, getTime(null));
    }

    @Override
    public Value add(Value v) {
        ValueTime t = (ValueTime) v.convertTo(Value.TIME);
        return ValueTime.fromNanos(nanos + t.getNanos());
    }

    @Override
    public Value subtract(Value v) {
        ValueTime t = (ValueTime) v.convertTo(Value.TIME);
        return ValueTime.fromNanos(nanos - t.getNanos());
    }

    @Override
    public Value multiply(Value v) {
        return ValueTime.fromNanos((long) (nanos * v.getDouble()));
    }

    @Override
    public Value divide(Value v) {
        return ValueTime.fromNanos((long) (nanos / v.getDouble()));
    }

}
