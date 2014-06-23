/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import org.h2.constant.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;

/**
 * Implementation of the TIME data type.
 */
public class ValueTime extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 6;

    /**
     * The display size of the textual representation of a time.
     * Example: 10:00:00
     */
    static final int DISPLAY_SIZE = 8;

    private final long nanos;

    private ValueTime(long nanos) {
        this.nanos = nanos;
    }

    /**
     * Get or create a time value.
     *
     * @param nanos the nanoseconds
     * @return the value
     */
    public static ValueTime fromNanos(long nanos) {
        return (ValueTime) Value.cache(new ValueTime(nanos));
    }

    /**
     * Get or create a time value for the given time.
     *
     * @param time the time
     * @return the value
     */
    public static ValueTime get(Time time) {
        return fromNanos(DateTimeUtils.nanosFromDate(time.getTime()));
    }

    /**
     * Parse a string to a ValueTime.
     *
     * @param s the string to parse
     * @return the time
     */

    public static ValueTime parse(String s) {
        try {
            return fromNanos(DateTimeUtils.parseTimeNanos(s, 0, s.length(), false));
        } catch (Exception e) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2,
                    e, "TIME", s);
        }
    }

    public long getNanos() {
        return nanos;
    }

    @Override
    public Time getTime() {
        return DateTimeUtils.convertNanoToTime(nanos);
    }

    @Override
    public int getType() {
        return Value.TIME;
    }

    @Override
    public String getString() {
        StringBuilder buff = new StringBuilder(DISPLAY_SIZE);
        appendTime(buff, nanos, false);
        return buff.toString();
    }

    @Override
    public String getSQL() {
        return "TIME '" + getString() + "'";
    }

    @Override
    public long getPrecision() {
        return PRECISION;
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        return MathUtils.compareLong(nanos, ((ValueTime) o).nanos);
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
        return getTime();
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setTime(parameterIndex, getTime());
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

    @Override
    public int getSignum() {
        return Long.signum(nanos);
    }

    @Override
    public Value negate() {
        return ValueTime.fromNanos(-nanos);
    }

    /**
     * Append a time to the string builder.
     *
     * @param buff the target string builder
     * @param nanos the time in nanoseconds
     * @param alwaysAddMillis whether to always add at least ".0"
     */
    static void appendTime(StringBuilder buff, long nanos, boolean alwaysAddMillis) {
        if (nanos < 0) {
            buff.append('-');
            nanos = -nanos;
        }
        long ms = nanos / 1000000;
        nanos -= ms * 1000000;
        long s = ms / 1000;
        ms -= s * 1000;
        long m = s / 60;
        s -= m * 60;
        long h = m / 60;
        m -= h * 60;
        StringUtils.appendZeroPadded(buff, 2, h);
        buff.append(':');
        StringUtils.appendZeroPadded(buff, 2, m);
        buff.append(':');
        StringUtils.appendZeroPadded(buff, 2, s);
        if (alwaysAddMillis || ms > 0 || nanos > 0) {
            buff.append('.');
            int start = buff.length();
            StringUtils.appendZeroPadded(buff, 3, ms);
            if (nanos > 0) {
                StringUtils.appendZeroPadded(buff, 6, nanos);
            }
            for (int i = buff.length() - 1; i > start; i--) {
                if (buff.charAt(i) != '0') {
                    break;
                }
                buff.deleteCharAt(i);
            }
        }
    }

}
