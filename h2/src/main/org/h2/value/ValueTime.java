/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import org.h2.util.DateTimeUtils;
import org.h2.util.MathUtils;

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
     * Parse a string to a ValueTime.
     *
     * @param s the string to parse
     * @return the time
     */
    public static ValueTime parse(String s) {
        return new ValueTime(DateTimeUtils.parseTime(s));
    }

    public Time getTime() {
        return DateTimeUtils.convertNanoToTime(nanos);
    }

    public long getNanos() {
        return nanos;
    }

    public String getSQL() {
        return "TIME '" + getString() + "'";
    }

    public int getType() {
        return Value.TIME;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        return MathUtils.compareLong(nanos, ((ValueTime) o).nanos);
    }

    public String getString() {
        StringBuilder buff = new StringBuilder(DISPLAY_SIZE);
        DateTimeUtils.appendTime(buff, nanos, false);
        return buff.toString();
    }

    public long getPrecision() {
        return PRECISION;
    }

    public int hashCode() {
        return (int) (nanos ^ (nanos >>> 32));
    }

    public Object getObject() {
        return getTime();
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setTime(parameterIndex, getTime());
    }

    /**
     * Get or create a time value for the given time.
     *
     * @param time the time
     * @return the value
     */
    public static ValueTime get(Time time) {
        long x = DateTimeUtils.nanosFromDate(time.getTime());
        return get(x);
    }

    /**
     * Get or create a time value.
     *
     * @param nanos the nanoseconds
     * @return the value
     */
    public static ValueTime get(long nanos) {
        return (ValueTime) Value.cache(new ValueTime(nanos));
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof ValueTime && nanos == (((ValueTime) other).nanos);
    }

    public Value add(Value v) {
        ValueTime t = (ValueTime) v.convertTo(Value.TIME);
        return ValueTime.get(nanos + t.getNanos());
    }

    public Value subtract(Value v) {
        ValueTime t = (ValueTime) v.convertTo(Value.TIME);
        return ValueTime.get(nanos - t.getNanos());
    }

    public Value multiply(Value v) {
        return ValueTime.get((long) (nanos * v.getDouble()));
    }

    public Value divide(Value v) {
        return ValueTime.get((long) (nanos / v.getDouble()));
    }

}
