/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.h2.message.DbException;
import org.h2.util.DateTimeUtils;
import org.h2.util.MathUtils;

/**
 * Implementation of the TIMESTAMP data type.
 */
public class ValueTimestamp extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 23;

    /**
     * The display size of the textual representation of a timestamp.
     * Example: 2001-01-01 23:59:59.000
     */
    static final int DISPLAY_SIZE = 23;

    /**
     * The default scale for timestamps.
     */
    static final int DEFAULT_SCALE = 10;

    private final long dateValue;
    private final long nanos;

    private ValueTimestamp(long dateValue, long nanos) {
        this.dateValue = dateValue;
        this.nanos = nanos;
    }

    public Timestamp getTimestamp() {
        return DateTimeUtils.convertDateValueToTimestamp(dateValue, nanos);
    }

    public long getDateValue() {
        return dateValue;
    }

    public long getNanos() {
        return nanos;
    }

    public String getSQL() {
        return "TIMESTAMP '" + getString() + "'";
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param dateValue the date value
     * @param nanos the nanoseconds
     * @return the value
     */
    public static ValueTimestamp get(long dateValue, long nanos) {
        return (ValueTimestamp) Value.cache(new ValueTimestamp(dateValue, nanos));
    }

    /**
     * Parse a string to a ValueTimestamp.
     *
     * @param s the string to parse
     * @return the date
     */
    public static ValueTimestamp parse(String s) {
        Value x = DateTimeUtils.parse(s, Value.TIMESTAMP);
        return (ValueTimestamp) Value.cache(x);
    }

    public int getType() {
        return Value.TIMESTAMP;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueTimestamp t = (ValueTimestamp) o;
        int c = MathUtils.compareLong(dateValue, t.dateValue);
        if (c != 0) {
            return c;
        }
        return MathUtils.compareLong(nanos, t.nanos);
    }

    public String getString() {
        // TODO verify display size
        StringBuilder buff = new StringBuilder(DISPLAY_SIZE);
        DateTimeUtils.appendDate(buff, dateValue);
        buff.append(' ');
        DateTimeUtils.appendTime(buff, nanos, true);
        return buff.toString();
    }

    public long getPrecision() {
        return PRECISION;
    }

    public int getScale() {
        return DEFAULT_SCALE;
    }

    public int hashCode() {
        return (int) (dateValue ^ (dateValue >>> 32) ^ nanos ^ (nanos >>> 32));
    }

    public Object getObject() {
        return getTimestamp();
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setTimestamp(parameterIndex, getTimestamp());
    }

    /**
     * Get or create a timestamp value for the given timestamp.
     *
     * @param timestamp the timestamp
     * @return the value
     */
    public static ValueTimestamp get(Timestamp timestamp) {
        long ms = timestamp.getTime();
        long dateValue = DateTimeUtils.dateValueFromDate(ms);
        long nanos = DateTimeUtils.nanosFromDate(ms);
        nanos += timestamp.getNanos() % 1000000;
        return get(dateValue, nanos);
    }

    public Value convertScale(boolean onlyToSmallerScale, int targetScale) {
        if (targetScale == DEFAULT_SCALE) {
            return this;
        }
        if (targetScale < 0 || targetScale > DEFAULT_SCALE) {
            throw DbException.getInvalidValueException("scale", targetScale);
        }
        long n = nanos;
        BigDecimal bd = BigDecimal.valueOf(n);
        bd = bd.movePointLeft(9);
        bd = MathUtils.setScale(bd, targetScale);
        bd = bd.movePointRight(9);
        long n2 = bd.longValue();
        if (n2 == n) {
            return this;
        }
        return get(dateValue, n2);
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof ValueTimestamp)) {
            return false;
        }
        ValueTimestamp x = (ValueTimestamp) other;
        return dateValue == x.dateValue && nanos == x.nanos;
    }

    public Value add(Value v) {
        // TODO test sum of timestamps, dates, times
        ValueTimestamp t = (ValueTimestamp) v.convertTo(Value.TIMESTAMP);
        long d1 = DateTimeUtils.absoluteDayFromDateValue(dateValue);
        long d2 = DateTimeUtils.absoluteDayFromDateValue(t.dateValue);
        return DateTimeUtils.normalize(d1 + d2, nanos + t.nanos);
    }

    public Value subtract(Value v) {
        // TODO test sum of timestamps, dates, times
        ValueTimestamp t = (ValueTimestamp) v.convertTo(Value.TIMESTAMP);
        long d1 = DateTimeUtils.absoluteDayFromDateValue(dateValue);
        long d2 = DateTimeUtils.absoluteDayFromDateValue(t.dateValue);
        return DateTimeUtils.normalize(d1 - d2, nanos - t.nanos);
    }

}
