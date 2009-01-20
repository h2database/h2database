/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;

import org.h2.constant.ErrorCode;
import org.h2.message.Message;
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

    /**
     * This is used to find out if a date is possibly BC. Because of time zone
     * issues (the date is time zone specific), the second day is used. That
     * means the value is not exact, but it does not need to be.
     */
    static final long YEAR_ONE = java.sql.Date.valueOf("0001-01-02").getTime();

    private final Timestamp value;

    private ValueTimestamp(Timestamp value) {
        this.value = value;
    }

    public Timestamp getTimestamp() {
        return (Timestamp) value.clone();
    }

    public Timestamp getTimestampNoCopy() {
        return value;
    }

    public String getSQL() {
        return "TIMESTAMP '" + getString() + "'";
    }

    /**
     * Parse a string to a java.sql.Timestamp object.
     *
     * @param s the string to parse
     * @return the timestamp
     */
    public static Timestamp parseTimestamp(String s) throws SQLException {
        return (Timestamp) DateTimeUtils.parseDateTime(s, Value.TIMESTAMP, ErrorCode.TIMESTAMP_CONSTANT_2);
    }

    public int getType() {
        return Value.TIMESTAMP;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueTimestamp v = (ValueTimestamp) o;
        int c = value.compareTo(v.value);
        return c == 0 ? 0 : (c < 0 ? -1 : 1);
    }

    public String getString() {
        String s = value.toString();
        long time = value.getTime();
        // special case: java.sql.Timestamp doesn't format
        // years below year 1 (BC) correctly
        if (time < YEAR_ONE) {
            int year = DateTimeUtils.getDatePart(value, Calendar.YEAR);
            if (year < 1) {
                s = year + s.substring(s.indexOf('-'));
            }
        }
        return s;
    }

    public long getPrecision() {
        return PRECISION;
    }

    public int getScale() {
        return DEFAULT_SCALE;
    }

    public int hashCode() {
        return value.hashCode();
    }

    public Object getObject() {
        // this class is mutable - must copy the object
        return getTimestamp();
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setTimestamp(parameterIndex, value);
    }

    /**
     * Get or create a timestamp value for the given timestamp.
     * Clone the timestamp.
     *
     * @param timestamp the timestamp
     * @return the value
     */
    public static ValueTimestamp get(Timestamp timestamp) {
        timestamp = (Timestamp) timestamp.clone();
        return getNoCopy(timestamp);
    }

    /**
     * Get or create a timestamp value for the given timestamp.
     * Do not clone the timestamp.
     *
     * @param timestamp the timestamp
     * @return the value
     */
    public static ValueTimestamp getNoCopy(Timestamp timestamp) {
        return (ValueTimestamp) Value.cache(new ValueTimestamp(timestamp));
    }

    public Value convertScale(boolean onlyToSmallerScale, int targetScale) throws SQLException {
        if (targetScale < 0 || targetScale > DEFAULT_SCALE) {
            // TODO convertScale for Timestamps: may throw an exception?
            throw Message.getInvalidValueException(""+targetScale, "scale");
        }
        int nanos = value.getNanos();
        BigDecimal bd = new BigDecimal("" + nanos);
        bd = bd.movePointLeft(9);
        bd = MathUtils.setScale(bd, targetScale);
        bd = bd.movePointRight(9);
        int n2 = bd.intValue();
        if (n2 == nanos) {
            return this;
        }
        long t = value.getTime();
        while (n2 >= 1000000000) {
            t += 1000;
            n2 -= 1000000000;
        }
        Timestamp t2 = new Timestamp(t);
        t2.setNanos(n2);
        return ValueTimestamp.getNoCopy(t2);
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    public boolean equals(Object other) {
        return other instanceof ValueTimestamp && value.equals(((ValueTimestamp) other).value);
    }

}
