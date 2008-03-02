/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import org.h2.constant.ErrorCode;
import org.h2.util.DateTimeUtils;

/**
 * Implementation of the TIME data type.
 */
public class ValueTime extends Value {
    public static final int PRECISION = 6;
    public static final int DISPLAY_SIZE = 8; // 10:00:00

    private final Time value;

    private ValueTime(Time value) {
        this.value = value;
    }

    public static Time parseTime(String s) throws SQLException {
        return (Time) DateTimeUtils.parseDateTime(s, Value.TIME, ErrorCode.TIME_CONSTANT_2);
    }

    public Time getTime() {
        // this class is mutable - must copy the object
        return (Time) value.clone();
    }

    public Time getTimeNoCopy() {
        return value;
    }

    public String getSQL() {
        return "TIME '" + getString() + "'";
    }

    public int getType() {
        return Value.TIME;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueTime v = (ValueTime) o;
        int c = value.compareTo(v.value);
        return c == 0 ? 0 : (c < 0 ? -1 : 1);
    }

    public String getString() {
        return value.toString();
    }

    public long getPrecision() {
        return PRECISION;
    }

    public int hashCode() {
        return value.hashCode();
    }

    public Object getObject() {
        return getTime();
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setTime(parameterIndex, value);
    }

    public static ValueTime get(Time time) {
        time = DateTimeUtils.cloneAndNormalizeTime(time);
        return getNoCopy(time);
    }

    public static ValueTime getNoCopy(Time time) {
        return (ValueTime) Value.cache(new ValueTime(time));
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    public boolean equals(Object other) {
        return other instanceof ValueTime && value.equals(((ValueTime) other).value);
    }

}
