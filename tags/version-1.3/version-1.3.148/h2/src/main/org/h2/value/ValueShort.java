/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.MathUtils;

/**
 * Implementation of the SMALLINT data type.
 */
public class ValueShort extends Value {

    /**
     * The precision in digits.
     */
    static final int PRECISION = 5;

    /**
     * The maximum display size of a short.
     * Example: -32768
     */
    static final int DISPLAY_SIZE = 6;

    private final short value;

    private ValueShort(short value) {
        this.value = value;
    }

    public Value add(Value v) {
        ValueShort other = (ValueShort) v;
        return checkRange(value + other.value);
    }

    private ValueShort checkRange(int x) {
        if (x < Short.MIN_VALUE || x > Short.MAX_VALUE) {
            throw DbException.get(ErrorCode.OVERFLOW_FOR_TYPE_1, DataType.getDataType(Value.SHORT).name);
        }
        return ValueShort.get((short) x);
    }

    public int getSignum() {
        return Integer.signum(value);
    }

    public Value negate() {
        return checkRange(-(int) value);
    }

    public Value subtract(Value v) {
        ValueShort other = (ValueShort) v;
        return checkRange(value - other.value);
    }

    public Value multiply(Value v) {
        ValueShort other = (ValueShort) v;
        return checkRange(value * other.value);
    }

    public Value divide(Value v) {
        ValueShort other = (ValueShort) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueShort.get((short) (value / other.value));
    }

    public String getSQL() {
        return getString();
    }

    public int getType() {
        return Value.SHORT;
    }

    public short getShort() {
        return value;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueShort v = (ValueShort) o;
        return MathUtils.compareInt(value, v.value);
    }

    public String getString() {
        return String.valueOf(value);
    }

    public long getPrecision() {
        return PRECISION;
    }

    public int hashCode() {
        return value;
    }

    public Object getObject() {
        return Short.valueOf(value);
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setShort(parameterIndex, value);
    }

    /**
     * Get or create a short value for the given short.
     *
     * @param i the short
     * @return the value
     */
    public static ValueShort get(short i) {
        return (ValueShort) Value.cache(new ValueShort(i));
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    public boolean equals(Object other) {
        return other instanceof ValueShort && value == ((ValueShort) other).value;
    }

}
