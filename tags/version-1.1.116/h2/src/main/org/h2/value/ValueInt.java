/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.Message;

/**
 * Implementation of the INT data type.
 */
public class ValueInt extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 10;

    /**
     * The maximum display size of an int.
     * Example: -2147483648
     */
    public static final int DISPLAY_SIZE = 11;

    private static final int STATIC_SIZE = 128;
    // must be a power of 2
    private static final int DYNAMIC_SIZE = 256;
    private static final ValueInt[] STATIC_CACHE = new ValueInt[STATIC_SIZE];
    private static final ValueInt[] DYNAMIC_CACHE = new ValueInt[DYNAMIC_SIZE];

    private final int value;

    static {
        for (int i = 0; i < STATIC_SIZE; i++) {
            STATIC_CACHE[i] = new ValueInt(i);
        }
    }

    private ValueInt(int value) {
        this.value = value;
    }

    /**
     * Get or create an int value for the given int.
     *
     * @param i the int
     * @return the value
     */
    public static ValueInt get(int i) {
        if (i >= 0 && i < STATIC_SIZE) {
            return STATIC_CACHE[i];
        }
        ValueInt v = DYNAMIC_CACHE[i & (DYNAMIC_SIZE - 1)];
        if (v == null || v.value != i) {
            v = new ValueInt(i);
            DYNAMIC_CACHE[i & (DYNAMIC_SIZE - 1)] = v;
        }
        return v;
    }

    public Value add(Value v) throws SQLException {
        ValueInt other = (ValueInt) v;
        if (SysProperties.OVERFLOW_EXCEPTIONS) {
            return checkRange((long) value + (long) other.value);
        }
        return ValueInt.get(value + other.value);
    }

    private ValueInt checkRange(long value) throws SQLException {
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw Message.getSQLException(ErrorCode.OVERFLOW_FOR_TYPE_1, DataType.getDataType(Value.INT).name);
        }
        return ValueInt.get((int) value);
    }

    public int getSignum() {
        return value == 0 ? 0 : (value < 0 ? -1 : 1);
    }

    public Value negate() throws SQLException {
        if (SysProperties.OVERFLOW_EXCEPTIONS) {
            return checkRange(-(long) value);
        }
        return ValueInt.get(-value);
    }

    public Value subtract(Value v) throws SQLException {
        ValueInt other = (ValueInt) v;
        if (SysProperties.OVERFLOW_EXCEPTIONS) {
            return checkRange((long) value - (long) other.value);
        }
        return ValueInt.get(value - other.value);
    }

    public Value multiply(Value v) throws SQLException {
        ValueInt other = (ValueInt) v;
        if (SysProperties.OVERFLOW_EXCEPTIONS) {
            return checkRange((long) value * (long) other.value);
        }
        return ValueInt.get(value * other.value);
    }

    public Value divide(Value v) throws SQLException {
        ValueInt other = (ValueInt) v;
        if (other.value == 0) {
            throw Message.getSQLException(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueInt.get(value / other.value);
    }

    public String getSQL() {
        return getString();
    }

    public int getType() {
        return Value.INT;
    }

    public int getInt() {
        return value;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueInt v = (ValueInt) o;
        if (value == v.value) {
            return 0;
        }
        return value > v.value ? 1 : -1;
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
        return value;
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setInt(parameterIndex, value);
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    public boolean equals(Object other) {
        return other instanceof ValueInt && value == ((ValueInt) other).value;
    }

}
