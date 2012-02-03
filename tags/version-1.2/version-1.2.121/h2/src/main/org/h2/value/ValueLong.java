/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.Message;

/**
 * Implementation of the BIGINT data type.
 */
public class ValueLong extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 19;

    /**
     * The maximum display size of a long.
     * Example: 9223372036854775808
     */
    public static final int DISPLAY_SIZE = 20;

    private static final int STATIC_SIZE = 100;
    private static final ValueLong[] STATIC_CACHE;
    private static final BigInteger MIN = new BigInteger("" + Long.MIN_VALUE);
    private static final BigInteger MAX = new BigInteger("" + Long.MAX_VALUE);

    private final long value;

    static {
        STATIC_CACHE = new ValueLong[STATIC_SIZE];
        for (int i = 0; i < STATIC_SIZE; i++) {
            STATIC_CACHE[i] = new ValueLong(i);
        }
    }

    private ValueLong(long value) {
        this.value = value;
    }

    public Value add(Value v) throws SQLException {
        ValueLong other = (ValueLong) v;
        if (SysProperties.OVERFLOW_EXCEPTIONS) {
            long result = value + other.value;
            int sv = value == 0 ? 0 : (value < 0 ? -1 : 1);
            int so = other.value == 0 ? 0 : (other.value < 0 ? -1 : 1);
            int sr = result == 0 ? 0 : (result < 0 ? -1 : 1);
            // if the operands have different signs overflow can not occur
            // if the operands have the same sign,
            // and the result has a different sign, then it is an overflow
            // it can not be an overflow when one of the operands is 0
            if (sv != so || sr == so || sv == 0 || so == 0) {
                return ValueLong.get(result);
            }
            throw getOverflow();
        }
        return ValueLong.get(value + other.value);
    }

    public int getSignum() {
        return value == 0 ? 0 : (value < 0 ? -1 : 1);
    }

    public Value negate() throws SQLException {
        if (SysProperties.OVERFLOW_EXCEPTIONS) {
            if (value == Long.MIN_VALUE) {
                throw getOverflow();
            }
        }
        return ValueLong.get(-value);
    }

    private SQLException getOverflow() {
        return Message.getSQLException(ErrorCode.OVERFLOW_FOR_TYPE_1, DataType.getDataType(Value.LONG).name);
    }

    public Value subtract(Value v) throws SQLException {
        ValueLong other = (ValueLong) v;
        if (SysProperties.OVERFLOW_EXCEPTIONS) {
            int sv = value == 0 ? 0 : (value < 0 ? -1 : 1);
            int so = other.value == 0 ? 0 : (other.value < 0 ? -1 : 1);
            // if the operands have the same sign, then overflow can not occur
            // if the second operand is 0, then overflow can not occur
            if (sv == so || so == 0) {
                return ValueLong.get(value - other.value);
            }
            // now, if the other value is Long.MIN_VALUE, it must be an overflow
            // x - Long.MIN_VALUE overflows for x>=0
            return add(other.negate());
        }
        return ValueLong.get(value - other.value);
    }

    private boolean isInteger(long a) {
        return a >= Integer.MIN_VALUE && a <= Integer.MAX_VALUE;
    }

    public Value multiply(Value v) throws SQLException {
        ValueLong other = (ValueLong) v;
        if (SysProperties.OVERFLOW_EXCEPTIONS) {
            long result = value * other.value;
            if (value == 0 || value == 1 || other.value == 0 || other.value == 1) {
                return ValueLong.get(result);
            }
            if (isInteger(value) && isInteger(other.value)) {
                return ValueLong.get(result);
            }
            // just checking one case is not enough: Long.MIN_VALUE * -1
            // probably this is correct but I'm not sure
            // if(result / value == other.value && result / other.value == value) {
            //    return ValueLong.get(result);
            //}
            BigInteger bv = new BigInteger("" + value);
            BigInteger bo = new BigInteger("" + other.value);
            BigInteger br = bv.multiply(bo);
            if (br.compareTo(MIN) < 0 || br.compareTo(MAX) > 0) {
                throw getOverflow();
            }
            return ValueLong.get(br.longValue());
        }
        return ValueLong.get(value * other.value);
    }

    public Value divide(Value v) throws SQLException {
        ValueLong other = (ValueLong) v;
        if (other.value == 0) {
            throw Message.getSQLException(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueLong.get(value / other.value);
    }

    public String getSQL() {
        return getString();
    }

    public int getType() {
        return Value.LONG;
    }

    public long getLong() {
        return value;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueLong v = (ValueLong) o;
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
        return (int) (value ^ (value >> 32));
    }

    public Object getObject() {
        return value;
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setLong(parameterIndex, value);
    }

    /**
     * Get or create a long value for the given long.
     *
     * @param i the long
     * @return the value
     */
    public static ValueLong get(long i) {
        if (i >= 0 && i < STATIC_SIZE) {
            return STATIC_CACHE[(int) i];
        }
        return (ValueLong) Value.cache(new ValueLong(i));
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    public boolean equals(Object other) {
        return other instanceof ValueLong && value == ((ValueLong) other).value;
    }

}
