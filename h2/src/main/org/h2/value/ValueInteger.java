/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.util.Bits;

/**
 * Implementation of the INTEGER data type.
 */
public final class ValueInteger extends Value {

    /**
     * The precision in bits.
     */
    public static final int PRECISION = 32;

    /**
     * The approximate precision in decimal digits.
     */
    public static final int DECIMAL_PRECISION = 10;

    /**
     * The maximum display size of an INT.
     * Example: -2147483648
     */
    public static final int DISPLAY_SIZE = 11;

    private static final int STATIC_SIZE = 128;
    // must be a power of 2
    private static final int DYNAMIC_SIZE = 256;
    private static final ValueInteger[] STATIC_CACHE = new ValueInteger[STATIC_SIZE];
    private static final ValueInteger[] DYNAMIC_CACHE = new ValueInteger[DYNAMIC_SIZE];

    private final int value;

    static {
        for (int i = 0; i < STATIC_SIZE; i++) {
            STATIC_CACHE[i] = new ValueInteger(i);
        }
    }

    private ValueInteger(int value) {
        this.value = value;
    }

    /**
     * Get or create an INTEGER value for the given int.
     *
     * @param i the int
     * @return the value
     */
    public static ValueInteger get(int i) {
        if (i >= 0 && i < STATIC_SIZE) {
            return STATIC_CACHE[i];
        }
        ValueInteger v = DYNAMIC_CACHE[i & (DYNAMIC_SIZE - 1)];
        if (v == null || v.value != i) {
            v = new ValueInteger(i);
            DYNAMIC_CACHE[i & (DYNAMIC_SIZE - 1)] = v;
        }
        return v;
    }

    @Override
    public Value add(Value v) {
        ValueInteger other = (ValueInteger) v;
        return checkRange((long) value + (long) other.value);
    }

    private static ValueInteger checkRange(long x) {
        if ((int) x != x) {
            throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1, Long.toString(x));
        }
        return ValueInteger.get((int) x);
    }

    @Override
    public int getSignum() {
        return Integer.signum(value);
    }

    @Override
    public Value negate() {
        return checkRange(-(long) value);
    }

    @Override
    public Value subtract(Value v) {
        ValueInteger other = (ValueInteger) v;
        return checkRange((long) value - (long) other.value);
    }

    @Override
    public Value multiply(Value v) {
        ValueInteger other = (ValueInteger) v;
        return checkRange((long) value * (long) other.value);
    }

    @Override
    public Value divide(Value v, TypeInfo quotientType) {
        int y = ((ValueInteger) v).value;
        if (y == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
        }
        int x = value;
        if (x == Integer.MIN_VALUE && y == -1) {
            throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1, "2147483648");
        }
        return ValueInteger.get(x / y);
    }

    @Override
    public Value modulus(Value v) {
        ValueInteger other = (ValueInteger) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
        }
        return ValueInteger.get(value % other.value);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return builder.append(value);
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_INTEGER;
    }

    @Override
    public int getValueType() {
        return INTEGER;
    }

    @Override
    public byte[] getBytes() {
        byte[] b = new byte[4];
        Bits.writeInt(b, 0, getInt());
        return b;
    }

    @Override
    public int getInt() {
        return value;
    }

    @Override
    public long getLong() {
        return value;
    }

    @Override
    public BigDecimal getBigDecimal() {
        return BigDecimal.valueOf(value);
    }

    @Override
    public float getFloat() {
        return value;
    }

    @Override
    public double getDouble() {
        return value;
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        return Integer.compare(value, ((ValueInteger) o).value);
    }

    @Override
    public String getString() {
        return Integer.toString(value);
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueInteger && value == ((ValueInteger) other).value;
    }

}
