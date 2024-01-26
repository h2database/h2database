/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;

/**
 * Implementation of the SMALLINT data type.
 */
public final class ValueSmallint extends Value {

    /**
     * The precision in bits.
     */
    static final int PRECISION = 16;

    /**
     * The approximate precision in decimal digits.
     */
    public static final int DECIMAL_PRECISION = 5;

    /**
     * The maximum display size of a SMALLINT.
     * Example: -32768
     */
    static final int DISPLAY_SIZE = 6;

    private final short value;

    private ValueSmallint(short value) {
        this.value = value;
    }

    @Override
    public Value add(Value v) {
        ValueSmallint other = (ValueSmallint) v;
        return checkRange(value + other.value);
    }

    private static ValueSmallint checkRange(int x) {
        if ((short) x != x) {
            throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1,
                    Integer.toString(x));
        }
        return ValueSmallint.get((short) x);
    }

    @Override
    public int getSignum() {
        return Integer.signum(value);
    }

    @Override
    public Value negate() {
        return checkRange(-(int) value);
    }

    @Override
    public Value subtract(Value v) {
        ValueSmallint other = (ValueSmallint) v;
        return checkRange(value - other.value);
    }

    @Override
    public Value multiply(Value v) {
        ValueSmallint other = (ValueSmallint) v;
        return checkRange(value * other.value);
    }

    @Override
    public Value divide(Value v, TypeInfo quotientType) {
        ValueSmallint other = (ValueSmallint) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
        }
        return checkRange(value / other.value);
    }

    @Override
    public Value modulus(Value v) {
        ValueSmallint other = (ValueSmallint) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
        }
        return ValueSmallint.get((short) (value % other.value));
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        if ((sqlFlags & NO_CASTS) == 0) {
            return builder.append("CAST(").append(value).append(" AS SMALLINT)");
        }
        return builder.append(value);
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_SMALLINT;
    }

    @Override
    public int getValueType() {
        return SMALLINT;
    }

    @Override
    public byte[] getBytes() {
        short value = this.value;
        return new byte[] { (byte) (value >> 8), (byte) value };
    }

    @Override
    public short getShort() {
        return value;
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
        return Integer.compare(value, ((ValueSmallint) o).value);
    }

    @Override
    public String getString() {
        return Integer.toString(value);
    }

    @Override
    public int hashCode() {
        return value;
    }

    /**
     * Get or create a SMALLINT value for the given short.
     *
     * @param i the short
     * @return the value
     */
    public static ValueSmallint get(short i) {
        return (ValueSmallint) Value.cache(new ValueSmallint(i));
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueSmallint && value == ((ValueSmallint) other).value;
    }

}
