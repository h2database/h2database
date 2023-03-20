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

/**
 * Implementation of the TINYINT data type.
 */
public final class ValueTinyint extends Value {

    /**
     * The precision in bits.
     */
    static final int PRECISION = 8;

    /**
     * The approximate precision in decimal digits.
     */
    public static final int DECIMAL_PRECISION = 3;

    /**
     * The display size for a TINYINT.
     * Example: -127
     */
    static final int DISPLAY_SIZE = 4;

    private static final ValueTinyint[] STATIC_CACHE;

    private final byte value;

    static {
        ValueTinyint[] cache = new ValueTinyint[256];
        for (int i = 0; i < 256; i++) {
            cache[i] = new ValueTinyint((byte) (i - 128));
        }
        STATIC_CACHE = cache;
    }

    private ValueTinyint(byte value) {
        this.value = value;
    }

    @Override
    public Value add(Value v) {
        ValueTinyint other = (ValueTinyint) v;
        return checkRange(value + other.value);
    }

    private static ValueTinyint checkRange(int x) {
        if ((byte) x != x) {
            throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1,
                    Integer.toString(x));
        }
        return ValueTinyint.get((byte) x);
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
        ValueTinyint other = (ValueTinyint) v;
        return checkRange(value - other.value);
    }

    @Override
    public Value multiply(Value v) {
        ValueTinyint other = (ValueTinyint) v;
        return checkRange(value * other.value);
    }

    @Override
    public Value divide(Value v, TypeInfo quotientType) {
        ValueTinyint other = (ValueTinyint) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
        }
        return checkRange(value / other.value);
    }

    @Override
    public Value modulus(Value v) {
        ValueTinyint other = (ValueTinyint) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
        }
        return ValueTinyint.get((byte) (value % other.value));
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        if ((sqlFlags & NO_CASTS) == 0) {
            return builder.append("CAST(").append(value).append(" AS TINYINT)");
        }
        return builder.append(value);
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_TINYINT;
    }

    @Override
    public int getValueType() {
        return TINYINT;
    }

    @Override
    public int getMemory() {
        // All possible values are statically initialized
        return 0;
    }

    @Override
    public byte[] getBytes() {
        return new byte[] { value };
    }

    @Override
    public byte getByte() {
        return value;
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
        return Integer.compare(value, ((ValueTinyint) o).value);
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
     * Get a TINYINT value for the given byte.
     *
     * @param i the byte
     * @return the value
     */
    public static ValueTinyint get(byte i) {
        return STATIC_CACHE[i + 128];
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueTinyint && value == ((ValueTinyint) other).value;
    }

}
