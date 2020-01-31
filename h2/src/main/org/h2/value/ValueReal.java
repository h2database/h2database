/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;

/**
 * Implementation of the REAL data type.
 */
public final class ValueReal extends Value {

    /**
     * The precision in digits.
     */
    static final int PRECISION = 7;

    /**
     * The maximum display size of a REAL.
     * Example: -1.12345676E-20
     */
    static final int DISPLAY_SIZE = 15;

    /**
     * Float.floatToIntBits(0f).
     */
    public static final int ZERO_BITS = 0;

    /**
     * The value 0.
     */
    public static final ValueReal ZERO = new ValueReal(0f);

    /**
     * The value 1.
     */
    public static final ValueReal ONE = new ValueReal(1f);

    private static final ValueReal NAN = new ValueReal(Float.NaN);

    private final float value;

    private ValueReal(float value) {
        this.value = value;
    }

    @Override
    public Value add(Value v) {
        ValueReal v2 = (ValueReal) v;
        return get(value + v2.value);
    }

    @Override
    public Value subtract(Value v) {
        ValueReal v2 = (ValueReal) v;
        return get(value - v2.value);
    }

    @Override
    public Value negate() {
        return get(-value);
    }

    @Override
    public Value multiply(Value v) {
        ValueReal v2 = (ValueReal) v;
        return get(value * v2.value);
    }

    @Override
    public Value divide(Value v, long divisorPrecision) {
        ValueReal v2 = (ValueReal) v;
        if (v2.value == 0.0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
        }
        return get(value / v2.value);
    }

    @Override
    public Value modulus(Value v) {
        ValueReal other = (ValueReal) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
        }
        return get(value % other.value);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        if ((sqlFlags & NO_CASTS) == 0) {
            return getSQL(builder.append("CAST(")).append(" AS REAL)");
        }
        return getSQL(builder);
    }

    private StringBuilder getSQL(StringBuilder builder) {
        if (value == Float.POSITIVE_INFINITY) {
            builder.append("POWER(0, -1)");
        } else if (value == Float.NEGATIVE_INFINITY) {
            builder.append("(-POWER(0, -1))");
        } else if (Float.isNaN(value)) {
            builder.append("SQRT(-1)");
        } else {
            builder.append(value);
        }
        return builder;
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_REAL;
    }

    @Override
    public int getValueType() {
        return REAL;
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        return Float.compare(value, ((ValueReal) o).value);
    }

    @Override
    public int getSignum() {
        return value == 0 ? 0 : (value < 0 ? -1 : 1);
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
    public BigDecimal getBigDecimal() {
        if (Math.abs(value) <= Float.MAX_VALUE) {
            // better rounding behavior than BigDecimal.valueOf(f)
            return new BigDecimal(Float.toString(value));
        }
        // Infinite or NaN
        throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, Float.toString(value));
    }

    @Override
    public String getString() {
        return Float.toString(value);
    }

    @Override
    public int hashCode() {
        /*
         * NaNs are normalized in get() method, so it's safe to use
         * floatToRawIntBits() instead of floatToIntBits() here.
         */
        return Float.floatToRawIntBits(value);
    }

    @Override
    public Object getObject() {
        return value;
    }

    /**
     * Get or create a REAL value for the given float.
     *
     * @param d the float
     * @return the value
     */
    public static ValueReal get(float d) {
        if (d == 1.0F) {
            return ONE;
        } else if (d == 0.0F) {
            // -0.0 == 0.0, and we want to return 0.0 for both
            return ZERO;
        } else if (Float.isNaN(d)) {
            return NAN;
        }
        return (ValueReal) Value.cache(new ValueReal(d));
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueReal)) {
            return false;
        }
        return compareTypeSafe((ValueReal) other, null, null) == 0;
    }

}
