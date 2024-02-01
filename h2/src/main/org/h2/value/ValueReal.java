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
 * Implementation of the REAL data type.
 */
public final class ValueReal extends Value {

    /**
     * The precision in bits.
     */
    static final int PRECISION = 24;

    /**
     * The approximate precision in decimal digits.
     */
    static final int DECIMAL_PRECISION = 7;

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
        return get(value + ((ValueReal) v).value);
    }

    @Override
    public Value subtract(Value v) {
        return get(value - ((ValueReal) v).value);
    }

    @Override
    public Value negate() {
        return get(-value);
    }

    @Override
    public Value multiply(Value v) {
        return get(value * ((ValueReal) v).value);
    }

    @Override
    public Value divide(Value v, TypeInfo quotientType) {
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
            return builder.append("'Infinity'");
        } else if (value == Float.NEGATIVE_INFINITY) {
            return builder.append("'-Infinity'");
        } else if (Float.isNaN(value)) {
            return builder.append("'NaN'");
        } else {
            return builder.append(value);
        }
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
        return value == 0 || Float.isNaN(value) ? 0 : value < 0 ? -1 : 1;
    }

    @Override
    public BigDecimal getBigDecimal() {
        if (Float.isFinite(value)) {
            // better rounding behavior than BigDecimal.valueOf(f)
            return new BigDecimal(Float.toString(value));
        }
        // Infinite or NaN
        throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, Float.toString(value));
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
