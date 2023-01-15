/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.util.Bits;

/**
 * Implementation of the BIGINT data type.
 */
public final class ValueBigint extends Value {

    /**
     * The smallest {@code ValueLong} value.
     */
    public static final ValueBigint MIN = get(Long.MIN_VALUE);

    /**
     * The largest {@code ValueLong} value.
     */
    public static final ValueBigint MAX = get(Long.MAX_VALUE);

    /**
     * The largest Long value, as a BigInteger.
     */
    public static final BigInteger MAX_BI = BigInteger.valueOf(Long.MAX_VALUE);

    /**
     * The precision in bits.
     */
    static final int PRECISION = 64;

    /**
     * The approximate precision in decimal digits.
     */
    public static final int DECIMAL_PRECISION = 19;

    /**
     * The maximum display size of a BIGINT.
     * Example: -9223372036854775808
     */
    public static final int DISPLAY_SIZE = 20;

    private static final int STATIC_SIZE = 100;
    private static final ValueBigint[] STATIC_CACHE;

    private final long value;

    static {
        STATIC_CACHE = new ValueBigint[STATIC_SIZE];
        for (int i = 0; i < STATIC_SIZE; i++) {
            STATIC_CACHE[i] = new ValueBigint(i);
        }
    }

    private ValueBigint(long value) {
        this.value = value;
    }

    @Override
    public Value add(Value v) {
        long x = value;
        long y = ((ValueBigint) v).value;
        long result = x + y;
        /*
         * If signs of both summands are different from the sign of the sum there is an
         * overflow.
         */
        if (((x ^ result) & (y ^ result)) < 0) {
            throw getOverflow();
        }
        return ValueBigint.get(result);
    }

    @Override
    public int getSignum() {
        return Long.signum(value);
    }

    @Override
    public Value negate() {
        if (value == Long.MIN_VALUE) {
            throw getOverflow();
        }
        return ValueBigint.get(-value);
    }

    private DbException getOverflow() {
        return DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1,
                Long.toString(value));
    }

    @Override
    public Value subtract(Value v) {
        long x = value;
        long y = ((ValueBigint) v).value;
        long result = x - y;
        /*
         * If minuend and subtrahend have different signs and minuend and difference
         * have different signs there is an overflow.
         */
        if (((x ^ y) & (x ^ result)) < 0) {
            throw getOverflow();
        }
        return ValueBigint.get(result);
    }

    @Override
    public Value multiply(Value v) {
        long x = value;
        long y = ((ValueBigint) v).value;
        long result = x * y;
        // Check whether numbers are large enough to overflow and second value != 0
        if ((Math.abs(x) | Math.abs(y)) >>> 31 != 0 && y != 0
                // Check with division
                && (result / y != x
                // Also check the special condition that is not handled above
                || x == Long.MIN_VALUE && y == -1)) {
            throw getOverflow();
        }
        return ValueBigint.get(result);
    }

    @Override
    public Value divide(Value v, TypeInfo quotientType) {
        long y = ((ValueBigint) v).value;
        if (y == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
        }
        long x = value;
        if (x == Long.MIN_VALUE && y == -1) {
            throw getOverflow();
        }
        return ValueBigint.get(x / y);
    }

    @Override
    public Value modulus(Value v) {
        ValueBigint other = (ValueBigint) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
        }
        return ValueBigint.get(this.value % other.value);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        if ((sqlFlags & NO_CASTS) == 0 && value == (int) value) {
            return builder.append("CAST(").append(value).append(" AS BIGINT)");
        }
        return builder.append(value);
    }

    @Override
    public TypeInfo getType() {
        return TypeInfo.TYPE_BIGINT;
    }

    @Override
    public int getValueType() {
        return BIGINT;
    }

    @Override
    public byte[] getBytes() {
        byte[] b = new byte[8];
        Bits.writeLong(b, 0, getLong());
        return b;
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
        return Long.compare(value, ((ValueBigint) o).value);
    }

    @Override
    public String getString() {
        return Long.toString(value);
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >> 32));
    }

    /**
     * Get or create a BIGINT value for the given long.
     *
     * @param i the long
     * @return the value
     */
    public static ValueBigint get(long i) {
        if (i >= 0 && i < STATIC_SIZE) {
            return STATIC_CACHE[(int) i];
        }
        return (ValueBigint) Value.cache(new ValueBigint(i));
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueBigint && value == ((ValueBigint) other).value;
    }

}
