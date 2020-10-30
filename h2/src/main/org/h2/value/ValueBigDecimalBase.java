/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.engine.Constants;
import org.h2.message.DbException;

/**
 * Base class for BigDecimal-based values.
 */
abstract class ValueBigDecimalBase extends Value {

    final BigDecimal value;

    TypeInfo type;

    ValueBigDecimalBase(BigDecimal value) {
        if (value == null) {
            throw new IllegalArgumentException("null");
        } else if (value.getClass() != BigDecimal.class) {
            throw DbException.get(ErrorCode.INVALID_CLASS_2, BigDecimal.class.getName(), value.getClass().getName());
        }
        int length = value.precision();
        if (length > Constants.MAX_NUMERIC_PRECISION) {
            throw DbException.getValueTooLongException(getTypeName(getValueType()), value.toString(), length);
        }
        this.value = value;
    }

    @Override
    public final Value add(Value v) {
        return getValue(value.add(((ValueBigDecimalBase) v).value));
    }

    @Override
    public final Value subtract(Value v) {
        return getValue(value.subtract(((ValueBigDecimalBase) v).value));
    }

    @Override
    public final Value negate() {
        return getValue(value.negate());
    }

    @Override
    public final Value multiply(Value v) {
        return getValue(value.multiply(((ValueBigDecimalBase) v).value));
    }

    @Override
    public final Value divide(Value v, long divisorPrecision) {
        BigDecimal divisor = ((ValueBigDecimalBase) v).value;
        if (divisor.signum() == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
        }
        return getValue(value.divide(divisor,
                getQuotientScale(value.scale(), divisorPrecision, divisor.scale()), RoundingMode.HALF_DOWN));
    }

    /**
     * Evaluates the scale of the quotient.
     *
     * @param dividerScale
     *            the scale of the divider
     * @param divisorPrecision
     *            the precision of the divisor
     * @param divisorScale
     *            the scale of the divisor
     * @return the scale of the quotient
     */
    public static int getQuotientScale(int dividerScale, long divisorPrecision, int divisorScale) {
        long scale = dividerScale - divisorScale + divisorPrecision * 2;
        return scale >= ValueNumeric.MAXIMUM_SCALE ? ValueNumeric.MAXIMUM_SCALE : (int) scale;
    }

    @Override
    public final Value modulus(Value v) {
        ValueBigDecimalBase dec = (ValueBigDecimalBase) v;
        if (dec.value.signum() == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
        }
        return getValue(value.remainder(dec.value));
    }

    abstract ValueBigDecimalBase getValue(BigDecimal value);

    @Override
    public final int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        return value.compareTo(((ValueBigDecimalBase) o).value);
    }

    @Override
    public final int getSignum() {
        return value.signum();
    }

    @Override
    public final BigDecimal getBigDecimal() {
        return value;
    }

    @Override
    public final float getFloat() {
        return value.floatValue();
    }

    @Override
    public final double getDouble() {
        return value.doubleValue();
    }

    @Override
    public final int hashCode() {
        return getClass().hashCode() * 31 + value.hashCode();
    }

    @Override
    public final boolean equals(Object other) {
        return getClass().isInstance(other) && value.equals(((ValueBigDecimalBase) other).value);
    }

    @Override
    public final int getMemory() {
        return value.precision() + 120;
    }

}
