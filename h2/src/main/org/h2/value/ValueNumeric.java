/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.util.MathUtils;

/**
 * Implementation of the NUMERIC data type.
 */
public final class ValueNumeric extends Value {

    /**
     * The value 'zero'.
     */
    public static final ValueNumeric ZERO = new ValueNumeric(BigDecimal.ZERO);

    /**
     * The value 'one'.
     */
    public static final ValueNumeric ONE = new ValueNumeric(BigDecimal.ONE);

    /**
     * The default precision for a NUMERIC value.
     */
    static final int DEFAULT_PRECISION = 65535;

    /**
     * The default scale for a NUMERIC value.
     */
    static final int DEFAULT_SCALE = 0;

    /**
     * The default display size for a NUMERIC value.
     */
    static final int DEFAULT_DISPLAY_SIZE = 65535;

    /**
     * The maximum scale.
     */
    public static final int MAXIMUM_SCALE = 100_000;

    /**
     * The minimum scale.
     */
    public static final int MINIMUM_SCALE = -100_000;

    private final BigDecimal value;
    private TypeInfo type;

    private ValueNumeric(BigDecimal value) {
        if (value == null) {
            throw new IllegalArgumentException("null");
        } else if (value.getClass() != BigDecimal.class) {
            throw DbException.get(ErrorCode.INVALID_CLASS_2,
                    BigDecimal.class.getName(), value.getClass().getName());
        }
        this.value = value;
    }

    @Override
    public Value add(Value v) {
        ValueNumeric dec = (ValueNumeric) v;
        return ValueNumeric.get(value.add(dec.value));
    }

    @Override
    public Value subtract(Value v) {
        ValueNumeric dec = (ValueNumeric) v;
        return ValueNumeric.get(value.subtract(dec.value));
    }

    @Override
    public Value negate() {
        return ValueNumeric.get(value.negate());
    }

    @Override
    public Value multiply(Value v) {
        ValueNumeric dec = (ValueNumeric) v;
        return ValueNumeric.get(value.multiply(dec.value));
    }

    @Override
    public Value divide(Value v, long divisorPrecision) {
        BigDecimal divisor = ((ValueNumeric) v).value;
        if (divisor.signum() == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
        }
        return ValueNumeric.get(value.divide(divisor,
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
        return scale >= MAXIMUM_SCALE ? MAXIMUM_SCALE : (int) scale;
    }

    @Override
    public ValueNumeric modulus(Value v) {
        ValueNumeric dec = (ValueNumeric) v;
        if (dec.value.signum() == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getTraceSQL());
        }
        BigDecimal bd = value.remainder(dec.value);
        return ValueNumeric.get(bd);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        String s = getString();
        if ((sqlFlags & NO_CASTS) == 0 && value.scale() == 0 && value.compareTo(MAX_LONG_DECIMAL) <= 0
                && value.compareTo(MIN_LONG_DECIMAL) >= 0) {
            return builder.append("CAST(").append(value).append(" AS NUMERIC(").append(value.precision()).append("))");
        }
        return builder.append(s);
    }

    @Override
    public TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            long precision = value.precision();
            this.type = type = new TypeInfo(NUMERIC, precision, value.scale(),
                    // add 2 characters for '-' and '.'
                    MathUtils.convertLongToInt(precision + 2), null);
        }
        return type;
    }

    @Override
    public int getValueType() {
        return NUMERIC;
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        return value.compareTo(((ValueNumeric) o).value);
    }

    @Override
    public int getSignum() {
        return value.signum();
    }

    @Override
    public BigDecimal getBigDecimal() {
        return value;
    }

    @Override
    public String getString() {
        return value.toString();
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public Object getObject() {
        return value;
    }

    /**
     * Get or create a NUMERIC value for the given big decimal.
     *
     * @param dec the big decimal
     * @return the value
     */
    public static ValueNumeric get(BigDecimal dec) {
        if (BigDecimal.ZERO.equals(dec)) {
            return ZERO;
        } else if (BigDecimal.ONE.equals(dec)) {
            return ONE;
        }
        return (ValueNumeric) Value.cache(new ValueNumeric(dec));
    }

    /**
     * Get or create a NUMERIC value for the given big integer.
     *
     * @param bigInteger the big integer
     * @return the value
     */
    public static ValueNumeric get(BigInteger bigInteger) {
        if (bigInteger.signum() == 0) {
            return ZERO;
        } else if (BigInteger.ONE.equals(bigInteger)) {
            return ONE;
        }
        return (ValueNumeric) Value.cache(new ValueNumeric(new BigDecimal(bigInteger)));
    }

    @Override
    public boolean equals(Object other) {
        // Two BigDecimal objects are considered equal only if they are equal in
        // value and scale (thus 2.0 is not equal to 2.00 when using equals;
        // however -0.0 and 0.0 are). Can not use compareTo because 2.0 and 2.00
        // have different hash codes
        return other instanceof ValueNumeric &&
                value.equals(((ValueNumeric) other).value);
    }

    @Override
    public int getMemory() {
        return value.precision() + 120;
    }

    /**
     * Set the scale of a BigDecimal value.
     *
     * @param bd the BigDecimal value
     * @param scale the new scale
     * @return the scaled value
     */
    public static BigDecimal setScale(BigDecimal bd, int scale) {
        if (scale > MAXIMUM_SCALE || scale < MINIMUM_SCALE) {
            throw DbException.getInvalidValueException("scale", scale);
        }
        return bd.setScale(scale, RoundingMode.HALF_UP);
    }

}
