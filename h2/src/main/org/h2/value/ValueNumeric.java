/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import org.h2.message.DbException;
import org.h2.util.MathUtils;

/**
 * Implementation of the NUMERIC data type.
 */
public final class ValueNumeric extends ValueBigDecimalBase {

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
    public static final int DEFAULT_PRECISION = 65535;

    /**
     * The default scale for a NUMERIC value.
     */
    public static final int DEFAULT_SCALE = 0;

    /**
     * The maximum scale.
     */
    public static final int MAXIMUM_SCALE = 100_000;

    /**
     * The minimum scale.
     */
    public static final int MINIMUM_SCALE = -100_000;

    private ValueNumeric(BigDecimal value) {
        super(value);
    }

    @Override
    ValueBigDecimalBase getValue(BigDecimal value) {
        return get(value);
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
