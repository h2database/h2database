/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;

import org.h2.util.MathUtils;

/**
 * Implementation of the DECFLOAT data type.
 */
public final class ValueDecfloat extends ValueBigDecimalBase {

    /**
     * The value 'zero'.
     */
    public static final ValueDecfloat ZERO = new ValueDecfloat(BigDecimal.ZERO);

    /**
     * The value 'one'.
     */
    public static final ValueDecfloat ONE = new ValueDecfloat(BigDecimal.ONE);

    /**
     * The default precision for a DECFLOAT value.
     */
    public static final int DEFAULT_PRECISION = 65535;

    private ValueDecfloat(BigDecimal value) {
        super(value);
    }

    @Override
    ValueBigDecimalBase getValue(BigDecimal value) {
        return ValueDecfloat.get(value);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return builder.append("CAST(").append(value).append(" AS DECFLOAT)");
    }

    @Override
    public TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            long precision = value.precision();
            this.type = type = new TypeInfo(DECFLOAT, precision, 0,
                    // -1.1E+100000
                    MathUtils.convertLongToInt(precision + 12), null);
        }
        return type;
    }

    @Override
    public int getValueType() {
        return DECFLOAT;
    }

    /**
     * Get or create a DECFLOAT value for the given big decimal.
     *
     * @param dec the big decimal
     * @return the value
     */
    public static ValueDecfloat get(BigDecimal dec) {
        dec = dec.stripTrailingZeros();
        if (BigDecimal.ZERO.equals(dec)) {
            return ZERO;
        } else if (BigDecimal.ONE.equals(dec)) {
            return ONE;
        }
        return (ValueDecfloat) Value.cache(new ValueDecfloat(dec));
    }

}
