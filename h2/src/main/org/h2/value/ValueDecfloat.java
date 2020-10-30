/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;

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

    private ValueDecfloat(BigDecimal value) {
        super(value);
    }

    @Override
    ValueBigDecimalBase getValue(BigDecimal value) {
        return ValueDecfloat.get(value);
    }

    @Override
    public String getString() {
        return value.toString();
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        String s = getString();
        if ((sqlFlags & NO_CASTS) == 0 && s.indexOf('E') < 0) {
            return builder.append("CAST(").append(s).append(" AS DECFLOAT)");
        }
        return builder.append(s);
    }

    @Override
    public TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            this.type = type = new TypeInfo(DECFLOAT, value.precision(), 0, null);
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
