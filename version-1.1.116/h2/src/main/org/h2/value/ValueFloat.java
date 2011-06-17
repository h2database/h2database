/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.message.Message;

/**
 * Implementation of the REAL data type.
 */
public class ValueFloat extends Value {

    /**
     * The precision in digits.
     */
    static final int PRECISION = 7;

    /**
     * The maximum display size of a float.
     * Example: -1.12345676E-20
     */
    static final int DISPLAY_SIZE = 15;

    private static final float FLOAT_ZERO = 0.0F;
    private static final float FLOAT_ONE = 1.0F;
    private static final ValueFloat ZERO = new ValueFloat(FLOAT_ZERO);
    private static final ValueFloat ONE = new ValueFloat(FLOAT_ONE);

    private final float value;

    private ValueFloat(float value) {
        this.value = value;
    }

    public Value add(Value v) {
        ValueFloat v2 = (ValueFloat) v;
        return ValueFloat.get(value + v2.value);
    }

    public Value subtract(Value v) {
        ValueFloat v2 = (ValueFloat) v;
        return ValueFloat.get(value - v2.value);
    }

    public Value negate() {
        return ValueFloat.get(-value);
    }

    public Value multiply(Value v) {
        ValueFloat v2 = (ValueFloat) v;
        return ValueFloat.get(value * v2.value);
    }

    public Value divide(Value v) throws SQLException {
        ValueFloat v2 = (ValueFloat) v;
        if (v2.value == 0.0) {
            throw Message.getSQLException(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueFloat.get(value / v2.value);
    }

    public String getSQL() {
        if (value == Float.POSITIVE_INFINITY) {
            return "POWER(0, -1)";
        } else if (value == Float.NEGATIVE_INFINITY) {
            return "(-POWER(0, -1))";
        } else if (Double.isNaN(value)) {
            // NaN
            return "SQRT(-1)";
        }
        return getString();
    }

    public int getType() {
        return Value.FLOAT;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueFloat v = (ValueFloat) o;
        return Float.compare(value, v.value);
    }

    public int getSignum() {
        return value == 0 ? 0 : (value < 0 ? -1 : 1);
    }

    public float getFloat() {
        return value;
    }

    public String getString() {
        return String.valueOf(value);
    }

    public long getPrecision() {
        return PRECISION;
    }

    public int getScale() {
        return 0;
    }

    public int hashCode() {
        long hash = Float.floatToIntBits(value);
        return (int) (hash ^ (hash >> 32));
    }

    public Object getObject() {
        return Float.valueOf(value);
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setFloat(parameterIndex, value);
    }

    /**
     * Get or create float value for the given float.
     *
     * @param d the float
     * @return the value
     */
    public static ValueFloat get(float d) {
        if (FLOAT_ZERO == d) {
            return ZERO;
        } else if (FLOAT_ONE == d) {
            return ONE;
        }
        return (ValueFloat) Value.cache(new ValueFloat(d));
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    public boolean equals(Object other) {
        if (!(other instanceof ValueFloat)) {
            return false;
        }
        return compareSecure((ValueFloat) other, null) == 0;
    }

}
