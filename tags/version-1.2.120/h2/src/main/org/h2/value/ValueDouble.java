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
 * Implementation of the DOUBLE data type.
 */
public class ValueDouble extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 17;

    /**
     * The maximum display size of a double.
     * Example: -3.3333333333333334E-100
     */
    public static final int DISPLAY_SIZE = 24;

    private static final double DOUBLE_ZERO = 0.0;
    private static final double DOUBLE_ONE = 1.0;
    private static final ValueDouble ZERO = new ValueDouble(DOUBLE_ZERO);
    private static final ValueDouble ONE = new ValueDouble(DOUBLE_ONE);
    private static final ValueDouble NAN = new ValueDouble(Double.NaN);

    private final double value;

    private ValueDouble(double value) {
        this.value = value;
    }

    public Value add(Value v) {
        ValueDouble v2 = (ValueDouble) v;
        return ValueDouble.get(value + v2.value);
    }

    public Value subtract(Value v) {
        ValueDouble v2 = (ValueDouble) v;
        return ValueDouble.get(value - v2.value);
    }

    public Value negate() {
        return ValueDouble.get(-value);
    }

    public Value multiply(Value v) {
        ValueDouble v2 = (ValueDouble) v;
        return ValueDouble.get(value * v2.value);
    }

    public Value divide(Value v) throws SQLException {
        ValueDouble v2 = (ValueDouble) v;
        if (v2.value == 0.0) {
            throw Message.getSQLException(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueDouble.get(value / v2.value);
    }

    public String getSQL() {
        if (value == Double.POSITIVE_INFINITY) {
            return "POWER(0, -1)";
        } else if (value == Double.NEGATIVE_INFINITY) {
            return "(-POWER(0, -1))";
        } else if (Double.isNaN(value)) {
            return "SQRT(-1)";
        }
        return getString();
    }

    public int getType() {
        return Value.DOUBLE;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueDouble v = (ValueDouble) o;
        return Double.compare(value, v.value);
    }

    public int getSignum() {
        return value == 0 ? 0 : (value < 0 ? -1 : 1);
    }

    public double getDouble() {
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
        long hash = Double.doubleToLongBits(value);
        return (int) (hash ^ (hash >> 32));
    }

    public Object getObject() {
        return Double.valueOf(value);
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setDouble(parameterIndex, value);
    }

    /**
     * Get or create double value for the given double.
     *
     * @param d the double
     * @return the value
     */
    public static ValueDouble get(double d) {
        if (DOUBLE_ZERO == d) {
            return ZERO;
        } else if (DOUBLE_ONE == d) {
            return ONE;
        } else if (Double.isNaN(d)) {
            return NAN;
        }
        return (ValueDouble) Value.cache(new ValueDouble(d));
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    public boolean equals(Object other) {
        if (!(other instanceof ValueDouble)) {
            return false;
        }
        return compareSecure((ValueDouble) other, null) == 0;
    }

}
