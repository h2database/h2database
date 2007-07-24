/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.util.ObjectUtils;

/**
 * @author Thomas
 */

public class ValueDouble extends Value {
    public static final int PRECISION = 17;

    private final double value;
    private static final double DOUBLE_ZERO = 0.0;
    private static final double DOUBLE_ONE = 1.0;
    private static final ValueDouble ZERO = new ValueDouble(DOUBLE_ZERO);
    private static final ValueDouble ONE = new ValueDouble(DOUBLE_ONE);

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
            throw Message.getSQLException(Message.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueDouble.get(value / v2.value);
    }

    public String getSQL() {
        return getString();
    }

    public int getType() {
        return Value.DOUBLE;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueDouble v = (ValueDouble) o;
        if (value == v.value) {
            return 0;
        }
        return value > v.value ? 1 : -1;
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
        // TODO value: what is the scale of a double?
        return 0;
    }

    public int hashCode() {
        long hash = Double.doubleToLongBits(value);
        return (int) (hash ^ (hash >> 32));
    }

    public Object getObject() {
        return ObjectUtils.getDouble(value);
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setDouble(parameterIndex, value);
    }

    public static ValueDouble get(double d) {
        if (DOUBLE_ZERO == d) {
            return ZERO;
        } else if (DOUBLE_ONE == d) {
            return ONE;
        }
        return (ValueDouble) Value.cache(new ValueDouble(d));
    }

//    public String getJavaString() {
//        return getString();
//    }

    public int getDisplaySize() {
        return PRECISION + 2;
    }    
    
    protected boolean isEqual(Value v) {
        return v instanceof ValueDouble && value == ((ValueDouble)v).value;
    } 

}
