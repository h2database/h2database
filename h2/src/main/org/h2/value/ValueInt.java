/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.message.Message;

/**
 * @author Thomas
 */
public class ValueInt extends Value {
    public static final int PRECISION = 10;

    private int value;
    private static final int STATIC_SIZE = 100;
    private static final int DYNAMIC_SIZE = 256; // must be a power of 2
    // TODO check performance of final static!
    private static ValueInt[] staticCache;
    private static ValueInt[] dynamicCache;

    static {
        staticCache = new ValueInt[STATIC_SIZE];
        dynamicCache = new ValueInt[DYNAMIC_SIZE];
        for (int i = 0; i < STATIC_SIZE; i++) {
            staticCache[i] = new ValueInt(i);
        }
    }

    public static ValueInt get(int i) {
        if (i >= 0 && i < STATIC_SIZE) {
            return staticCache[i];
        }
        ValueInt v = dynamicCache[i & DYNAMIC_SIZE - 1];
        if (v == null || v.value != i) {
            v = new ValueInt(i);
            dynamicCache[i & DYNAMIC_SIZE - 1] = v;
        }
        return v;
    }

    private ValueInt(int value) {
        this.value = value;
    }

    public Value add(Value v) throws SQLException {
        ValueInt other = (ValueInt) v;
        if(Constants.OVERFLOW_EXCEPTIONS) {
            return checkRange((long)value + (long)other.value);
        }
        return ValueInt.get(value + other.value);
    }
    
    private ValueInt checkRange(long value) throws SQLException {
        if(value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw Message.getSQLException(Message.OVERFLOW_FOR_TYPE_1, DataType.getDataType(Value.INT).name);
        } else {
            return ValueInt.get((int)value);
        }
    }

    public int getSignum() {
        return value == 0 ? 0 : (value < 0 ? -1 : 1);
    }

    public Value negate() throws SQLException {
        if(Constants.OVERFLOW_EXCEPTIONS) {
            return checkRange(-(long)value);
        }
        return ValueInt.get(-value);
    }

    public Value subtract(Value v) throws SQLException {
        ValueInt other = (ValueInt) v;
        if(Constants.OVERFLOW_EXCEPTIONS) {
            return checkRange((long)value - (long)other.value);
        }
        return ValueInt.get(value - other.value);
    }

    public Value multiply(Value v) throws SQLException {
        ValueInt other = (ValueInt) v;
        if(Constants.OVERFLOW_EXCEPTIONS) {
            return checkRange((long)value * (long)other.value);
        }        
        return ValueInt.get(value * other.value);
    }

    public Value divide(Value v) throws SQLException {
        ValueInt other = (ValueInt) v;
        if (other.value == 0) {
            throw Message.getSQLException(Message.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueInt.get(value / other.value);
    }

    public String getSQL() {
        return getString();
    }

    public int getType() {
        return Value.INT;
    }

    public int getInt() {
        return value;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueInt v = (ValueInt) o;
        if (value == v.value) {
            return 0;
        }
        return value > v.value ? 1 : -1;
    }

    public String getString() {
        return String.valueOf(value);
    }

    public long getPrecision() {
        return PRECISION;
    }

    public int hashCode() {
        return value;
    }

    public Object getObject() {
        return new Integer(value);
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setInt(parameterIndex, value);
    }

//    public String getJavaString() {
//        return getString();
//    }

    public int getDisplaySize() {
        return PRECISION;
    }    
    
    protected boolean isEqual(Value v) {
        return v instanceof ValueInt && value == ((ValueInt)v).value;
    }    

}
