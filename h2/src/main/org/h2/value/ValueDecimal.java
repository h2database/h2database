/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.Message;
import org.h2.util.MathUtils;

/**
 * Implementation of the DECIMAL data type.
 */
public class ValueDecimal extends Value {
    
    static final int DEFAULT_PRECISION = 65535;
    static final int DEFAULT_SCALE = 32767;
    static final int DEFAULT_DISPLAY_SIZE = 65535;
    private static final int DIVIDE_SCALE_ADD = 25;

    private static final BigDecimal DEC_ZERO = new BigDecimal("0");
    private static final BigDecimal DEC_ONE = new BigDecimal("1");
    private static final ValueDecimal ZERO = new ValueDecimal(DEC_ZERO);
    private static final ValueDecimal ONE = new ValueDecimal(DEC_ONE);

    // TODO doc: document differences for BigDecimal 1.5 <> 1.4
    private final BigDecimal value;
    private String valueString;
    private int precision;

    private ValueDecimal(BigDecimal value) {
        if (value == null) {
            throw new IllegalArgumentException();
        } else if (!SysProperties.ALLOW_BIG_DECIMAL_EXTENSIONS && !value.getClass().equals(BigDecimal.class)) {
            SQLException e = Message.getSQLException(ErrorCode.INVALID_CLASS_2, new String[] {
                    BigDecimal.class.getName(), value.getClass().getName() });
            throw Message.convertToInternal(e);
        }
        this.value = value;
    }

    public Value add(Value v) {
        ValueDecimal dec = (ValueDecimal) v;
        return ValueDecimal.get(value.add(dec.value));
    }

    public Value subtract(Value v) {
        ValueDecimal dec = (ValueDecimal) v;
        return ValueDecimal.get(value.subtract(dec.value));
    }

    public Value negate() {
        return ValueDecimal.get(value.negate());
    }

    public Value multiply(Value v) {
        ValueDecimal dec = (ValueDecimal) v;
        return ValueDecimal.get(value.multiply(dec.value));
    }

    public Value divide(Value v) throws SQLException {
        ValueDecimal dec = (ValueDecimal) v;
        // TODO value: divide decimal: rounding?
        if (dec.value.signum() == 0) {
            throw Message.getSQLException(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        BigDecimal bd = value.divide(dec.value, value.scale() + DIVIDE_SCALE_ADD, BigDecimal.ROUND_HALF_DOWN);
        if (bd.signum() == 0) {
            bd = DEC_ZERO;
        } else if (bd.scale() > 0) {
            if (!bd.unscaledValue().testBit(0)) {
                String s = bd.toString();
                int i = s.length() - 1;
                while (i >= 0 && s.charAt(i) == '0') {
                    i--;
                }
                if (i < s.length() - 1) {
                    s = s.substring(0, i + 1);
                    bd = new BigDecimal(s);
                }
            }
        }
        return ValueDecimal.get(bd);
    }

    public String getSQL() {
        return getString();
    }

    public int getType() {
        return Value.DECIMAL;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueDecimal v = (ValueDecimal) o;
        int c = value.compareTo(v.value);
        return c == 0 ? 0 : (c < 0 ? -1 : 1);
    }

    public int getSignum() {
        return value.signum();
    }

    public BigDecimal getBigDecimal() {
        return value;
    }

    public String getString() {
        if (valueString == null) {
            valueString = value.toString();
        }
        return valueString;
    }

    public long getPrecision() {
        if (precision == 0) {
            precision = value.unscaledValue().abs().toString().length();
        }
        return precision;
    }

    public boolean checkPrecision(long precision) {
        if (precision == DEFAULT_PRECISION) {
            return true;
        }
        return getPrecision() <= precision;
    }

    public int getScale() {
        return value.scale();
    }

    public int hashCode() {
        return value.hashCode();
    }

    public Object getObject() {
        return value;
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setBigDecimal(parameterIndex, value);
    }

    public Value convertScale(boolean onlyToSmallerScale, int targetScale) throws SQLException {
        if (value.scale() == targetScale) {
            return this;
        }
        if (onlyToSmallerScale || targetScale >= DEFAULT_SCALE) {
            if (value.scale() < targetScale) {
                return this;
            }
        }
        BigDecimal bd = MathUtils.setScale(value, targetScale);
        return ValueDecimal.get(bd);
    }

    public Value convertPrecision(long precision) throws SQLException {
        if (getPrecision() <= precision) {
            return this;
        }
        throw Message.getSQLException(ErrorCode.VALUE_TOO_LARGE_FOR_PRECISION_1, "" + precision);
    }

    /**
     * Get or create big decimal value for the given big decimal.
     * 
     * @param dec the bit decimal
     * @return the value
     */
    public static ValueDecimal get(BigDecimal dec) {
        if (DEC_ZERO.equals(dec)) {
            return ZERO;
        } else if (DEC_ONE.equals(dec)) {
            return ONE;
        }
        // TODO value optimization: find a way to read size of BigDecimal,
        // check max cache size
        return (ValueDecimal) Value.cache(new ValueDecimal(dec));
    }

    public int getDisplaySize() {
        // add 2 characters for '-' and '.'
        return MathUtils.convertLongToInt(getPrecision() + 2);
    }

    public boolean equals(Object other) {
        return other instanceof ValueDecimal && value.equals(((ValueDecimal) other).value);
    }

    public int getMemory() {
        return getString().length() * 3 + 120;
    }

}
