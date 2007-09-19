/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.h2.constant.ErrorCode;
import org.h2.message.Message;
import org.h2.util.DateTimeUtils;
import org.h2.util.MathUtils;

public class ValueTimestamp extends Value {
    public static final int PRECISION = 23;
    public static final int DEFAULT_SCALE = 10;
    private final Timestamp value;

    private ValueTimestamp(Timestamp value) {
        this.value = value;
    }

    public Timestamp getTimestamp() {
        return (Timestamp) value.clone();
    }
    
    public Timestamp getTimestampNoCopy() {
        return value;
    }    

    public String getSQL() {
        return "TIMESTAMP '" + getString() + "'";
    }

    public static Timestamp parseTimestamp(String s) throws SQLException {
        return (Timestamp) DateTimeUtils.parseDateTime(s, Value.TIMESTAMP, ErrorCode.TIMESTAMP_CONSTANT_2);
    }

    public int getType() {
        return Value.TIMESTAMP;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueTimestamp v = (ValueTimestamp) o;
        int c = value.compareTo(v.value);
        return c == 0 ? 0 : (c < 0 ? -1 : 1);
    }

    public String getString() {
        return value.toString();
    }

    public long getPrecision() {
        return PRECISION;
    }

    public int getScale() {
        return DEFAULT_SCALE;
    }

    public int hashCode() {
        return value.hashCode();
    }

    public Object getObject() {
        // this class is mutable - must copy the object
        return getTimestamp();
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setTimestamp(parameterIndex, value);
    }

    public static ValueTimestamp get(Timestamp timestamp) {
        timestamp = (Timestamp) timestamp.clone();
        return getNoCopy(timestamp);
    }

    public static ValueTimestamp getNoCopy(Timestamp timestamp) {
        return (ValueTimestamp) Value.cache(new ValueTimestamp(timestamp));
    }

    public Value convertScale(boolean onlyToSmallerScale, int targetScale) throws SQLException {
        if (targetScale < 0 || targetScale > DEFAULT_SCALE) {
            // TODO convertScale for Timestamps: may throw an exception?
            throw Message.getInvalidValueException(""+targetScale, "scale");
        }
        int nanos = value.getNanos();
        BigDecimal bd = new BigDecimal("" + nanos);
        bd = bd.movePointLeft(9);
        bd = MathUtils.setScale(bd, targetScale);
        bd = bd.movePointRight(9);
        int n2 = bd.intValue();
        if (n2 == nanos) {
            return this;
        }
        long t = value.getTime();
        while (n2 >= 1000000000) {
            t += 1000;
            n2 -= 1000000000;
        }
        Timestamp t2 = new Timestamp(t);
        t2.setNanos(n2);
        return ValueTimestamp.getNoCopy(t2);
    }

//    public String getJavaString() {
//        return "Timestamp.valueOf(\"" + toString() + "\")";
//    }
    
    public int getDisplaySize() {
        return "2001-01-01 23:59:59.000".length();
    }    
    
    protected boolean isEqual(Value v) {
        return v instanceof ValueTimestamp && value.equals(((ValueTimestamp) v).value);
    }    

}
