/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.h2.constant.SysProperties;
import org.h2.util.MathUtils;
import org.h2.util.StringUtils;

/**
 * Implementation of the VARCHAR data type.
 * It is also the base class for other ValueString* classes.
 */
public class ValueString extends Value {

    private static final ValueString EMPTY = new ValueString("");

    /**
     * The string data.
     */
    protected final String value;

    protected ValueString(String value) {
        this.value = value;
    }

    public String getSQL() {
        return StringUtils.quoteStringSQL(value);
    }

    public boolean equals(Object other) {
        return other instanceof ValueString && value.equals(((ValueString) other).value);
    }

    protected int compareSecure(Value o, CompareMode mode) {
        // compatibility: the other object could be another type
        ValueString v = (ValueString) o;
        return mode.compareString(value, v.value, false);
    }

    public String getString() {
        return value;
    }

    public long getPrecision() {
        return value.length();
    }

    public Object getObject() {
        return value;
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setString(parameterIndex, value);
    }

    public int getDisplaySize() {
        return value.length();
    }

    public int getMemory() {
        return value.length() * 2 + 30;
    }

    public Value convertPrecision(long precision) {
        if (precision == 0 || value.length() <= precision) {
            return this;
        }
        int p = MathUtils.convertLongToInt(precision);
        return getNew(value.substring(0, p));
    }

    public int hashCode() {
        // TODO hash performance: could build a quicker hash
        // by hashing the size and a few characters
        return value.hashCode();

        // proposed code:
//        private int hash = 0;
//
//        public int hashCode() {
//            int h = hash;
//            if (h == 0) {
//                String s = value;
//                int l = s.length();
//                if (l > 0) {
//                    if (l < 16)
//                        h = s.hashCode();
//                    else {
//                        h = l;
//                        for (int i = 1; i <= l; i <<= 1)
//                            h = 31 *
//                                (31 * h + s.charAt(i - 1)) +
//                                s.charAt(l - i);
//                    }
//                    hash = h;
//                }
//            }
//            return h;
//        }

    }

    public int getType() {
        return Value.STRING;
    }

    /**
     * Get or create a string value for the given string.
     *
     * @param s the string
     * @return the value
     */
    public static ValueString get(String s) {
        if (s.length() == 0) {
            return EMPTY;
        }
        ValueString obj = new ValueString(StringUtils.cache(s));
        if (s.length() > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return (ValueString) Value.cache(obj);
        // this saves memory, but is really slow
        // return new ValueString(s.intern());
    }

    /**
     * Create a new String value of the current class.
     * This method is meant to be overridden by subclasses.
     *
     * @param s the string
     * @return the value
     */
    protected Value getNew(String s) {
        return ValueString.get(s);
    }

}
