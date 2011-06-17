/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.constant.SysProperties;
import org.h2.util.MathUtils;
import org.h2.util.StringCache;

/**
 * Implementation of the VARCHAR data type.
 */
public class ValueString extends ValueStringBase {

    private static final ValueString EMPTY = new ValueString("");

    protected ValueString(String value) {
        super(value);
    }

    public int getType() {
        return Value.STRING;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        // compatibility: the other object could be ValueStringFixed
        ValueStringBase v = (ValueStringBase) o;
        return mode.compareString(value, v.value, false);
    }

    public boolean equals(Object other) {
        return other instanceof ValueStringBase && value.equals(((ValueStringBase) other).value);
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

    public Value convertPrecision(long precision) {
        if (precision == 0 || value.length() <= precision) {
            return this;
        }
        int p = MathUtils.convertLongToInt(precision);
        return ValueString.get(value.substring(0, p));
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
        ValueString obj = new ValueString(StringCache.get(s));
        if (s.length() > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return (ValueString) Value.cache(obj);
        // this saves memory, but is really slow
        // return new ValueString(s.intern());
    }

}
