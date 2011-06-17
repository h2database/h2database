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
 * Implementation of the CHAR data type.
 */
public class ValueStringFixed extends ValueStringBase {

    private static final ValueStringFixed EMPTY = new ValueStringFixed("");

    protected ValueStringFixed(String value) {
        super(value);
    }

    protected int compareSecure(Value o, CompareMode mode) {
        // compatibility: the other object could be ValueString
        ValueStringBase v = (ValueStringBase) o;
        return mode.compareString(value, v.value, false);
    }

    private static String trimRight(String s) {
        int endIndex = s.length() - 1;
        int i = endIndex;
        while (i >= 0 && s.charAt(i) == ' ') {
            i--;
        }
        s = i == endIndex ? s : s.substring(0, i + 1);
        return s;
    }

    public boolean equals(Object other) {
        return other instanceof ValueStringBase && value.equals(((ValueStringBase) other).value);
    }

    public int hashCode() {
        // TODO hash performance: could build a quicker hash
        // by hashing the size and a few characters
        return value.hashCode();
    }

    public int getType() {
        return Value.STRING_FIXED;
    }

    /**
     * Get or create a fixed length string value for the given string.
     * Spaces at the end of the string will be removed.
     *
     * @param s the string
     * @return the value
     */
    public static ValueStringFixed get(String s) {
        s = trimRight(s);
        if (s.length() == 0) {
            return EMPTY;
        }
        ValueStringFixed obj = new ValueStringFixed(StringCache.get(s));
        if (s.length() > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return (ValueStringFixed) Value.cache(obj);
    }

    public Value convertPrecision(long precision) {
        if (precision == 0 || value.length() <= precision) {
            return this;
        }
        int p = MathUtils.convertLongToInt(precision);
        return ValueStringFixed.get(value.substring(0, p));
    }

}
