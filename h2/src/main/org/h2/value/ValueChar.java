/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.engine.SysProperties;
import org.h2.util.StringUtils;

/**
 * Implementation of the CHAR data type.
 */
public final class ValueChar extends ValueStringBase {

    private static final ValueChar EMPTY = new ValueChar("");

    private ValueChar(String value) {
        super(value);
    }

    private static String trimRight(String s) {
        return trimRight(s, 0);
    }

    private static String trimRight(String s, int minLength) {
        int endIndex = s.length() - 1;
        int i = endIndex;
        while (i >= minLength && s.charAt(i) == ' ') {
            i--;
        }
        s = i == endIndex ? s : s.substring(0, i + 1);
        return s;
    }

    @Override
    public int getValueType() {
        return CHAR;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        if ((sqlFlags & NO_CASTS) == 0) {
            int length = value.length();
            return StringUtils.quoteStringSQL(builder.append("CAST("), value).append(" AS CHAR(")
                    .append(length > 0 ? length : 1).append("))");
        }
        return StringUtils.quoteStringSQL(builder, value);
    }

    /**
     * Get or create a CHAR value for the given string.
     * Spaces at the end of the string will be removed.
     *
     * @param s the string
     * @return the value
     */
    public static ValueChar get(String s) {
        s = trimRight(s);
        int length = s.length();
        if (length == 0) {
            return EMPTY;
        }
        ValueChar obj = new ValueChar(StringUtils.cache(s));
        if (length > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return (ValueChar) Value.cache(obj);
    }

}
