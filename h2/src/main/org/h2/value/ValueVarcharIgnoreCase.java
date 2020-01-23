/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.engine.CastDataProvider;
import org.h2.engine.SysProperties;
import org.h2.util.StringUtils;

/**
 * Implementation of the VARCHAR_IGNORECASE data type.
 */
public class ValueVarcharIgnoreCase extends ValueVarchar {

    private static final ValueVarcharIgnoreCase EMPTY =
            new ValueVarcharIgnoreCase("");
    private int hash;

    protected ValueVarcharIgnoreCase(String value) {
        super(value);
    }

    @Override
    public int getValueType() {
        return VARCHAR_IGNORECASE;
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        return mode.compareString(value, ((ValueVarcharIgnoreCase) o).value, true);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueVarchar
                && value.equalsIgnoreCase(((ValueVarchar) other).value);
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            // this is locale sensitive
            hash = value.toUpperCase().hashCode();
        }
        return hash;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        builder.append("CAST(");
        return StringUtils.quoteStringSQL(builder, value).append(" AS VARCHAR_IGNORECASE)");
    }

    /**
     * Get or create a VARCHAR_IGNORECASE value for the given string.
     * The value will have the same case as the passed string.
     *
     * @param s the string
     * @return the value
     */
    public static ValueVarcharIgnoreCase get(String s) {
        int length = s.length();
        if (length == 0) {
            return EMPTY;
        }
        ValueVarcharIgnoreCase obj = new ValueVarcharIgnoreCase(StringUtils.cache(s));
        if (length > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        ValueVarcharIgnoreCase cache = (ValueVarcharIgnoreCase) Value.cache(obj);
        // the cached object could have the wrong case
        // (it would still be 'equal', but we don't like to store it)
        if (cache.value.equals(s)) {
            return cache;
        }
        return obj;
    }

}
