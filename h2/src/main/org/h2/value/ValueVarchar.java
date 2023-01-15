/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.engine.CastDataProvider;
import org.h2.engine.SysProperties;
import org.h2.util.StringUtils;

/**
 * Implementation of the CHARACTER VARYING data type.
 */
public final class ValueVarchar extends ValueStringBase {

    /**
     * Empty string. Should not be used in places where empty string can be
     * treated as {@code NULL} depending on database mode.
     */
    public static final ValueVarchar EMPTY = new ValueVarchar("");

    private ValueVarchar(String value) {
        super(value);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return StringUtils.quoteStringSQL(builder, value);
    }

    @Override
    public int getValueType() {
        return VARCHAR;
    }

    /**
     * Get or create a VARCHAR value for the given string.
     *
     * @param s the string
     * @return the value
     */
    public static Value get(String s) {
        return get(s, null);
    }

    /**
     * Get or create a VARCHAR value for the given string.
     *
     * @param s the string
     * @param provider the cast information provider, or {@code null}
     * @return the value
     */
    public static Value get(String s, CastDataProvider provider) {
        if (s.isEmpty()) {
            return provider != null && provider.getMode().treatEmptyStringsAsNull ? ValueNull.INSTANCE : EMPTY;
        }
        ValueVarchar obj = new ValueVarchar(StringUtils.cache(s));
        if (s.length() > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return Value.cache(obj);
        // this saves memory, but is really slow
        // return new ValueString(s.intern());
    }

}
