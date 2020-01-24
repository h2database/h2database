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
 * Implementation of the VARCHAR data type.
 * It is also the base class for ValueVarcharIgnoreCase and ValueChar classes.
 */
public class ValueVarchar extends Value {

    /**
     * Empty string. Should not be used in places where empty string can be
     * treated as {@code NULL} depending on database mode.
     */
    public static final ValueVarchar EMPTY = new ValueVarchar("");

    /**
     * The string data.
     */
    protected final String value;

    private TypeInfo type;

    protected ValueVarchar(String value) {
        this.value = value;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return StringUtils.quoteStringSQL(builder, value);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueVarchar
                && value.equals(((ValueVarchar) other).value);
    }

    @Override
    public int compareTypeSafe(Value o, CompareMode mode, CastDataProvider provider) {
        return mode.compareString(value, ((ValueVarchar) o).value, false);
    }

    @Override
    public String getString() {
        return value;
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public int getMemory() {
        /*
         * Java 11 with -XX:-UseCompressedOops
         * Empty string: 88 bytes
         * 1 to 4 UTF-16 chars: 96 bytes
         */
        return value.length() * 2 + 94;
    }

    @Override
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

    @Override
    public final TypeInfo getType() {
        TypeInfo type = this.type;
        if (type == null) {
            int length = value.length();
            this.type = type = new TypeInfo(getValueType(), length, 0, length, null);
        }
        return type;
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
