/*
 * Copyright 2004-2024 H2 Group. 
 * Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.api.ErrorCode;
import org.h2.engine.CastDataProvider;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.util.StringUtils;

import java.util.regex.Pattern;

/**
 * Implementation of the EMAIL data type.
 */
public final class ValueEmail extends ValueStringBase {

    // Empty email instance
    public static final ValueEmail EMPTY = new ValueEmail("");

    // Regex pattern for validating email addresses
    private static final Pattern EMAIL_PATTERN = Pattern.compile(
        "^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,6}$",
        Pattern.CASE_INSENSITIVE
    );

    private ValueEmail(String value) {
        super(value);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return StringUtils.quoteStringSQL(builder, value);
    }

    @Override
    public int getValueType() {
        return EMAIL;
    }

    /**
     * Get or create an EMAIL value for the given string.
     *
     * @param s the email string
     * @return the EMAIL value
     */
    public static Value get(String s) {
        return get(s, null);
    }

    /**
     * Get or create an EMAIL value for the given string with a CastDataProvider.
     *
     * @param s        the email string
     * @param provider the cast information provider, or {@code null}
     * @return the EMAIL value
     */
    public static Value get(String s, CastDataProvider provider) {
        if (s.isEmpty()) {
            return provider != null && provider.getMode().treatEmptyStringsAsNull ? ValueNull.INSTANCE : EMPTY;
        }
        // Validate email format
        if (!EMAIL_PATTERN.matcher(s).matches()) {
            throw DbException.get(ErrorCode.INVALID_VALUE_2, "EMAIL", s);
            // throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, s);
        }
        // Normalize email to lowercase for consistency
        String normalized = s.toLowerCase();
        ValueEmail obj = new ValueEmail(StringUtils.cache(normalized));
        if (s.length() > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return Value.cache(obj);
    }

    // @Override
    // public String getString() {
    //     return value;
    // }

    // @Override
    // public Object getObject() {
    //     return value;
    // }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueEmail)) {
            return false;
        }
        ValueEmail o = (ValueEmail) other;
        return value.equals(o.value);
    }

    // @Override
    // public int compareSecure(Value o, CompareMode mode) {
    //     if (!(o instanceof ValueEmail)) {
    //         throw DbException.get(ErrorCode.INVALID_CLASS_2, "EMAIL", o.getClass().getName());
    //     }
    //     return value.compareTo(((ValueEmail) o).value);
    // }

    // Implement other required methods if necessary
}