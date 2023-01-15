/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.util.StringUtils;

/**
 * ENUM value.
 */
public final class ValueEnum extends ValueEnumBase {

    private final ExtTypeInfoEnum enumerators;

    ValueEnum(ExtTypeInfoEnum enumerators, String label, int ordinal) {
        super(label, ordinal);
        this.enumerators = enumerators;
    }

    @Override
    public TypeInfo getType() {
        return enumerators.getType();
    }

    public ExtTypeInfoEnum getEnumerators() {
        return enumerators;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        if ((sqlFlags & NO_CASTS) == 0) {
            StringUtils.quoteStringSQL(builder.append("CAST("), label).append(" AS ");
            return enumerators.getType().getSQL(builder, sqlFlags).append(')');
        }
        return StringUtils.quoteStringSQL(builder, label);
    }

}
