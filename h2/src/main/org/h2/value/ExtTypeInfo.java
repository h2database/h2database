/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import org.h2.engine.CastDataProvider;

/**
 * Extended parameters of a data type.
 */
public abstract class ExtTypeInfo {

    /**
     * Casts a specified value to this data type.
     *
     * @param value
     *            value to cast
     * @param provider
     *            the cast information provider
     * @return casted value
     */
    public abstract Value cast(Value value, CastDataProvider provider);

    /**
     * Returns SQL including parentheses that should be appended to a type name.
     *
     * @return SQL including parentheses that should be appended to a type name
     */
    public abstract String getCreateSQL();

    @Override
    public String toString() {
        return getCreateSQL();
    }

}
