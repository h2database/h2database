/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

/**
 * Extended parameters of a data type.
 */
public abstract class ExtTypeInfo {

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
