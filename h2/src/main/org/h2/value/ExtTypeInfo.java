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
     * Appends SQL representation of this object to the specified string
     * builder.
     *
     * @param builder
     *            string builder
     * @return the specified string builder
     */
    public abstract StringBuilder getSQL(StringBuilder builder);

    @Override
    public String toString() {
        return getSQL(new StringBuilder()).toString();
    }

}
