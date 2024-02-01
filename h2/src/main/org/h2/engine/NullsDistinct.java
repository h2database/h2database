/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.util.HasSQL;

/**
 * Determines how rows with {@code NULL} values in indexed columns are handled
 * in unique indexes, unique constraints, or by unique predicate.
 */
public enum NullsDistinct implements HasSQL {

    /**
     * {@code NULL} values of columns are distinct.
     */
    DISTINCT,

    /**
     * {@code NULL} values of columns are distinct only if all columns have null values.
     */
    ALL_DISTINCT,

    /**
     * {@code NULL} values of columns are never distinct.
     */
    NOT_DISTINCT;

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        builder.append("NULLS ");
        switch (this) {
        case DISTINCT:
            builder.append("DISTINCT");
            break;
        case ALL_DISTINCT:
            builder.append("ALL DISTINCT");
            break;
        case NOT_DISTINCT:
            builder.append("NOT DISTINCT");
        }
        return builder;
    }

}
