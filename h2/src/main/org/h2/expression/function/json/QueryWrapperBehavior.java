/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function.json;

/**
 * JSON_QUERY wrapper behavior.
 */
public enum QueryWrapperBehavior {

    /**
     * Don't wrap multiple results into JSON array (default).
     */
    WITHOUT("WITHOUT ARRAY WRAPPER"),

    /**
     * Wrap multiple results into JSON array and leave single results as is.
     */
    WITH_CONDITIONAL("WITH CONDITIONAL ARRAY WRAPPER"),

    /**
     * Wrap single and multiple results into JSON array (default if WITH is
     * specified)
     */
    WITH_UNCONDITIONAL("WITH UNCONDITIONAL ARRAY WRAPPER");

    private final String sql;

    private QueryWrapperBehavior(String sql) {
        this.sql = sql;
    }

    /**
     * Returns SQL representation of this clause.
     *
     * @return SQL representation of this clause
     */
    public String getSQL() {
        return sql;
    }

}
