/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function.json;

/**
 * JSON_QUERY quotes behavior.
 */
public enum QueryQuotesBehavior {

    /**
     * Leave JSON string result as is (default).
     */
    KEEP,

    /**
     * Re-interpret JSON string result as JSON text.
     */
    OMIT;

}
