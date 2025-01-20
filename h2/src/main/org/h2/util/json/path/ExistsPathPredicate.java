/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import org.h2.util.json.JSONValue;

/**
 * {@code exists} predicate.
 */
final class ExistsPathPredicate extends JsonPathPredicate {

    private final JsonPathExpression expression;

    ExistsPathPredicate(JsonPathExpression expression) {
        this.expression = expression;
    }

    @Override
    int doTest(Parameters params, JSONValue item, int lastCardinality) {
        return expression.getValue(params, item, lastCardinality).findAny().isPresent() ? J_TRUE : J_FALSE;
    }

}
