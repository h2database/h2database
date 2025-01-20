/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import java.util.stream.Stream;

import org.h2.util.json.JSONValue;

/**
 * A filter.
 */
class FilterExpression extends JsonPathExpression {

    private final JsonPathExpression expression;

    private final JsonPathPredicate predicate;

    FilterExpression(JsonPathExpression expression, JsonPathPredicate predicate) {
        this.expression = expression;
        this.predicate = predicate;
    }

    @Override
    Stream<JSONValue> getValue(Parameters params, JSONValue item, int lastCardinality) {
        Stream<JSONValue> stream = expression.getValue(params, item, lastCardinality);
        if (!params.strict) {
            stream = stream.flatMap(ARRAY_ELEMENTS_LAX);
        }
        return stream.filter(t -> predicate.test(params, t, lastCardinality) == JsonPathPredicate.J_TRUE);
    }

}
