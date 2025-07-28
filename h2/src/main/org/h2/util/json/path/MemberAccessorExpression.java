/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import java.util.List;
import java.util.stream.Stream;

import org.h2.message.DbException;
import org.h2.util.json.JSONObject;
import org.h2.util.json.JSONValue;

/**
 * A member accessor.
 */
final class MemberAccessorExpression extends JsonPathExpression {

    private final JsonPathExpression expression;

    private final String key;

    MemberAccessorExpression(JsonPathExpression expression, String key) {
        this.expression = expression;
        this.key = key;
    }

    @Override
    Stream<JSONValue> getValue(Parameters params, JSONValue item, int lastCardinality) {
        Stream<JSONValue> stream = expression.getValue(params, item, lastCardinality);
        boolean strict = params.strict;
        if (!strict) {
            stream = stream.flatMap(ARRAY_ELEMENTS_LAX);
        }
        return stream.flatMap(t -> {
            if (t instanceof JSONObject) {
                List<JSONValue> list = ((JSONObject) t).getAll(key);
                if (strict && list.isEmpty()) {
                    throw DbException.getInvalidValueException("JSON object with member ", t);
                }
                return list.stream();
            } else {
                if (strict) {
                    throw DbException.getInvalidValueException("JSON object", t);
                }
                return Stream.empty();
            }
        });
    }

}
