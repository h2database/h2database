/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import java.util.function.Function;
import java.util.stream.Stream;

import org.h2.message.DbException;
import org.h2.util.json.JSONArray;
import org.h2.util.json.JSONNumber;
import org.h2.util.json.JSONValue;

/**
 * SQL/JSON path expression.
 */
abstract class JsonPathExpression {

    static final Function<JSONValue, Stream<JSONValue>> ARRAY_ELEMENTS_LAX = t -> {
        if (t instanceof JSONArray) {
            return ((JSONArray) t).stream();
        }
        return Stream.of(t);
    };

    JsonPathExpression() {
    }

    abstract Stream<JSONValue> getValue(Parameters params, JSONValue item, int lastCardinality);

    static Stream<JSONNumber> toNumericStream(Stream<JSONValue> stream, boolean strict) {
        if (!strict) {
            stream = stream.flatMap(ARRAY_ELEMENTS_LAX);
        }
        return stream.map(t -> {
            if (!(t instanceof JSONNumber)) {
                throw DbException.getInvalidValueException("numeric value", t);
            }
            return (JSONNumber) t;
        });
    }

}
