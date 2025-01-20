/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import java.util.stream.Stream;

import org.h2.message.DbException;
import org.h2.util.DateTimeTemplate;
import org.h2.util.json.JSONString;
import org.h2.util.json.JSONValue;

/**
 * A datetime method.
 */
final class DatetimeMethod extends JsonPathExpression {

    private final JsonPathExpression expression;

    private final DateTimeTemplate compiledTemplate;

    DatetimeMethod(JsonPathExpression expression, String template) {
        this.expression = expression;
        compiledTemplate = DateTimeTemplate.of(template);
    }

    @Override
    Stream<JSONValue> getValue(Parameters params, JSONValue item, int lastCardinality) {
        Stream<JSONValue> stream = expression.getValue(params, item, lastCardinality);
        if (!params.strict) {
            stream = stream.flatMap(ARRAY_ELEMENTS_LAX);
        }
        return stream.map(t -> {
            if (t instanceof JSONString) {
                return new JSONDatetime(compiledTemplate.parse(((JSONString) t).getString(), params.provider));
            } else {
                throw DbException.getInvalidValueException("datetime string", t);
            }
        });
    }

}
