/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import static org.h2.util.json.path.PathToken.MINUS_SIGN;
import static org.h2.util.json.path.PathToken.PLUS_SIGN;

import java.math.BigDecimal;
import java.util.stream.Stream;

import org.h2.message.DbException;
import org.h2.util.json.JSONNumber;
import org.h2.util.json.JSONValue;

/**
 * A unary expression.
 */
final class UnaryExpression extends JsonPathExpression {

    private final JsonPathExpression expression;

    private final int type;

    UnaryExpression(JsonPathExpression expression, int type) {
        this.expression = expression;
        this.type = type;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    Stream<JSONValue> getValue(Parameters params, JSONValue item, int lastCardinality) {
        Stream<JSONNumber> stream = toNumericStream(expression.getValue(params, item, lastCardinality), params.strict);
        switch (type) {
        case PLUS_SIGN:
            return (Stream) stream;
        case MINUS_SIGN:
            return stream.map(t -> {
                BigDecimal n = t.getBigDecimal();
                return n.signum() == 0 ? t : new JSONNumber(n.negate());
            });
        default:
            throw DbException.getInternalError("type=" + type);
        }
    }

}
