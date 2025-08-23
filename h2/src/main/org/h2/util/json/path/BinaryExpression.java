/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import static org.h2.util.json.path.PathToken.ASTERISK;
import static org.h2.util.json.path.PathToken.MINUS_SIGN;
import static org.h2.util.json.path.PathToken.PERCENT;
import static org.h2.util.json.path.PathToken.PLUS_SIGN;
import static org.h2.util.json.path.PathToken.SOLIDUS;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.json.JSONNumber;
import org.h2.util.json.JSONValue;

/**
 * A binary expression.
 */
final class BinaryExpression extends JsonPathExpression {

    private final JsonPathExpression a;
    private final JsonPathExpression b;
    private final int type;

    BinaryExpression(JsonPathExpression a, JsonPathExpression b, int type) {
        this.a = a;
        this.b = b;
        this.type = type;
    }

    @Override
    Stream<JSONValue> getValue(Parameters params, JSONValue item, int lastCardinality) {
        boolean strict = params.strict;
        BigDecimal n1 = getSingleNumeric(a, params, strict, item, lastCardinality);
        BigDecimal n2 = getSingleNumeric(b, params, strict, item, lastCardinality);
        switch (type) {
        case PLUS_SIGN:
            n1 = n1.add(n2);
            break;
        case MINUS_SIGN:
            n1 = n1.subtract(n2);
            break;
        case ASTERISK:
            n1 = n1.multiply(n2);
            break;
        case SOLIDUS:
            if (n2.signum() == 0) {
                throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, n1.toString());
            }
            n1 = n1.divide(n2);
            break;
        case PERCENT:
            if (n2.signum() == 0) {
                throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, n1.toString());
            }
            n1 = n1.remainder(n2);
            break;
        default:
            throw DbException.getInternalError("type=" + type);
        }
        return Stream.of(new JSONNumber(n1));
    }

    private static BigDecimal getSingleNumeric(JsonPathExpression expression, //
            Parameters params, boolean strict, JSONValue item, int lastCardinality) {
        Stream<JSONValue> stream = expression.getValue(params, item, lastCardinality);
        ArrayList<JSONValue> list = stream.collect(Collectors.toCollection(() -> new ArrayList<>(1)));
        if (list.size() != 1) {
            throw DbException.getInvalidValueException("single value", list.size() + " values");
        }
        JSONValue v = list.get(0);
        if (!(v instanceof JSONNumber)) {
            throw DbException.getInvalidValueException("numeric value", v);
        }
        return ((JSONNumber) v).getBigDecimal();
    }

}
