/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import static org.h2.util.json.path.PathToken.DOUBLE_EQUALS;
import static org.h2.util.json.path.PathToken.GREATER_THAN_OPERATOR;
import static org.h2.util.json.path.PathToken.GREATER_THAN_OR_EQUALS_OPERATOR;
import static org.h2.util.json.path.PathToken.LESS_THAN_OPERATOR;
import static org.h2.util.json.path.PathToken.LESS_THAN_OR_EQUALS_OPERATOR;
import static org.h2.util.json.path.PathToken.NOT_EQUALS_OPERATOR;

import java.util.stream.Stream;

import org.h2.message.DbException;
import org.h2.util.json.JSONArray;
import org.h2.util.json.JSONBoolean;
import org.h2.util.json.JSONNull;
import org.h2.util.json.JSONNumber;
import org.h2.util.json.JSONObject;
import org.h2.util.json.JSONString;
import org.h2.util.json.JSONValue;

/**
 * A comparison predicate.
 */
class ComparisonPredicate extends JsonPathPredicate {

    private final JsonPathExpression a;

    private final JsonPathExpression b;

    private final int type;

    ComparisonPredicate(JsonPathExpression a, JsonPathExpression b, int type) {
        this.a = a;
        this.b = b;
        this.type = type;
    }

    @Override
    int doTest(Parameters params, JSONValue item, int lastCardinality) {
        Stream<JSONValue> streamA = a.getValue(params, item, lastCardinality);
        Stream<JSONValue> streamB = b.getValue(params, item, lastCardinality);
        boolean strict = params.strict;
        if (!strict) {
            streamA = streamA.flatMap(JsonPathExpression.ARRAY_ELEMENTS_LAX);
            streamB = streamB.flatMap(JsonPathExpression.ARRAY_ELEMENTS_LAX);
        }
        JSONValue[] arrayA = streamA.toArray(JSONValue[]::new);
        JSONValue[] arrayB = streamB.toArray(JSONValue[]::new);
        int result = J_FALSE;
        for (JSONValue vA : arrayA) {
            for (JSONValue vB : arrayB) {
                int r = evaluate(params, vA, vB);
                if (r == J_UNKNOWN) {
                    return J_UNKNOWN;
                } else if (r == J_TRUE) {
                    if (strict) {
                        result = J_TRUE;
                    } else {
                        return J_TRUE;
                    }
                }
            }
        }
        return result;
    }

    private int evaluate(Parameters params, JSONValue vA, JSONValue vB) {
        if (vA instanceof JSONArray || vA instanceof JSONObject || vB instanceof JSONArray
                || vB instanceof JSONObject) {
            return J_UNKNOWN;
        }
        boolean aNull = vA instanceof JSONNull, bNull = vB instanceof JSONNull;
        if (aNull || bNull) {
            switch (type) {
            case DOUBLE_EQUALS:
                return aNull == bNull ? J_TRUE : J_FALSE;
            case NOT_EQUALS_OPERATOR:
                return aNull == bNull ? J_FALSE : J_TRUE;
            default:
                return J_UNKNOWN;
            }
        }
        if (vA instanceof JSONBoolean) {
            if (vB instanceof JSONBoolean) {
                return toResult(Boolean.compare(vA == JSONBoolean.TRUE, vB == JSONBoolean.TRUE));
            }
        } else if (vA instanceof JSONNumber) {
            if (vB instanceof JSONNumber) {
                return toResult(((JSONNumber) vA).getBigDecimal().compareTo(((JSONNumber) vB).getBigDecimal()));
            }
        } else if (vA instanceof JSONString) {
            if (vB instanceof JSONString) {
                return toResult(((JSONString) vA).getString().compareTo(((JSONString) vB).getString()));
            }
        } else if (vA instanceof JSONDatetime) {
            if (vB instanceof JSONDatetime) {
                return toResult(((JSONDatetime) vA).getValue().compareTo(((JSONDatetime) vB).getValue(),
                        params.provider, null));
            }
        }
        return J_UNKNOWN;
    }

    private int toResult(int cmp) {
        boolean b;
        switch (type) {
        case DOUBLE_EQUALS:
            b = cmp == 0;
            break;
        case NOT_EQUALS_OPERATOR:
            b = cmp != 0;
            break;
        case GREATER_THAN_OPERATOR:
            b = cmp > 0;
            break;
        case GREATER_THAN_OR_EQUALS_OPERATOR:
            b = cmp >= 0;
            break;
        case LESS_THAN_OPERATOR:
            b = cmp < 0;
            break;
        case LESS_THAN_OR_EQUALS_OPERATOR:
            b = cmp <= 0;
            break;
        default:
            throw DbException.getInternalError("type=" + type);
        }
        return b ? J_TRUE : J_FALSE;
    }

}
