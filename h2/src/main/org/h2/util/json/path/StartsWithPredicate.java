/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import java.util.Iterator;
import java.util.stream.Stream;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.json.JSONString;
import org.h2.util.json.JSONValue;

/**
 * Starts with predicate.
 */
class StartsWithPredicate extends JsonPathPredicate {

    private final JsonPathExpression expression;

    private final String string;

    private final boolean isVariable;

    StartsWithPredicate(JsonPathExpression expression, String string, boolean isVariable) {
        this.expression = expression;
        this.string = string;
        this.isVariable = isVariable;
    }

    @Override
    int doTest(Parameters params, JSONValue item, int lastCardinality) {
        Stream<JSONValue> stream = expression.getValue(params, item, lastCardinality);
        boolean strict = params.strict;
        if (!strict) {
            stream = stream.flatMap(JsonPathExpression.ARRAY_ELEMENTS_LAX);
        }
        String s;
        if (isVariable) {
            JSONValue value = params.parameters.get(string);
            if (value == null) {
                throw DbException.get(ErrorCode.PARAMETER_NOT_SET_1, '$' + string);
            }
            if (value instanceof JSONString) {
                s = ((JSONString) value).getString();
            } else {
                throw DbException.getInvalidValueException("starts with initial", value);
            }
        } else {
            s = string;
        }
        int result = J_FALSE;
        for (Iterator<JSONValue> i = stream.iterator(); i.hasNext();) {
            int r = evaluate(i.next(), s);
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
        return result;
    }

    private static int evaluate(JSONValue v, String s) {
        if (!(v instanceof JSONString)) {
            return J_UNKNOWN;
        }
        return ((JSONString) v).getString().startsWith(s) ? J_TRUE : J_FALSE;
    }

}
