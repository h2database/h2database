/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Stream;

import org.h2.message.DbException;
import org.h2.util.json.JSONArray;
import org.h2.util.json.JSONNumber;
import org.h2.util.json.JSONValue;

/**
 * JSON array accessor.
 */
final class ArrayAccessorExpression extends JsonPathExpression {

    private static final Function<JSONValue, JSONValue[]> ARRAY_ELEMENTS_STRICT = t -> {
        if (t instanceof JSONArray) {
            return ((JSONArray) t).getArray();
        }
        throw DbException.getInvalidValueException("array", t);
    };

    private static final Function<JSONValue, JSONValue[]> ARRAY_ELEMENTS_LAX = t -> {
        if (t instanceof JSONArray) {
            return ((JSONArray) t).getArray();
        }
        return new JSONValue[] { t };
    };

    private final JsonPathExpression expression;

    private final ArrayList<JsonPathExpression[]> subscripts;

    ArrayAccessorExpression(JsonPathExpression expression, ArrayList<JsonPathExpression[]> subscripts) {
        this.expression = expression;
        this.subscripts = subscripts;
    }

    @Override
    Stream<JSONValue> getValue(Parameters params, JSONValue item, int lastCardinality) {
        boolean strict = params.strict;
        Stream<JSONValue[]> stream = expression.getValue(params, item, lastCardinality)
                .map(strict ? ARRAY_ELEMENTS_STRICT : ARRAY_ELEMENTS_LAX);
        if (subscripts == null) {
            return stream.flatMap(Arrays::stream);
        }
        return stream.flatMap(elements -> {
            ArrayList<JSONValue> result = new ArrayList<>();
            int cardinality = elements.length;
            for (JsonPathExpression[] range : subscripts) {
                int from = getIndex(range[0], params, item, cardinality);
                int to = range.length > 1 ? getIndex(range[1], params, item, cardinality) : from;
                if (from > to) {
                    if (strict) {
                        throw DbException.getInvalidValueException("array index range", from + ".." + to);
                    }
                    continue;
                }
                if (from < 0) {
                    if (strict) {
                        throw DbException.getInvalidValueException("array index", from);
                    }
                    from = 0;
                } else if (from >= cardinality) {
                    if (strict) {
                        throw DbException.getInvalidValueException("array index", from);
                    }
                    continue;
                }
                if (to < 0) {
                    // Strict mode throws an exception earlier
                    continue;
                } else if (to >= cardinality) {
                    if (strict) {
                        throw DbException.getInvalidValueException("array index", to);
                    }
                    to = cardinality - 1;
                }
                for (int i = from; i <= to; i++) {
                    result.add(elements[i]);
                }
            }
            return result.stream();
        });
    }

    private static int getIndex(JsonPathExpression expression, Parameters params, JSONValue item, int cardinality) {
        JSONValue[] a = expression.getValue(params, item, cardinality).toArray(JSONValue[]::new);
        if (a.length != 1) {
            throw DbException.getInvalidValueException("array index", a.length + " values");
        }
        JSONValue v = a[0];
        if (v instanceof JSONNumber) {
            try {
                return ((JSONNumber) v).getBigDecimal().setScale(0, RoundingMode.DOWN).intValueExact();
            } catch (Exception e) {
                DbException.getInvalidValueException("array index", v);
            }
        }
        throw DbException.getInvalidValueException("array index", v.toString());
    }

}
