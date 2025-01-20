/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.h2.message.DbException;
import org.h2.util.json.JSONString;
import org.h2.util.json.JSONValue;

/**
 * {@code like_regex} predicate.
 */
class LikeRegexPredicate extends JsonPathPredicate {

    private final JsonPathExpression expression;

    private final Pattern compiled;

    LikeRegexPredicate(JsonPathExpression expression, String pattern, String flags) {
        this.expression = expression;
        int intFlags = 0;
        if (flags != null) {
            for (int i = 0, l = flags.length(); i < l; i++) {
                switch (flags.charAt(i)) {
                case 's':
                    intFlags |= Pattern.DOTALL;
                    break;
                case 'm':
                    intFlags |= Pattern.MULTILINE;
                    break;
                case 'i':
                    intFlags |= Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
                    break;
                case 'x':
                    intFlags |= Pattern.COMMENTS;
                    break;
                case 'q':
                    intFlags |= Pattern.LITERAL;
                    break;
                default:
                    throw DbException.getInvalidValueException("Invalid like_regexp flags", flags);
                }
            }
        }
        compiled = Pattern.compile(pattern, intFlags);
    }

    @Override
    int doTest(Parameters params, JSONValue item, int lastCardinality) {
        Stream<JSONValue> stream = expression.getValue(params, item, lastCardinality);
        boolean strict = params.strict;
        if (!strict) {
            stream = stream.flatMap(JsonPathExpression.ARRAY_ELEMENTS_LAX);
        }
        int result = J_FALSE;
        for (Iterator<JSONValue> i = stream.iterator(); i.hasNext();) {
            int r = evaluate(i.next());
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

    private int evaluate(JSONValue v) {
        if (!(v instanceof JSONString)) {
            return J_UNKNOWN;
        }
        return compiled.matcher(((JSONString) v).getString()).matches() ? J_TRUE : J_FALSE;
    }

}
