/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import static org.h2.util.json.path.PathToken.DOUBLE_AMPERSAND;
import static org.h2.util.json.path.PathToken.DOUBLE_VERTICAL_BAR;

import org.h2.message.DbException;
import org.h2.util.json.JSONValue;

/**
 * A binary predicate.
 */
final class BinaryPredicate extends JsonPathPredicate {

    private final JsonPathPredicate a;

    private final JsonPathPredicate b;

    private final int type;

    BinaryPredicate(JsonPathPredicate a, JsonPathPredicate b, int type) {
        this.a = a;
        this.b = b;
        this.type = type;
    }

    @Override
    int doTest(Parameters params, JSONValue item, int lastCardinality) {
        int r1 = a.test(params, item, lastCardinality);
        switch (type) {
        case DOUBLE_AMPERSAND: {
            if (r1 == J_FALSE) {
                return J_FALSE;
            }
            int r2 = b.test(params, item, lastCardinality);
            return r1 == J_TRUE ? r2 : r2 == J_FALSE ? J_FALSE : J_UNKNOWN;
        }
        case DOUBLE_VERTICAL_BAR: {
            if (r1 == J_TRUE) {
                return J_TRUE;
            }
            int r2 = b.test(params, item, lastCardinality);
            return r1 == J_FALSE ? r2 : r2 == J_TRUE ? J_TRUE : J_UNKNOWN;
        }
        default:
            throw DbException.getInternalError("type=" + type);
        }
    }

}
