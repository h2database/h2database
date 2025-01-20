/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import static org.h2.util.json.path.PathToken.EXCLAMATION_MARK;
import static org.h2.util.json.path.PathToken.UNKNOWN;

import org.h2.message.DbException;
import org.h2.util.json.JSONValue;

/**
 * A unary predicate.
 */
final class UnaryPredicate extends JsonPathPredicate {

    private final JsonPathPredicate a;

    private final int type;

    UnaryPredicate(JsonPathPredicate a, int type) {
        this.a = a;
        this.type = type;
    }

    @Override
    int doTest(Parameters params, JSONValue item, int lastCardinality) {
        int r = a.test(params, item, lastCardinality);
        switch (type) {
        case EXCLAMATION_MARK:
            return r != J_UNKNOWN ? -r : J_UNKNOWN;
        case UNKNOWN:
            return r == J_UNKNOWN ? J_TRUE : J_FALSE;
        default:
            throw DbException.getInternalError("type=" + type);
        }
    }

}
