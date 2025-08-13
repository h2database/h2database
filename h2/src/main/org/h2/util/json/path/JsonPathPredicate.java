/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import org.h2.message.DbException;
import org.h2.util.json.JSONValue;

/**
 * SQL/JSON path predicate.
 */
abstract class JsonPathPredicate {

    static final int J_TRUE = 1, J_FALSE = -1, J_UNKNOWN = 0;

    JsonPathPredicate() {
    }

    final int test(Parameters params, JSONValue item, int lastCardinality) {
        try {
            return doTest(params, item, lastCardinality);
        } catch (DbException e) {
            return J_UNKNOWN;
        }
    }

    abstract int doTest(Parameters params, JSONValue item, int lastCardinality);

}
