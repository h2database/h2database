/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import java.util.stream.Stream;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.json.JSONValue;

/**
 * Current item "@".
 */
final class CurrentItemExpression extends JsonPathExpression {

    /**
     * Singleton instance.
     */
    static final CurrentItemExpression INSTANCE = new CurrentItemExpression();

    private CurrentItemExpression() {
    }

    @Override
    Stream<JSONValue> getValue(Parameters params, JSONValue item, int lastCardinality) {
        if (item == null) {
            throw DbException.get(ErrorCode.SYNTAX_ERROR_1, "@ in root expression");
        }
        return Stream.of(item);
    }

}
