/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import java.util.stream.Stream;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.json.JSONNumber;
import org.h2.util.json.JSONValue;

/**
 * Last subscript.
 */
final class LastSubscriptExpression extends JsonPathExpression {

    /**
     * Singelon instance.
     */
    static final LastSubscriptExpression INSTANCE = new LastSubscriptExpression();

    private LastSubscriptExpression() {
    }

    @Override
    Stream<JSONValue> getValue(Parameters params, JSONValue item, int lastCardinality) {
        if (lastCardinality < 0) {
            throw DbException.get(ErrorCode.SYNTAX_ERROR_1, "last outside array accessor");
        }
        return Stream.of(JSONNumber.valueOf(lastCardinality - 1));
    }

}
