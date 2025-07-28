/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import java.util.stream.Stream;

import org.h2.util.json.JSONValue;

/**
 * The context variable "$".
 */
final class ContextVariableExpression extends JsonPathExpression {

    /**
     * Singleton instance.
     */
    static final ContextVariableExpression INSTANCE = new ContextVariableExpression();

    private ContextVariableExpression() {
    }

    @Override
    Stream<JSONValue> getValue(Parameters params, JSONValue item, int lastCardinality) {
        return Stream.of(params.context);
    }

}
