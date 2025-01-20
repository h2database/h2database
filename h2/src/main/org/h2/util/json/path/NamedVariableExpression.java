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
 * A named variable.
 */
final class NamedVariableExpression extends JsonPathExpression {

    private final String name;

    NamedVariableExpression(String name) {
        this.name = name;
    }

    @Override
    Stream<JSONValue> getValue(Parameters params, JSONValue item, int lastCardinality) {
        JSONValue value = params.parameters.get(name);
        if (value != null) {
            return Stream.of(value);
        }
        throw DbException.get(ErrorCode.PARAMETER_NOT_SET_1, '$' + name);
    }

}
