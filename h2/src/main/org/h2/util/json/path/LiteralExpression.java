/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import java.math.BigDecimal;
import java.util.stream.Stream;

import org.h2.util.json.JSONBoolean;
import org.h2.util.json.JSONNull;
import org.h2.util.json.JSONNumber;
import org.h2.util.json.JSONString;
import org.h2.util.json.JSONValue;

/**
 * A literal.
 */
final class LiteralExpression extends JsonPathExpression {

    /**
     * {@code true}.
     */
    static final LiteralExpression TRUE = new LiteralExpression(JSONBoolean.TRUE);

    /**
     * {@code false}.
     */
    static final LiteralExpression FALSE = new LiteralExpression(JSONBoolean.FALSE);

    /**
     * {@code null}.
     */
    static final LiteralExpression NULL = new LiteralExpression(JSONNull.NULL);

    static LiteralExpression get(String s) {
        return new LiteralExpression(new JSONString(s));
    }

    static LiteralExpression get(BigDecimal n) {
        return new LiteralExpression(new JSONNumber(n));
    }

    static LiteralExpression get(int n) {
        return new LiteralExpression(new JSONNumber(BigDecimal.valueOf(n)));
    }

    private final JSONValue value;

    private LiteralExpression(JSONValue value) {
        this.value = value;
    }

    @Override
    Stream<JSONValue> getValue(Parameters params, JSONValue item, int lastCardinality) {
        return Stream.of(value);
    }

}
