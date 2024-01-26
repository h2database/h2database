/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import java.util.regex.Pattern;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * A SIGNAL function.
 */
public final class SignalFunction extends Function2 {

    private static final Pattern SIGNAL_PATTERN = Pattern.compile("[0-9A-Z]{5}");

    public SignalFunction(Expression arg1, Expression arg2) {
        super(arg1, arg2);
    }

    @Override
    public Value getValue(SessionLocal session, Value v1, Value v2) {
        String sqlState = v1.getString();
        if (sqlState.startsWith("00") || !SIGNAL_PATTERN.matcher(sqlState).matches()) {
            throw DbException.getInvalidValueException("SQLSTATE", sqlState);
        }
        throw DbException.fromUser(sqlState, v2.getString());
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        right = right.optimize(session);
        type = TypeInfo.TYPE_NULL;
        return this;
    }

    @Override
    public String getName() {
        return "SIGNAL";
    }

}
