/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A NULLIF function.
 */
public final class NullIfFunction extends Function2 {

    public NullIfFunction(Expression arg1, Expression arg2) {
        super(arg1, arg2);
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v = left.getValue(session);
        if (session.compareWithNull(v, right.getValue(session), true) == 0) {
            v = ValueNull.INSTANCE;
        }
        return v;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        right = right.optimize(session);
        type = left.getType();
        TypeInfo.checkComparable(type, right.getType());
        if (left.isConstant() && right.isConstant()) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public String getName() {
        return "NULLIF";
    }

}
