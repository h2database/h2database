/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.api.ErrorCode;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Variable;
import org.h2.message.DbException;
import org.h2.value.Value;

/**
 * A SET function.
 */
public final class SetFunction extends Function2 {

    public SetFunction(Expression arg1, Expression arg2) {
        super(arg1, arg2);
    }

    @Override
    public Value getValue(SessionLocal session) {
        Variable var = (Variable) left;
        Value v = right.getValue(session);
        session.setVariable(var.getName(), v);
        return v;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        right = right.optimize(session);
        type = right.getType();
        if (!(left instanceof Variable)) {
            throw DbException.get(ErrorCode.CAN_ONLY_ASSIGN_TO_VARIABLE_1, left.getTraceSQL());
        }
        return this;
    }

    @Override
    public String getName() {
        return "SET";
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (!super.isEverything(visitor)) {
            return false;
        }
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.QUERY_COMPARABLE:
        case ExpressionVisitor.READONLY:
            return false;
        default:
            return true;
        }
    }

}
