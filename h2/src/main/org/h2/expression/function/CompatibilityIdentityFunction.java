/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Operation0;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * IDENTITY() or SCOPE_IDENTITY() compatibility functions.
 */
public final class CompatibilityIdentityFunction extends Operation0 implements NamedExpression {

    private final boolean scope;

    private TypeInfo type;

    public CompatibilityIdentityFunction(boolean scope) {
        this.scope = scope;
    }

    @Override
    public Value getValue(SessionLocal session) {
        return (scope ? session.getLastScopeIdentity() : session.getLastIdentity()).convertTo(type);
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return builder.append(getName()).append("()");
    }

    @Override
    public Expression optimize(SessionLocal session) {
        type = session.getMode().decimalSequences ? TypeInfo.TYPE_NUMERIC_BIGINT : TypeInfo.TYPE_BIGINT;
        return this;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.INDEPENDENT:
        case ExpressionVisitor.QUERY_COMPARABLE:
            return false;
        default:
            return true;
        }
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    @Override
    public int getCost() {
        return 1;
    }

    @Override
    public String getName() {
        return scope ? "SCOPE_IDENTITY" : "IDENTITY";
    }

}
