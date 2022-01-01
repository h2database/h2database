/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.expression.ValueExpression;
import org.h2.schema.Domain;
import org.h2.table.Column;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A cast specification.
 */
public final class CastSpecification extends Function1 {

    private Domain domain;

    public CastSpecification(Expression arg, Column column) {
        super(arg);
        type = column.getType();
        domain = column.getDomain();
    }

    public CastSpecification(Expression arg, TypeInfo type) {
        super(arg);
        this.type = type;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v = arg.getValue(session).castTo(type, session);
        if (domain != null) {
            domain.checkConstraints(session, v);
        }
        return v;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        arg = arg.optimize(session);
        if (arg.isConstant()) {
            Value v = getValue(session);
            if (v == ValueNull.INSTANCE || canOptimizeCast(arg.getType().getValueType(), type.getValueType())) {
                return TypedValueExpression.get(v, type);
            }
        }
        return this;
    }

    @Override
    public boolean isConstant() {
        return arg instanceof ValueExpression && canOptimizeCast(arg.getType().getValueType(), type.getValueType());
    }

    private static boolean canOptimizeCast(int src, int dst) {
        switch (src) {
        case Value.TIME:
            switch (dst) {
            case Value.TIME_TZ:
            case Value.TIMESTAMP:
            case Value.TIMESTAMP_TZ:
                return false;
            }
            break;
        case Value.TIME_TZ:
            switch (dst) {
            case Value.TIME:
            case Value.TIMESTAMP:
            case Value.TIMESTAMP_TZ:
                return false;
            }
            break;
        case Value.DATE:
            if (dst == Value.TIMESTAMP_TZ) {
                return false;
            }
            break;
        case Value.TIMESTAMP:
            switch (dst) {
            case Value.TIME_TZ:
            case Value.TIMESTAMP_TZ:
                return false;
            }
            break;
        case Value.TIMESTAMP_TZ:
            switch (dst) {
            case Value.TIME:
            case Value.DATE:
            case Value.TIMESTAMP:
                return false;
            }
        }
        return true;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        builder.append("CAST(");
        arg.getUnenclosedSQL(builder, arg instanceof ValueExpression ? sqlFlags | NO_CASTS : sqlFlags).append(" AS ");
        return (domain != null ? domain : type).getSQL(builder, sqlFlags).append(')');
    }

    @Override
    public String getName() {
        return "CAST";
    }

}
