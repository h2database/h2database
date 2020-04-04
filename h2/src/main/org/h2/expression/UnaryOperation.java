/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Session;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Unary operation. Only negation operation is currently supported.
 */
public class UnaryOperation extends Operation1 {

    public UnaryOperation(Expression arg) {
        super(arg);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        // don't remove the space, otherwise it might end up some thing like
        // --1 which is a line remark
        builder.append("(- ");
        return arg.getSQL(builder, sqlFlags).append(')');
    }

    @Override
    public Value getValue(Session session) {
        Value a = arg.getValue(session).convertTo(type, session);
        return a == ValueNull.INSTANCE ? a : a.negate();
    }

    @Override
    public Expression optimize(Session session) {
        arg = arg.optimize(session);
        type = arg.getType();
        if (type.getValueType() == Value.UNKNOWN) {
            type = TypeInfo.TYPE_NUMERIC_FLOATING_POINT;
        } else if (type.getValueType() == Value.ENUM) {
            type = TypeInfo.TYPE_INTEGER;
        }
        if (arg.isConstant()) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

}
