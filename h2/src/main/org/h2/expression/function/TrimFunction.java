/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.Operation1_2;
import org.h2.expression.TypedValueExpression;
import org.h2.util.StringUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;

/**
 * A TRIM function.
 */
public final class TrimFunction extends Operation1_2 implements NamedExpression {

    /**
     * The LEADING flag.
     */
    public static final int LEADING = 1;

    /**
     * The TRAILING flag.
     */
    public static final int TRAILING = 2;

    private int flags;

    public TrimFunction(Expression from, Expression space, int flags) {
        super(from, space);
        this.flags = flags;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v1 = left.getValue(session);
        if (v1 == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        String space;
        if (right != null) {
            Value v2 = right.getValue(session);
            if (v2 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            space = v2.getString();
        } else {
            space = " ";
        }
        return ValueVarchar.get(StringUtils.trim(v1.getString(), (flags & LEADING) != 0, (flags & TRAILING) != 0, //
                space), session);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        if (right != null) {
            right = right.optimize(session);
        }
        type = TypeInfo.getTypeInfo(Value.VARCHAR, left.getType().getPrecision(), 0, null);
        if (left.isConstant() && (right == null || right.isConstant())) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        builder.append(getName()).append('(');
        switch (flags) {
        case LEADING:
            builder.append("LEADING ");
            break;
        case TRAILING:
            builder.append("TRAILING ");
            break;
        }
        if (right != null) {
            right.getUnenclosedSQL(builder, sqlFlags).append(" FROM ");
        }
        return left.getUnenclosedSQL(builder, sqlFlags).append(')');
    }

    @Override
    public String getName() {
        return "TRIM";
    }

}
