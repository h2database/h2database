/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.util.StringUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueVarchar;

/**
 * A TRIM, LTRIM, RTRIM, or BTRIM function.
 */
public final class TrimFunction extends Function1_2 {

    /**
     * The LEADING flag.
     */
    public static final int LEADING = 1;

    /**
     * The TRAILING flag.
     */
    public static final int TRAILING = 2;

    /**
     * The multi-character flag.
     */
    public static final int MULTI_CHARACTER = 4;

    private int flags;

    public TrimFunction(Expression from, Expression space, int flags) {
        super(from, space);
        this.flags = flags;
    }

    @Override
    public Value getValue(SessionLocal session, Value v1, Value v2) {
        return ValueVarchar.get(StringUtils.trim(v1.getString(), (flags & LEADING) != 0, (flags & TRAILING) != 0,
                v2 != null ? v2.getString() : " "), session);
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
        if ((flags & MULTI_CHARACTER) != 0) {
            left.getUnenclosedSQL(builder, sqlFlags);
            if (right != null) {
                right.getUnenclosedSQL(builder.append(", "), sqlFlags);
            }
        } else {
            boolean needFrom = false;
            switch (flags) {
            case LEADING:
                builder.append("LEADING ");
                needFrom = true;
                break;
            case TRAILING:
                builder.append("TRAILING ");
                needFrom = true;
                break;
            }
            if (right != null) {
                right.getUnenclosedSQL(builder, sqlFlags);
                needFrom = true;
            }
            if (needFrom) {
                builder.append(" FROM ");
            }
            left.getUnenclosedSQL(builder, sqlFlags);
        }
        return builder.append(')');
    }

    @Override
    public String getName() {
        switch (flags) {
        case LEADING | MULTI_CHARACTER:
            return "LTRIM";
        case TRAILING | MULTI_CHARACTER:
            return "RTRIM";
        case LEADING | TRAILING | MULTI_CHARACTER:
            return "BTRIM";
        default:
            return "TRIM";
        }
    }

}
