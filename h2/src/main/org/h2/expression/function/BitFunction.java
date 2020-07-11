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
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * A bitwise function.
 */
public class BitFunction extends Operation1_2 implements NamedExpression {

    /**
     * BITAND() (non-standard).
     */
    public static final int BITAND = 0;

    /**
     * BITOR() (non-standard).
     */
    public static final int BITOR = BITAND + 1;

    /**
     * BITXOR() (non-standard).
     */
    public static final int BITXOR = BITOR + 1;

    /**
     * BITNOT() (non-standard).
     */
    public static final int BITNOT = BITXOR + 1;

    /**
     * BITGET() (non-standard).
     */
    public static final int BITGET = BITNOT + 1;

    /**
     * LSHIFT() (non-standard).
     */
    public static final int LSHIFT = BITGET + 1;

    /**
     * RSHIFT() (non-standard).
     */
    public static final int RSHIFT = LSHIFT + 1;

    private static final String[] NAMES = { //
            "BITAND", "BITOR", "BITXOR", "BITNOT", "BITGET", "LSHIFT", "RSHIFT" //
    };

    private final int function;

    public BitFunction(Expression arg1, Expression arg2, int function) {
        super(arg1, arg2);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v1 = left.getValue(session);
        if (v1 == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        if (function == BITNOT) {
            return ValueBigint.get(~v1.getLong());
        }
        Value v2 = right.getValue(session);
        if (v2 == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        long l1 = v1.getLong();
        switch (function) {
        case BITAND:
            l1 &= v2.getLong();
            break;
        case BITOR:
            l1 |= v2.getLong();
            break;
        case BITXOR:
            l1 ^= v2.getLong();
            break;
        case BITGET:
            return ValueBoolean.get((l1 & (1L << v2.getInt())) != 0);
        case LSHIFT:
            l1 <<= v2.getInt();
            break;
        case RSHIFT:
            l1 >>= v2.getInt();
            break;
        default:
            throw DbException.throwInternalError("function=" + function);
        }
        return ValueBigint.get(l1);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        if (right != null) {
            right = right.optimize(session);
        }
        type = function == BITGET ? TypeInfo.TYPE_BOOLEAN : TypeInfo.TYPE_BIGINT;
        if (left.isConstant() && (right == null || right.isConstant())) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        left.getUnenclosedSQL(builder.append(getName()).append('('), sqlFlags);
        if (right != null) {
            right.getUnenclosedSQL(builder.append(", "), sqlFlags);
        }
        return builder.append(')');
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
