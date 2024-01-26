/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBigint;
import org.h2.value.ValueNull;

/**
 * CHAR_LENGTH(), or OCTET_LENGTH() function.
 */
public final class LengthFunction extends Function1 {

    /**
     * CHAR_LENGTH().
     */
    public static final int CHAR_LENGTH = 0;

    /**
     * OCTET_LENGTH().
     */
    public static final int OCTET_LENGTH = CHAR_LENGTH + 1;

    /**
     * BIT_LENGTH() (non-standard).
     */
    public static final int BIT_LENGTH = OCTET_LENGTH + 1;

    private static final String[] NAMES = { //
            "CHAR_LENGTH", "OCTET_LENGTH", "BIT_LENGTH" //
    };

    private final int function;

    public LengthFunction(Expression arg, int function) {
        super(arg);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v = arg.getValue(session);
        if (v == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        long l;
        switch (function) {
        case CHAR_LENGTH:
            l = v.charLength();
            break;
        case OCTET_LENGTH:
            l = v.octetLength();
            break;
        case BIT_LENGTH:
            l = v.octetLength() * 8;
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return ValueBigint.get(l);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        arg = arg.optimize(session);
        type = TypeInfo.TYPE_BIGINT;
        if (arg.isConstant()) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
