/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.tools.CompressTool;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueVarbinary;

/**
 * A COMPRESS or EXPAND function.
 */
public final class CompressFunction extends Function1_2 {

    /**
     * COMPRESS() (non-standard).
     */
    public static final int COMPRESS = 0;

    /**
     * EXPAND() (non-standard).
     */
    public static final int EXPAND = COMPRESS + 1;

    private static final String[] NAMES = { //
            "COMPRESS", "EXPAND" //
    };

    private final int function;

    public CompressFunction(Expression arg1, Expression arg2, int function) {
        super(arg1, arg2);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session, Value v1, Value v2) {
        switch (function) {
        case COMPRESS:
            v1 = ValueVarbinary.getNoCopy(
                    CompressTool.getInstance().compress(v1.getBytesNoCopy(), v2 != null ? v2.getString() : null));
            break;
        case EXPAND:
            v1 = ValueVarbinary.getNoCopy(CompressTool.getInstance().expand(v1.getBytesNoCopy()));
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return v1;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        if (right != null) {
            right = right.optimize(session);
        }
        type = TypeInfo.TYPE_VARBINARY;
        if (left.isConstant() && (right == null || right.isConstant())) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
