/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
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
import org.h2.value.ValueNull;

/**
 * A COALESCE, GREATEST, or LEAST function.
 */
public final class CoalesceFunction extends FunctionN {

    /**
     * COALESCE().
     */
    public static final int COALESCE = 0;

    /**
     * GREATEST() (non-standard).
     */
    public static final int GREATEST = COALESCE + 1;

    /**
     * LEAST() (non-standard).
     */
    public static final int LEAST = GREATEST + 1;

    private static final String[] NAMES = { //
            "COALESCE", "GREATEST", "LEAST" //
    };

    private final int function;

    public CoalesceFunction(int function) {
        this(function, new Expression[4]);
    }

    public CoalesceFunction(int function, Expression... args) {
        super(args);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v = ValueNull.INSTANCE;
        switch (function) {
        case COALESCE: {
            for (int i = 0, l = args.length; i < l; i++) {
                Value v2 = args[i].getValue(session);
                if (v2 != ValueNull.INSTANCE) {
                    v = v2.convertTo(type, session);
                    break;
                }
            }
            break;
        }
        case GREATEST:
        case LEAST: {
            for (int i = 0, l = args.length; i < l; i++) {
                Value v2 = args[i].getValue(session);
                if (v2 != ValueNull.INSTANCE) {
                    v2 = v2.convertTo(type, session);
                    if (v == ValueNull.INSTANCE) {
                        v = v2;
                    } else {
                        int comp = session.compareTypeSafe(v, v2);
                        if (function == GREATEST) {
                            if (comp < 0) {
                                v = v2;
                            }
                        } else if (comp > 0) {
                            v = v2;
                        }
                    }
                }
            }
            break;
        }
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return v;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        boolean allConst = optimizeArguments(session, true);
        type = TypeInfo.getHigherType(args);
        if (type.getValueType() <= Value.NULL) {
            type = TypeInfo.TYPE_VARCHAR;
        }
        if (allConst) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
