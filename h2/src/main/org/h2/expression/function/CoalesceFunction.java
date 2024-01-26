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

    private boolean ignoreNulls;

    public CoalesceFunction(int function) {
        this(function, new Expression[4]);
    }

    public CoalesceFunction(int function, Expression... args) {
        super(args);
        this.function = function;
    }

    public void setIgnoreNulls(boolean ignoreNulls) {
        this.ignoreNulls = ignoreNulls;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v;
        switch (function) {
        case COALESCE:
            v = ValueNull.INSTANCE;
            for (int i = 0, l = args.length; i < l; i++) {
                Value v2 = args[i].getValue(session);
                if (v2 != ValueNull.INSTANCE) {
                    v = v2.convertTo(type, session);
                    break;
                }
            }
            break;
        case GREATEST:
        case LEAST:
            v = greatestOrLeast(session);
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return v;
    }

    private Value greatestOrLeast(SessionLocal session) {
        Value v = ValueNull.INSTANCE, x = null;
        for (int i = 0, l = args.length; i < l; i++) {
            Value v2 = args[i].getValue(session);
            if (v2 != ValueNull.INSTANCE) {
                v2 = v2.convertTo(type, session);
                if (v == ValueNull.INSTANCE) {
                    if (x == null) {
                        v = v2;
                    } else {
                        int comp = session.compareWithNull(x, v2, false);
                        if (comp == Integer.MIN_VALUE) {
                            x = getWithNull(x, v2);
                        } else if (test(comp)) {
                            v = v2;
                            x = null;
                        }
                    }
                } else {
                    int comp = session.compareWithNull(v, v2, false);
                    if (comp == Integer.MIN_VALUE) {
                        if (i + 1 == l) {
                            return ValueNull.INSTANCE;
                        }
                        x = getWithNull(v, v2);
                        v = ValueNull.INSTANCE;
                    } else if (test(comp)) {
                        v = v2;
                    }
                }
            } else if (!ignoreNulls) {
                return ValueNull.INSTANCE;
            }
        }
        return v;
    }

    private static Value getWithNull(Value v, Value v2) {
        Value x = v.getValueWithFirstNull(v2);
        return x != null ? x : v;
    }

    private boolean test(int comp) {
        return function == GREATEST ? comp < 0 : comp > 0;
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

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        super.getUnenclosedSQL(builder, sqlFlags);
        if (function == GREATEST || function == LEAST) {
            builder.append(ignoreNulls ? " IGNORE NULLS" : " RESPECT NULLS");
        }
        return builder;
    }

}
