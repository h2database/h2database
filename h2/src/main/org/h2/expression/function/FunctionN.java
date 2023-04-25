/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.OperationN;
import org.h2.message.DbException;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Function with many arguments.
 */
public abstract class FunctionN extends OperationN implements NamedExpression {

    protected FunctionN(Expression[] args) {
        super(args);
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v1, v2, v3;
        int count = args.length;
        if (count >= 1) {
            v1 = args[0].getValue(session);
            if (v1 == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            if (count >= 2) {
                v2 = args[1].getValue(session);
                if (v2 == ValueNull.INSTANCE) {
                    return ValueNull.INSTANCE;
                }
                if (count >= 3) {
                    v3 = args[2].getValue(session);
                    if (v3 == ValueNull.INSTANCE) {
                        return ValueNull.INSTANCE;
                    }
                } else {
                    v3 = null;
                }
            } else {
                v3 = v2 = null;
            }
        } else {
            v3 = v2 = v1 = null;
        }
        return getValue(session, v1, v2, v3);
    }

    /**
     * Returns the value of this function.
     *
     * @param session
     *            the session
     * @param v1
     *            the value of first argument, or {@code null}
     * @param v2
     *            the value of second argument, or {@code null}
     * @param v3
     *            the value of third argument, or {@code null}
     * @return the resulting value
     */
    protected Value getValue(SessionLocal session, Value v1, Value v2, Value v3) {
        throw DbException.getInternalError();
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return writeExpressions(builder.append(getName()).append('('), args, sqlFlags).append(')');
    }

}
