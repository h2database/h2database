/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Operation2;
import org.h2.expression.TypedValueExpression;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueDouble;
import org.h2.value.ValueNull;

/**
 * A math function with two arguments and DOUBLE PRECISION result.
 */
public class MathFunction2 extends Operation2 implements NamedExpression {

    /**
     * ATAN2() (non-standard).
     */
    public static final int ATAN2 = 0;

    /**
     * LOG().
     */
    public static final int LOG = ATAN2 + 1;

    /**
     * POWER().
     */
    public static final int POWER = LOG + 1;

    private static final String[] NAMES = { //
            "LOG", "POWER", //
    };

    /**
     * Returns whether specified function is known by this class.
     *
     * @param upperName
     *            the name of the function in upper case
     * @return {@code true} if it exists
     */
    public static boolean exists(String upperName) {
        for (String n : NAMES) {
            if (n.equals(upperName)) {
                return true;
            }
        }
        return false;
    }

    private final int function;

    public MathFunction2(Expression arg1, Expression arg2, int function) {
        super(arg1, arg2);
        this.function = function;
    }

    @Override
    public Value getValue(Session session) {
        Value v1 = left.getValue(session);
        if (v1 == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        Value v2 = right.getValue(session);
        if (v2 == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        double d1 = v1.getDouble(), d2 = v2.getDouble();
        switch (function) {
        case ATAN2:
            d1 = Math.atan2(d1, d2);
            break;
        case LOG: {
            if (session.getMode().swapLogFunctionParameters) {
                double t = d2;
                d2 = d1;
                d1 = t;
            }
            if (d2 <= 0) {
                throw DbException.getInvalidValueException("LOG() argument", d2);
            }
            if (d1 <= 0 || d1 == 1) {
                throw DbException.getInvalidValueException("LOG() base", d1);
            }
            if (d1 == Math.E) {
                d1 = Math.log(d2);
            } else if (d1 == 10d) {
                d1 = Math.log10(d2);
            } else {
                d1 = Math.log(d2) / Math.log(d1);
            }
            break;
        }
        case POWER:
            d1 = Math.pow(d1, d2);
            break;
        default:
            throw DbException.throwInternalError("function=" + function);
        }
        return ValueDouble.get(d1);
    }

    @Override
    public Expression optimize(Session session) {
        left = left.optimize(session);
        right = right.optimize(session);
        type = TypeInfo.TYPE_DOUBLE;
        if (left.isConstant() && right.isConstant()) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        builder.append(getName()).append('(');
        left.getSQL(builder, sqlFlags).append(", ");
        return right.getSQL(builder, sqlFlags).append(')');
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
