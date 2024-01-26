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
import org.h2.value.ValueVarchar;

/**
 * A string function with two arguments.
 */
public final class StringFunction2 extends Function2 {

    /**
     * LEFT() (non-standard).
     */
    public static final int LEFT = 0;

    /**
     * RIGHT() (non-standard).
     */
    public static final int RIGHT = LEFT + 1;

    /**
     * REPEAT() (non-standard).
     */
    public static final int REPEAT = RIGHT + 1;

    private static final String[] NAMES = { //
            "LEFT", "RIGHT", "REPEAT" //
    };

    private final int function;

    public StringFunction2(Expression arg1, Expression arg2, int function) {
        super(arg1, arg2);
        this.function = function;
    }

    @Override
    public Value getValue(SessionLocal session, Value v1, Value v2) {
        String s = v1.getString();
        int count = v2.getInt();
        if (count <= 0) {
            return ValueVarchar.get("", session);
        }
        int length = s.length();
        switch (function) {
        case LEFT:
            if (count > length) {
                count = length;
            }
            s = s.substring(0, count);
            break;
        case RIGHT:
            if (count > length) {
                count = length;
            }
            s = s.substring(length - count);
            break;
        case REPEAT: {
            StringBuilder builder = new StringBuilder(length * count);
            while (count-- > 0) {
                builder.append(s);
            }
            s = builder.toString();
            break;
        }
        default:
            throw DbException.getInternalError("function=" + function);
        }
        return ValueVarchar.get(s, session);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        right = right.optimize(session);
        switch (function) {
        case LEFT:
        case RIGHT:
            type = TypeInfo.getTypeInfo(Value.VARCHAR, left.getType().getPrecision(), 0, null);
            break;
        case REPEAT:
            type = TypeInfo.TYPE_VARCHAR;
            break;
        default:
            throw DbException.getInternalError("function=" + function);
        }
        if (left.isConstant() && right.isConstant()) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public String getName() {
        return NAMES[function];
    }

}
