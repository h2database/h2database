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
import org.h2.util.MathUtils;
import org.h2.util.json.JSONArray;
import org.h2.util.json.JSONValue;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;

/**
 * Cardinality expression.
 */
public final class CardinalityExpression extends Function1 {

    private final boolean max;

    /**
     * Creates new instance of cardinality expression.
     *
     * @param arg
     *            argument
     * @param max
     *            {@code false} for {@code CARDINALITY}, {@code true} for
     *            {@code ARRAY_MAX_CARDINALITY}
     */
    public CardinalityExpression(Expression arg, boolean max) {
        super(arg);
        this.max = max;
    }

    @Override
    public Value getValue(SessionLocal session) {
        int result;
        if (max) {
            TypeInfo t = arg.getType();
            if (t.getValueType() == Value.ARRAY) {
                result = MathUtils.convertLongToInt(t.getPrecision());
            } else {
                throw DbException.getInvalidValueException("array", arg.getValue(session).getTraceSQL());
            }
        } else {
            Value v = arg.getValue(session);
            if (v == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            switch (v.getValueType()) {
            case Value.JSON: {
                JSONValue value = v.convertToAnyJson().getDecomposition();
                if (value instanceof JSONArray) {
                    result = ((JSONArray) value).length();
                } else {
                    return ValueNull.INSTANCE;
                }
                break;
            }
            case Value.ARRAY:
                result = ((ValueArray) v).getList().length;
                break;
            default:
                throw DbException.getInvalidValueException("array", v.getTraceSQL());
            }
        }
        return ValueInteger.get(result);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        arg = arg.optimize(session);
        type = TypeInfo.TYPE_INTEGER;
        if (arg.isConstant()) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

    @Override
    public String getName() {
        return max ? "ARRAY_MAX_CARDINALITY" : "CARDINALITY";
    }

}
