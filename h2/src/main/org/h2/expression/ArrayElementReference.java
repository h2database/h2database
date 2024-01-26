/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.api.ErrorCode;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.mvstore.db.Store;
import org.h2.util.json.JSONArray;
import org.h2.util.json.JSONValue;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueJson;
import org.h2.value.ValueNull;

/**
 * Array element reference.
 */
public final class ArrayElementReference extends Operation2 {

    public ArrayElementReference(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        left.getSQL(builder, sqlFlags, AUTO_PARENTHESES).append('[');
        return right.getUnenclosedSQL(builder, sqlFlags).append(']');
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value l = left.getValue(session);
        Value r = right.getValue(session);
        if (l != ValueNull.INSTANCE && r != ValueNull.INSTANCE) {
            int element = r.getInt();
            if (left.getType().getValueType() == Value.ARRAY) {
                Value[] list = ((ValueArray) l).getList();
                int cardinality = list.length;
                if (element >= 1 && element <= cardinality) {
                    return list[element - 1];
                }
                throw DbException.get(ErrorCode.ARRAY_ELEMENT_ERROR_2, Integer.toString(element), "1.." + cardinality);
            } else {
                JSONValue value = l.convertToAnyJson().getDecomposition();
                if (value instanceof JSONArray) {
                    JSONValue jsonValue = ((JSONArray) value).getElement(element - 1);
                    if (jsonValue != null) {
                        return ValueJson.fromJson(jsonValue);
                    }
                }
            }
        }
        return ValueNull.INSTANCE;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        right = right.optimize(session);
        TypeInfo leftType = left.getType();
        switch (leftType.getValueType()) {
        case Value.NULL:
            return ValueExpression.NULL;
        case Value.JSON:
            type = TypeInfo.TYPE_JSON;
            if (left.isConstant() && right.isConstant()) {
                return TypedValueExpression.getTypedIfNull(getValue(session), type);
            }
            break;
        case Value.ARRAY:
            type = (TypeInfo) leftType.getExtTypeInfo();
            if (left.isConstant() && right.isConstant()) {
                return TypedValueExpression.get(getValue(session), type);
            }
            break;
        default:
            throw Store.getInvalidExpressionTypeException("Array", left);
        }
        return this;
    }

}
