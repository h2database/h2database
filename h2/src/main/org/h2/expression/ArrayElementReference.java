/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueCollectionBase;
import org.h2.value.ValueNull;

/**
 * Array element reference.
 */
public class ArrayElementReference extends Operation2 {

    public ArrayElementReference(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        left.getSQL(builder.append('('), sqlFlags).append('[');
        return right.getSQL(builder, sqlFlags).append("])");
    }

    @Override
    public Value getValue(Session session) {
        Value l = left.getValue(session);
        Value r = right.getValue(session);
        if (l != ValueNull.INSTANCE && r != ValueNull.INSTANCE) {
            Value[] list = ((ValueCollectionBase) l).getList();
            int element = r.getInt();
            if (element >= 1 && element <= list.length) {
                return list[element - 1];
            }
        }
        return ValueNull.INSTANCE;
    }

    @Override
    public Expression optimize(Session session) {
        left = left.optimize(session);
        right = right.optimize(session);
        TypeInfo leftType = left.getType();
        switch (leftType.getValueType()) {
        case Value.NULL:
            return ValueExpression.NULL;
        case Value.ARRAY:
            type = (TypeInfo) leftType.getExtTypeInfo();
            if (left.isConstant() && right.isConstant()) {
                return TypedValueExpression.get(getValue(session), type);
            }
            break;
        case Value.ROW:
            type = TypeInfo.TYPE_NULL;
            if (left.isConstant() && right.isConstant()) {
                return ValueExpression.get(getValue(session));
            }
            break;
        default:
            throw DbException.getInvalidValueException("Array", leftType.getSQL(new StringBuilder()));
        }
        return this;
    }

}
