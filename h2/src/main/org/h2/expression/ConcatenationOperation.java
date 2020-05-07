/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.Arrays;

import org.h2.engine.Session;
import org.h2.expression.function.Function;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarbinary;
import org.h2.value.ValueVarchar;

/**
 * Character string concatenation as in {@code 'Hello' || 'World'}, binary
 * string concatenation as in {@code X'01' || X'AB'} or an array concatenation
 * as in {@code ARRAY[1, 2] || 3}.
 */
public class ConcatenationOperation extends Operation2 {

    public ConcatenationOperation(Expression left, Expression right) {
        super(left, right);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        builder.append('(');
        left.getSQL(builder, sqlFlags).append(" || ");
        return right.getSQL(builder, sqlFlags).append(')');
    }

    @Override
    public Value getValue(Session session) {
        Value l = left.getValue(session).convertTo(type, session);
        if (l == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        Value r = right.getValue(session).convertTo(type, session);
        if (r == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        int valueType = type.getValueType();
        if (valueType == Value.VARCHAR) {
            String s1 = l.getString(), s2 = r.getString();
            return ValueVarchar.get(new StringBuilder(s1.length() + s2.length()).append(s1).append(s2).toString());
        } else if (valueType == Value.VARBINARY) {
            byte[] leftBytes = l.getBytesNoCopy(), rightBytes = r.getBytesNoCopy();
            int leftLength = leftBytes.length, rightLength = rightBytes.length;
            byte[] bytes = Arrays.copyOf(leftBytes, leftLength + rightLength);
            System.arraycopy(rightBytes, 0, bytes, leftLength, rightLength);
            return ValueVarbinary.getNoCopy(bytes);
        } else {
            Value[] leftValues = ((ValueArray) l).getList(), rightValues = ((ValueArray) r).getList();
            int leftLength = leftValues.length, rightLength = rightValues.length;
            Value[] values = Arrays.copyOf(leftValues, leftLength + rightLength);
            System.arraycopy(rightValues, 0, values, leftLength, rightLength);
            return ValueArray.get(values, session);
        }
    }

    @Override
    public Expression optimize(Session session) {
        left = left.optimize(session);
        right = right.optimize(session);
        TypeInfo l = left.getType(), r = right.getType();
        int lValueType = l.getValueType(), rValueType = r.getValueType();
        if (lValueType == Value.ARRAY || rValueType == Value.ARRAY) {
            type = TypeInfo.getTypeInfo(Value.ARRAY, -1, 0, TypeInfo.getHigherType(l, r).getExtTypeInfo());
        } else if (DataType.isBinaryStringType(lValueType) && DataType.isBinaryStringType(rValueType)) {
            type = TypeInfo.getTypeInfo(Value.VARBINARY, DataType.addPrecision(l.getPrecision(), r.getPrecision()), 0,
                    null);
        } else {
            if (session.getMode().treatEmptyStringsAsNull) {
                return Function.getFunctionWithArgs(Function.CONCAT, new Expression[] { left, right })
                        .optimize(session);
            }
            if (DataType.isCharacterStringType(lValueType) && DataType.isCharacterStringType(rValueType)) {
                type = TypeInfo.getTypeInfo(Value.VARCHAR, DataType.addPrecision(l.getPrecision(), r.getPrecision()),
                        0, null);
            } else {
                type = TypeInfo.TYPE_VARCHAR;
            }
        }
        if (left.isConstant() && right.isConstant()) {
            return TypedValueExpression.getTypedIfNull(getValue(session), type);
        }
        return this;
    }

}
