/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.Arrays;

import org.h2.engine.Session;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBytes;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * Character string concatenation as in {@code 'Hello' || 'World'}, binary
 * string concatenation as in {@code X'01' || X'AB'} or an array concatenation
 * as in {@code ARRAY[1, 2] || 3}.
 */
public class ConcatenationOperation extends Expression {

    private Expression left, right;
    private TypeInfo type;

    public ConcatenationOperation(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        builder.append('(');
        left.getSQL(builder, alwaysQuote).append(" || ");
        return right.getSQL(builder, alwaysQuote).append(')');
    }

    @Override
    public Value getValue(Session session) {
        Value l = left.getValue(session).convertTo(type, session, false, null);
        Value r = right.getValue(session).convertTo(type, session, false, null);
        switch (type.getValueType()) {
        case Value.ARRAY: {
            if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            Value[] leftValues = ((ValueArray) l).getList(), rightValues = ((ValueArray) r).getList();
            int leftLength = leftValues.length, rightLength = rightValues.length;
            Value[] values = Arrays.copyOf(leftValues, leftLength + rightLength);
            System.arraycopy(rightValues, 0, values, leftLength, rightLength);
            return ValueArray.get(values);
        }
        case Value.BYTES: {
            if (l == ValueNull.INSTANCE || r == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            byte[] leftBytes = l.getBytesNoCopy(), rightBytes = r.getBytesNoCopy();
            int leftLength = leftBytes.length, rightLength = rightBytes.length;
            byte[] bytes = Arrays.copyOf(leftBytes, leftLength + rightLength);
            System.arraycopy(rightBytes, 0, bytes, leftLength, rightLength);
            return ValueBytes.getNoCopy(bytes);
        }
        default: {
            if (l == ValueNull.INSTANCE) {
                if (session.getDatabase().getMode().nullConcatIsNull) {
                    return ValueNull.INSTANCE;
                }
                return r;
            } else if (r == ValueNull.INSTANCE) {
                if (session.getDatabase().getMode().nullConcatIsNull) {
                    return ValueNull.INSTANCE;
                }
                return l;
            }
            String s1 = l.getString(), s2 = r.getString();
            StringBuilder buff = new StringBuilder(s1.length() + s2.length());
            buff.append(s1).append(s2);
            return ValueString.get(buff.toString());
        }
        }
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
        right.mapColumns(resolver, level, state);
    }

    @Override
    public Expression optimize(Session session) {
        left = left.optimize(session);
        right = right.optimize(session);
        TypeInfo l = left.getType(), r = right.getType();
        int lValueType = l.getValueType(), rValueType = r.getValueType();
        if (lValueType == Value.ARRAY || rValueType == Value.ARRAY) {
            type = TypeInfo.TYPE_ARRAY;
        } else if (DataType.isBinaryStringType(lValueType) && DataType.isBinaryStringType(rValueType)) {
            type = TypeInfo.getTypeInfo(Value.BYTES, DataType.addPrecision(l.getPrecision(), r.getPrecision()), 0,
                    null);
        } else if (DataType.isCharacterStringType(lValueType) && DataType.isCharacterStringType(rValueType)) {
            type = TypeInfo.getTypeInfo(Value.STRING, DataType.addPrecision(l.getPrecision(), r.getPrecision()), 0,
                    null);
        } else {
            type = TypeInfo.TYPE_STRING;
        }
        if (left.isConstant() && right.isConstant()) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        right.setEvaluatable(tableFilter, b);
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        left.updateAggregate(session, stage);
        right.updateAggregate(session, stage);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && right.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return left.getCost() + right.getCost() + 1;
    }

    @Override
    public int getSubexpressionCount() {
        return 2;
    }

    @Override
    public Expression getSubexpression(int index) {
        switch (index) {
        case 0:
            return left;
        case 1:
            return right;
        default:
            throw new IndexOutOfBoundsException();
        }
    }

}
