/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import java.util.Arrays;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * Type predicate (IS [NOT] OF).
 */
public final class TypePredicate extends SimplePredicate {

    private final TypeInfo[] typeList;
    private int[] valueTypes;

    public TypePredicate(Expression left, boolean not, boolean whenOperand, TypeInfo[] typeList) {
        super(left, not, whenOperand);
        this.typeList = typeList;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return getWhenSQL(left.getSQL(builder, sqlFlags, AUTO_PARENTHESES), sqlFlags);
    }

    @Override
    public StringBuilder getWhenSQL(StringBuilder builder, int sqlFlags) {
        builder.append(" IS");
        if (not) {
            builder.append(" NOT");
        }
        builder.append(" OF (");
        for (int i = 0; i < typeList.length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            typeList[i].getSQL(builder, sqlFlags);
        }
        return builder.append(')');
    }

    @Override
    public Expression optimize(SessionLocal session) {
        int count = typeList.length;
        valueTypes = new int[count];
        for (int i = 0; i < count; i++) {
            valueTypes[i] = typeList[i].getValueType();
        }
        Arrays.sort(valueTypes);
        return super.optimize(session);
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value l = left.getValue(session);
        if (l == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(Arrays.binarySearch(valueTypes, l.getValueType()) >= 0 ^ not);
    }

    @Override
    public boolean getWhenValue(SessionLocal session, Value left) {
        if (!whenOperand) {
            return super.getWhenValue(session, left);
        }
        if (left == ValueNull.INSTANCE) {
            return false;
        }
        return Arrays.binarySearch(valueTypes, left.getValueType()) >= 0 ^ not;
    }

    @Override
    public Expression getNotIfPossible(SessionLocal session) {
        if (whenOperand) {
            return null;
        }
        return new TypePredicate(left, !not, false, typeList);
    }

}
