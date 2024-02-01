/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import java.util.ArrayList;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionList;
import org.h2.expression.ValueExpression;
import org.h2.index.IndexCondition;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;

/**
 * Null predicate (IS [NOT] NULL).
 */
public final class NullPredicate extends SimplePredicate {

    private boolean optimized;

    public NullPredicate(Expression left, boolean not, boolean whenOperand) {
        super(left, not, whenOperand);
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return getWhenSQL(left.getSQL(builder, sqlFlags, AUTO_PARENTHESES), sqlFlags);
    }

    @Override
    public StringBuilder getWhenSQL(StringBuilder builder, int sqlFlags) {
        return builder.append(not ? " IS NOT NULL" : " IS NULL");
    }

    @Override
    public Expression optimize(SessionLocal session) {
        if (optimized) {
            return this;
        }
        Expression o = super.optimize(session);
        if (o != this) {
            return o;
        }
        optimized = true;
        if (!whenOperand && left instanceof ExpressionList) {
            ExpressionList list = (ExpressionList) left;
            if (!list.isArray()) {
                for (int i = 0, count = list.getSubexpressionCount(); i < count; i++) {
                    if (list.getSubexpression(i).isNullConstant()) {
                        if (not) {
                            return ValueExpression.FALSE;
                        }
                        ArrayList<Expression> newList = new ArrayList<>(count - 1);
                        for (int j = 0; j < i; j++) {
                            newList.add(list.getSubexpression(j));
                        }
                        for (int j = i + 1; j < count; j++) {
                            Expression e = list.getSubexpression(j);
                            if (!e.isNullConstant()) {
                                newList.add(e);
                            }
                        }
                        left = newList.size() == 1 ? newList.get(0) //
                                : new ExpressionList(newList.toArray(new Expression[0]), false);
                        break;
                    }
                }
            }
        }
        return this;
    }

    @Override
    public Value getValue(SessionLocal session) {
        return ValueBoolean.get(getValue(left.getValue(session)));
    }

    @Override
    public boolean getWhenValue(SessionLocal session, Value left) {
        if (!whenOperand) {
            return super.getWhenValue(session, left);
        }
        return getValue(left);
    }

    private boolean getValue(Value left) {
        if (left.getType().getValueType() == Value.ROW) {
            for (Value v : ((ValueRow) left).getList()) {
                if (v != ValueNull.INSTANCE ^ not) {
                    return false;
                }
            }
            return true;
        }
        return left == ValueNull.INSTANCE ^ not;
    }

    @Override
    public Expression getNotIfPossible(SessionLocal session) {
        if (whenOperand) {
            return null;
        }
        Expression o = optimize(session);
        if (o != this) {
            return o.getNotIfPossible(session);
        }
        switch (left.getType().getValueType()) {
        case Value.UNKNOWN:
        case Value.ROW:
            return null;
        }
        return new NullPredicate(left, !not, false);
    }

    @Override
    public void createIndexConditions(SessionLocal session, TableFilter filter) {
        if (not || whenOperand || !filter.getTable().isQueryComparable()) {
            return;
        }
        if (left instanceof ExpressionColumn) {
            createNullIndexCondition(filter, (ExpressionColumn) left);
        } else if (left instanceof ExpressionList) {
            ExpressionList list = (ExpressionList) left;
            if (!list.isArray()) {
                for (int i = 0, count = list.getSubexpressionCount(); i < count; i++) {
                    Expression e = list.getSubexpression(i);
                    if (e instanceof ExpressionColumn) {
                        createNullIndexCondition(filter, (ExpressionColumn) e);
                    }
                }
            }
        }
    }

    private static void createNullIndexCondition(TableFilter filter, ExpressionColumn c) {
        /*
         * Columns with row value data type aren't valid, but perform such check
         * to be sure.
         */
        if (filter == c.getTableFilter() && c.getType().getValueType() != Value.ROW) {
            filter.addIndexCondition(IndexCondition.get(Comparison.EQUAL_NULL_SAFE, c, ValueExpression.NULL));
        }
    }

}
