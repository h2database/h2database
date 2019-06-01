/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import java.util.ArrayList;

import org.h2.engine.Session;
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
public class NullPredicate extends SimplePredicate {

    private boolean optimized;

    public NullPredicate(Expression left, boolean not) {
        super(left, not);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        return left.getSQL(builder.append('('), alwaysQuote).append(not ? " IS NOT NULL)" : " IS NULL)");
    }

    @Override
    public Expression optimize(Session session) {
        if (optimized) {
            return this;
        }
        Expression o = super.optimize(session);
        if (o != this) {
            return o;
        }
        optimized = true;
        if (left instanceof ExpressionList) {
            ExpressionList list = (ExpressionList) left;
            if (list.getType().getValueType() == Value.ROW) {
                for (int i = 0, count = list.getSubexpressionCount(); i < count; i++) {
                    if (list.getSubexpression(i).isNullConstant()) {
                        if (not) {
                            return ValueExpression.getBoolean(false);
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
    public Value getValue(Session session) {
        Value l = left.getValue(session);
        if (l.getType().getValueType() == Value.ROW) {
            for (Value v : ((ValueRow) l).getList()) {
                if (v != ValueNull.INSTANCE ^ not) {
                    return ValueBoolean.FALSE;
                }
            }
            return ValueBoolean.TRUE;
        }
        return ValueBoolean.get(l == ValueNull.INSTANCE ^ not);
    }

    @Override
    public Expression getNotIfPossible(Session session) {
        Expression o = optimize(session);
        if (o != this) {
            return o.getNotIfPossible(session);
        }
        switch (left.getType().getValueType()) {
        case Value.UNKNOWN:
        case Value.ROW:
            return null;
        }
        return new NullPredicate(left, !not);
    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
        if (not || !filter.getTable().isQueryComparable()) {
            return;
        }
        if (left instanceof ExpressionColumn) {
            createNullIndexCondition(filter, (ExpressionColumn) left);
        } else if (left instanceof ExpressionList) {
            ExpressionList list = (ExpressionList) left;
            if (list.getType().getValueType() == Value.ROW) {
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
            filter.addIndexCondition(IndexCondition.get(Comparison.EQUAL_NULL_SAFE, c, ValueExpression.getNull()));
        }
    }

}
