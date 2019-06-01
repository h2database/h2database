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
import org.h2.expression.TypedValueExpression;
import org.h2.expression.ValueExpression;
import org.h2.index.IndexCondition;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * Boolean test (IS [NOT] { TRUE | FALSE | UNKNOWN }).
 */
public class BooleanTest extends SimplePredicate {

    private final Boolean right;

    public BooleanTest(Expression left, boolean not, Boolean right) {
        super(left, not);
        this.right = right;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        return left.getSQL(builder.append('('), alwaysQuote).append(not ? " IS NOT " : " IS ")
                .append(right == null ? "UNKNOWN)" : right ? "TRUE)" : "FALSE)");
    }

    @Override
    public Value getValue(Session session) {
        Value l = left.getValue(session);
        return ValueBoolean
                .get((l == ValueNull.INSTANCE ? right == null : right != null && right == l.getBoolean()) ^ not);
    }

    @Override
    public Expression getNotIfPossible(Session session) {
        return new BooleanTest(left, !not, right);
    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
        if (!filter.getTable().isQueryComparable()) {
            return;
        }
        if (left instanceof ExpressionColumn) {
            ExpressionColumn c = (ExpressionColumn) left;
            if (c.getType().getValueType() == Value.BOOLEAN && filter == c.getTableFilter()) {
                if (not) {
                    if (right == null && c.getColumn().isNullable()) {
                        ArrayList<Expression> list = new ArrayList<>(2);
                        list.add(ValueExpression.getBoolean(false));
                        list.add(ValueExpression.getBoolean(true));
                        filter.addIndexCondition(IndexCondition.getInList(c, list));
                    }
                } else {
                    filter.addIndexCondition(IndexCondition.get(Comparison.EQUAL_NULL_SAFE, c,
                            right == null ? TypedValueExpression.getUnknown() : ValueExpression.getBoolean(right)));
                }
            }
        }
    }

}
