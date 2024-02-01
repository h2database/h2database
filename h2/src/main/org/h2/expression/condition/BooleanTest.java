/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import java.util.List;

import org.h2.engine.SessionLocal;
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
public final class BooleanTest extends SimplePredicate {

    private final Boolean right;

    public BooleanTest(Expression left, boolean not, boolean whenOperand, Boolean right) {
        super(left, not, whenOperand);
        this.right = right;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return getWhenSQL(left.getSQL(builder, sqlFlags, AUTO_PARENTHESES), sqlFlags);
    }

    @Override
    public StringBuilder getWhenSQL(StringBuilder builder, int sqlFlags) {
        return builder.append(not ? " IS NOT " : " IS ").append(right == null ? "UNKNOWN" : right ? "TRUE" : "FALSE");
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
        return (left == ValueNull.INSTANCE ? right == null : right != null && right == left.getBoolean()) ^ not;
    }

    @Override
    public Expression getNotIfPossible(SessionLocal session) {
        if (whenOperand) {
            return null;
        }
        return new BooleanTest(left, !not, false, right);
    }

    @Override
    public void createIndexConditions(SessionLocal session, TableFilter filter) {
        if (whenOperand || !filter.getTable().isQueryComparable()) {
            return;
        }
        if (left instanceof ExpressionColumn) {
            ExpressionColumn c = (ExpressionColumn) left;
            if (c.getType().getValueType() == Value.BOOLEAN && filter == c.getTableFilter()) {
                if (not) {
                    if (right == null && c.getColumn().isNullable()) {
                        filter.addIndexCondition(
                                IndexCondition.getInList(c, List.of(ValueExpression.FALSE, ValueExpression.TRUE)));
                    }
                } else {
                    filter.addIndexCondition(IndexCondition.get(Comparison.EQUAL_NULL_SAFE, c,
                            right == null ? TypedValueExpression.UNKNOWN : ValueExpression.getBoolean(right)));
                }
            }
        }
    }

}
