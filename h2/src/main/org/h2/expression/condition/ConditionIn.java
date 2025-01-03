/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import java.util.ArrayList;
import java.util.List;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionList;
import org.h2.expression.ExpressionVisitor;
import org.h2.index.IndexCondition;
import org.h2.table.Column;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * Abstract IN predicate with a list of values.
 */
abstract class ConditionIn extends Condition {

    Expression left;
    final boolean not;
    final boolean whenOperand;
    final ArrayList<Expression> valueList;

    /**
     * Create a new IN predicate.
     *
     * @param left
     *            the expression before IN
     * @param not
     *            whether the result should be negated
     * @param whenOperand
     *            whether this is a when operand
     * @param valueList
     *            the value list (at least one element)
     */
    ConditionIn(Expression left, boolean not, boolean whenOperand, ArrayList<Expression> valueList) {
        this.left = left;
        this.not = not;
        this.whenOperand = whenOperand;
        this.valueList = valueList;
    }

    @Override
    public final Value getValue(SessionLocal session) {
        return getValue(session, left.getValue(session));
    }

    @Override
    public final boolean getWhenValue(SessionLocal session, Value left) {
        if (!whenOperand) {
            return super.getWhenValue(session, left);
        }
        return getValue(session, left).isTrue();
    }

    abstract Value getValue(SessionLocal session, Value left);

    @Override
    public final boolean isWhenConditionOperand() {
        return whenOperand;
    }

    @Override
    public final void createIndexConditions(SessionLocal session, TableFilter filter) {
        if (not || whenOperand || !session.getDatabase().getSettings().optimizeInList) {
            return;
        }
        if (left instanceof ExpressionColumn) {
            ExpressionColumn l = (ExpressionColumn) left;
            if (filter == l.getTableFilter()) {
                createIndexConditions(filter, l, valueList);
            }
        } else if (left instanceof ExpressionList) {
            ExpressionList list = (ExpressionList) left;
            if (!list.isArray()) {
                // First we create a compound index condition.
                createCompoundIndexCondition(filter, list);
                // If there is no compound index, then the TableFilter#prepare()
                // method will drop this condition.
                // Then we create a unique index condition for each column.
                createUniqueIndexConditions(filter, list);
                // If there are two or more index conditions, IndexCursor will
                // only use the first one.
                // See: IndexCursor#canUseIndexForIn(Column)
            }
        }
    }

    /**
     * Creates a compound index condition containing every item in the
     * expression list.
     *
     * @param filter
     *            the table filter
     * @param list
     *            list of expressions
     *
     * @see IndexCondition#getCompoundInList(Column[], List)
     */
    private void createCompoundIndexCondition(TableFilter filter, ExpressionList list) {
        int c = list.getSubexpressionCount();
        Column[] columns = new Column[c];
        for (int i = 0; i < c; i++) {
            Expression e = list.getSubexpression(i);
            if (!(e instanceof ExpressionColumn)) {
                return;
            }
            ExpressionColumn l = (ExpressionColumn) e;
            if (filter != l.getTableFilter()) {
                return;
            }
            columns[i] = l.getColumn();
        }
        TypeInfo colType = left.getType();
        ExpressionVisitor visitor = ExpressionVisitor.getNotFromResolverVisitor(filter);
        for (Expression e : valueList) {
            if (!e.isEverything(visitor)
                    || !TypeInfo.haveSameOrdering(colType, TypeInfo.getHigherType(colType, e.getType()))) {
                return;
            }
        }
        filter.addIndexCondition(IndexCondition.getCompoundInList(columns, valueList));
    }

    abstract void createUniqueIndexConditions(TableFilter filter, ExpressionList list);

    abstract void createIndexConditions(TableFilter filter, ExpressionColumn l, ArrayList<Expression> valueList);

    @Override
    public final boolean needParentheses() {
        return true;
    }

    @Override
    public final StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return getWhenSQL(left.getSQL(builder, sqlFlags, AUTO_PARENTHESES), sqlFlags);
    }

    @Override
    public final StringBuilder getWhenSQL(StringBuilder builder, int sqlFlags) {
        if (not) {
            builder.append(" NOT");
        }
        return writeExpressions(builder.append(" IN("), valueList, sqlFlags).append(')');
    }

    @Override
    public final int getSubexpressionCount() {
        return 1 + valueList.size();
    }

    @Override
    public final Expression getSubexpression(int index) {
        if (index == 0) {
            return left;
        } else if (index > 0 && index <= valueList.size()) {
            return valueList.get(index - 1);
        }
        throw new IndexOutOfBoundsException();
    }

}
