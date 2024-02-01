/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
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
import org.h2.expression.Parameter;
import org.h2.expression.TypedValueExpression;
import org.h2.expression.ValueExpression;
import org.h2.index.IndexCondition;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;

/**
 * An 'in' condition with a list of values, as in WHERE NAME IN(...)
 */
public final class ConditionIn extends Condition {

    private Expression left;
    private final boolean not;
    private final boolean whenOperand;
    private final ArrayList<Expression> valueList;

    /**
     * Create a new IN(..) condition.
     *
     * @param left the expression before IN
     * @param not whether the result should be negated
     * @param whenOperand whether this is a when operand
     * @param values the value list (at least one element)
     */
    public ConditionIn(Expression left, boolean not, boolean whenOperand, ArrayList<Expression> values) {
        this.left = left;
        this.not = not;
        this.whenOperand = whenOperand;
        this.valueList = values;
    }

    @Override
    public Value getValue(SessionLocal session) {
        return getValue(session, left.getValue(session));
    }

    @Override
    public boolean getWhenValue(SessionLocal session, Value left) {
        if (!whenOperand) {
            return super.getWhenValue(session, left);
        }
        return getValue(session, left).isTrue();
    }

    private Value getValue(SessionLocal session, Value left) {
        if (left.containsNull()) {
            return ValueNull.INSTANCE;
        }
        boolean hasNull = false;
        for (Expression e : valueList) {
            Value r = e.getValue(session);
            Value cmp = Comparison.compare(session, left, r, Comparison.EQUAL);
            if (cmp == ValueNull.INSTANCE) {
                hasNull = true;
            } else if (cmp == ValueBoolean.TRUE) {
                return ValueBoolean.get(!not);
            }
        }
        if (hasNull) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(not);
    }

    @Override
    public boolean isWhenConditionOperand() {
        return whenOperand;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
        for (Expression e : valueList) {
            e.mapColumns(resolver, level, state);
        }
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        boolean constant = !whenOperand && left.isConstant();
        if (constant && left.isNullConstant()) {
            return TypedValueExpression.UNKNOWN;
        }
        boolean allValuesConstant = true;
        boolean allValuesNull = true;
        TypeInfo leftType = left.getType();
        for (int i = 0, l = valueList.size(); i < l; i++) {
            Expression e = valueList.get(i);
            e = e.optimize(session);
            TypeInfo.checkComparable(leftType, e.getType());
            if (e.isConstant() && !e.getValue(session).containsNull()) {
                allValuesNull = false;
            }
            if (allValuesConstant && !e.isConstant()) {
                allValuesConstant = false;
            }
            if (left instanceof ExpressionColumn && e instanceof Parameter) {
                ((Parameter) e).setColumn(((ExpressionColumn) left).getColumn());
            }
            valueList.set(i, e);
        }
        return optimize2(session, constant, allValuesConstant, allValuesNull, valueList);
    }

    private Expression optimize2(SessionLocal session, boolean constant, boolean allValuesConstant,
            boolean allValuesNull, ArrayList<Expression> values) {
        if (constant && allValuesConstant) {
            return ValueExpression.getBoolean(getValue(session));
        }
        if (values.size() == 1) {
            return new Comparison(not ? Comparison.NOT_EQUAL : Comparison.EQUAL, left, values.get(0), whenOperand)
                    .optimize(session);
        }
        if (allValuesConstant && !allValuesNull) {
            int leftType = left.getType().getValueType();
            if (leftType == Value.UNKNOWN) {
                return this;
            }
            if (leftType == Value.ENUM && !(left instanceof ExpressionColumn)) {
                return this;
            }
            return new ConditionInConstantSet(session, left, not, whenOperand, values).optimize(session);
        }
        return this;
    }

    @Override
    public Expression getNotIfPossible(SessionLocal session) {
        if (whenOperand) {
            return null;
        }
        return new ConditionIn(left, !not, false, valueList);
    }

    @Override
    public void createIndexConditions(SessionLocal session, TableFilter filter) {
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
                createCompoundIndexCondition(filter);
                // If there is no compound index, then the TableFilter#prepare() method will drop this condition.
                // Then we create a unique index condition for each column.
                createUniqueIndexConditions(filter, list);
                // If there are two or more index conditions, IndexCursor will only use the first one.
                // See: IndexCursor#canUseIndexForIn(Column)
            }
        }
    }

    /**
     * Creates a compound index condition containing every item in the expression list.
     * @see IndexCondition#getCompoundInList(ExpressionList, List)
     */
    private void createCompoundIndexCondition(TableFilter filter) {
        // We do not check filter here, because the IN condition can contain columns from multiple tables.
        ExpressionVisitor visitor = ExpressionVisitor.getNotFromResolverVisitor(filter);
        for (Expression e : valueList) {
            if (!e.isEverything(visitor)) {
                return;
            }
        }
        filter.addIndexCondition(IndexCondition.getCompoundInList((ExpressionList) left, valueList));
    }

    /**
     * Creates a unique index condition for every item in the expression list.
     * @see IndexCondition#getInList(ExpressionColumn, List)
     */
    private void createUniqueIndexConditions(TableFilter filter, ExpressionList list) {
        int c = list.getSubexpressionCount();
        for (int i = 0; i < c; i++) {
            Expression e = list.getSubexpression(i);
            if (e instanceof ExpressionColumn) {
                ExpressionColumn l = (ExpressionColumn) e;
                if (filter == l.getTableFilter()) {
                    ArrayList<Expression> subList = new ArrayList<>(valueList.size());
                    for (Expression row : valueList) {
                        if (row instanceof ExpressionList) {
                            ExpressionList r = (ExpressionList) row;
                            if (r.isArray() || r.getSubexpressionCount() != c) {
                                return;
                            }
                            subList.add(r.getSubexpression(i));
                        } else if (row instanceof ValueExpression) {
                            Value v = row.getValue(null);
                            if (v.getValueType() != Value.ROW) {
                                return;
                            }
                            Value[] values = ((ValueRow) v).getList();
                            if (c != values.length) {
                                return;
                            }
                            subList.add(ValueExpression.get(values[i]));
                        } else {
                            return;
                        }
                    }
                    createIndexConditions(filter, l, subList);
                }
            }
        }
    }

    private static void createIndexConditions(TableFilter filter, ExpressionColumn l, //
            ArrayList<Expression> valueList) {
        ExpressionVisitor visitor = ExpressionVisitor.getNotFromResolverVisitor(filter);
        TypeInfo colType = l.getType();
        for (Expression e : valueList) {
            if (!e.isEverything(visitor)
                    || !TypeInfo.haveSameOrdering(colType, TypeInfo.getHigherType(colType, e.getType()))) {
                return;
            }
        }
        filter.addIndexCondition(IndexCondition.getInList(l, valueList));
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        for (Expression e : valueList) {
            e.setEvaluatable(tableFilter, b);
        }
    }

    @Override
    public boolean needParentheses() {
        return true;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return getWhenSQL(left.getSQL(builder, sqlFlags, AUTO_PARENTHESES), sqlFlags);
    }

    @Override
    public StringBuilder getWhenSQL(StringBuilder builder, int sqlFlags) {
        if (not) {
            builder.append(" NOT");
        }
        return writeExpressions(builder.append(" IN("), valueList, sqlFlags).append(')');
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        left.updateAggregate(session, stage);
        for (Expression e : valueList) {
            e.updateAggregate(session, stage);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (!left.isEverything(visitor)) {
            return false;
        }
        return areAllValues(visitor);
    }

    private boolean areAllValues(ExpressionVisitor visitor) {
        for (Expression e : valueList) {
            if (!e.isEverything(visitor)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int getCost() {
        int cost = left.getCost();
        for (Expression e : valueList) {
            cost += e.getCost();
        }
        return cost;
    }

    /**
     * Add an additional element if possible. Example: given two conditions
     * A IN(1, 2) OR A=3, the constant 3 is added: A IN(1, 2, 3).
     *
     * @param other the second condition
     * @return null if the condition was not added, or the new condition
     */
    Expression getAdditional(Comparison other) {
        if (!not && !whenOperand && left.isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR)) {
            Expression add = other.getIfEquals(left);
            if (add != null) {
                ArrayList<Expression> list = new ArrayList<>(valueList.size() + 1);
                list.addAll(valueList);
                list.add(add);
                return new ConditionIn(left, false, false, list);
            }
        }
        return null;
    }

    @Override
    public int getSubexpressionCount() {
        return 1 + valueList.size();
    }

    @Override
    public Expression getSubexpression(int index) {
        if (index == 0) {
            return left;
        } else if (index > 0 && index <= valueList.size()) {
            return valueList.get(index - 1);
        }
        throw new IndexOutOfBoundsException();
    }

}
