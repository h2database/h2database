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
public final class ConditionInList extends ConditionIn {

    /**
     * Create a new IN(..) condition.
     *
     * @param left the expression before IN
     * @param not whether the result should be negated
     * @param whenOperand whether this is a when operand
     * @param valueList the value list (at least one element)
     */
    public ConditionInList(Expression left, boolean not, boolean whenOperand, ArrayList<Expression> valueList) {
        super(left, not, whenOperand, valueList);
    }

    @Override
    Value getValue(SessionLocal session, Value left) {
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
        return new ConditionInList(left, !not, false, valueList);
    }

    /**
     * Creates a unique index condition for every item in the expression list.
     * @see IndexCondition#getInList(ExpressionColumn, List)
     */
    @Override
    void createUniqueIndexConditions(TableFilter filter, ExpressionList list) {
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

    @Override
    void createIndexConditions(TableFilter filter, ExpressionColumn l, ArrayList<Expression> valueList) {
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
                return new ConditionInList(left, false, false, list);
            }
        }
        return null;
    }

}
