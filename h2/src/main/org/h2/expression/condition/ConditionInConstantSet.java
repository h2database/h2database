/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionList;
import org.h2.expression.ExpressionVisitor;
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
 * Used for optimised IN(...) queries where the contents of the IN list are all
 * constant and of the same type.
 */
public final class ConditionInConstantSet extends ConditionIn {

    // HashSet cannot be used here, because we need to compare values of
    // different type or scale properly.
    private final TreeSet<Value> valueSet;
    private boolean hasNull;
    private final TypeInfo type;

    /**
     * Create a new IN(..) condition.
     *
     * @param session the session
     * @param left
     *            the expression before IN. Cannot have {@link Value#UNKNOWN}
     *            data type and {@link Value#ENUM} type is also supported only
     *            for {@link ExpressionColumn}.
     * @param not whether the result should be negated
     * @param whenOperand whether this is a when operand
     * @param valueList
     *            the value list (at least two elements); all values must be
     *            comparable with left value
     */
    ConditionInConstantSet(SessionLocal session, Expression left, boolean not, boolean whenOperand,
            ArrayList<Expression> valueList) {
        super(left, not, whenOperand, valueList);
        this.valueSet = new TreeSet<>(session);
        TypeInfo type = left.getType();
        for (Expression expression : valueList) {
            type = TypeInfo.getHigherType(type, expression.getType());
        }
        this.type = type;
        for (Expression expression : valueList) {
            add(expression.getValue(session), session);
        }
    }

    private void add(Value v, SessionLocal session) {
        if ((v = v.convertTo(type, session)).containsNull()) {
            hasNull = true;
        } else {
            valueSet.add(v);
        }
    }

    @Override
    Value getValue(SessionLocal session, Value left) {
        if ((left = left.convertTo(type, session)).containsNull()) {
            return ValueNull.INSTANCE;
        }
        boolean result = valueSet.contains(left);
        if (!result && hasNull) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(not ^ result);
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        return this;
    }

    @Override
    public Expression getNotIfPossible(SessionLocal session) {
        if (whenOperand) {
            return null;
        }
        return new ConditionInConstantSet(session, left, !not, false, valueList);
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
                    TypeInfo type = l.getType();
                    for (Expression expression : subList) {
                        type = TypeInfo.getHigherType(type, expression.getType());
                    }
                    createIndexConditions(filter, l, subList, type);
                }
            }
        }
    }

    @Override
    void createIndexConditions(TableFilter filter, ExpressionColumn l, ArrayList<Expression> valueList) {
        createIndexConditions(filter, l, valueList, type);
    }

    private static void createIndexConditions(TableFilter filter, ExpressionColumn l, ArrayList<Expression> valueList,
            TypeInfo type) {
        TypeInfo colType = l.getType();
        if (TypeInfo.haveSameOrdering(colType, TypeInfo.getHigherType(colType, type))) {
            filter.addIndexCondition(IndexCondition.getInList(l, valueList));
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        left.updateAggregate(session, stage);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return left.getCost();
    }

    /**
     * Add an additional element if possible. Example: given two conditions
     * A IN(1, 2) OR A=3, the constant 3 is added: A IN(1, 2, 3).
     *
     * @param session the session
     * @param other the second condition
     * @return null if the condition was not added, or the new condition
     */
    Expression getAdditional(SessionLocal session, Comparison other) {
        if (!not && !whenOperand && left.isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR)) {
            Expression add = other.getIfEquals(left);
            if (add != null) {
                if (add.isConstant()) {
                    ArrayList<Expression> list = new ArrayList<>(valueList.size() + 1);
                    list.addAll(valueList);
                    list.add(add);
                    return new ConditionInConstantSet(session, left, false, false, list);
                }
            }
        }
        return null;
    }

}
