/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import java.util.AbstractList;
import java.util.Arrays;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.ValueExpression;
import org.h2.index.IndexCondition;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * Quantified comparison predicate with array.
 */
public class ConditionInArray extends Condition {

    private static final class ParameterList extends AbstractList<Expression> {
        private final Parameter parameter;

        ParameterList(Parameter parameter) {
            this.parameter = parameter;
        }

        @Override
        public Expression get(int index) {
            Value value = parameter.getParamValue();
            if (value instanceof ValueArray) {
                return ValueExpression.get(((ValueArray) value).getList()[index]);
            }
            if (index != 0) {
                throw new IndexOutOfBoundsException();
            }
            return ValueExpression.get(value);
        }

        @Override
        public int size() {
            if (!parameter.isValueSet()) {
                return 0;
            }
            Value value = parameter.getParamValue();
            if (value instanceof ValueArray) {
                return ((ValueArray) value).getList().length;
            }
            return 1;
        }
    }

    private Expression left;
    private final boolean whenOperand;
    private Expression right;
    private final boolean all;
    private final int compareType;

    public ConditionInArray(Expression left, boolean whenOperand, Expression right, boolean all, int compareType) {
        this.left = left;
        this.whenOperand = whenOperand;
        this.right = right;
        this.all = all;
        this.compareType = compareType;
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
        Value r = right.getValue(session);
        if (r == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        Value[] array = r.convertToAnyArray(session).getList();
        if (array.length == 0) {
            return ValueBoolean.get(all);
        }
        if ((compareType & ~1) == Comparison.EQUAL_NULL_SAFE) {
            return getNullSafeValueSlow(session, array, left);
        }
        if (left.containsNull()) {
            return ValueNull.INSTANCE;
        }
        return getValueSlow(session, array, left);
    }

    private Value getValueSlow(SessionLocal session, Value[] array, Value l) {
        // this only returns the correct result if the array has at least one
        // element, and if l is not null
        boolean hasNull = false;
        ValueBoolean searched = ValueBoolean.get(!all);
        for (Value v : array) {
            Value cmp = Comparison.compare(session, l, v, compareType);
            if (cmp == ValueNull.INSTANCE) {
                hasNull = true;
            } else if (cmp == searched) {
                return ValueBoolean.get(!all);
            }
        }
        if (hasNull) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(all);
    }

    private Value getNullSafeValueSlow(SessionLocal session, Value[] array, Value l) {
        boolean searched = all == (compareType == Comparison.NOT_EQUAL_NULL_SAFE);
        for (Value v : array) {
            if (session.areEqual(l, v) == searched) {
                return ValueBoolean.get(!all);
            }
        }
        return ValueBoolean.get(all);
    }

    @Override
    public boolean isWhenConditionOperand() {
        return whenOperand;
    }

    @Override
    public Expression getNotIfPossible(SessionLocal session) {
        if (whenOperand) {
            return null;
        }
        return new ConditionInArray(left, false, right, !all, Comparison.getNotCompareType(compareType));
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
        right.mapColumns(resolver, level, state);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        right = right.optimize(session);
        left = left.optimize(session);
        if (!whenOperand && left.isConstant() && right.isConstant()) {
            return ValueExpression.getBoolean(getValue(session));
        }
        return this;
    }

    @Override
    public void createIndexConditions(SessionLocal session, TableFilter filter) {
        if (whenOperand || all || compareType != Comparison.EQUAL || !(left instanceof ExpressionColumn)) {
            return;
        }
        ExpressionColumn l = (ExpressionColumn) left;
        if (filter != l.getTableFilter()) {
            return;
        }
        if (right instanceof Parameter) {
            filter.addIndexCondition(IndexCondition.getInList(l, new ParameterList((Parameter) right)));
        } else if (right.isConstant()) {
            Value r = right.getValue(null);
            if (r instanceof ValueArray) {
                Value[] values = ((ValueArray) r).getList();
                int count = values.length;
                if (count == 0) {
                    filter.addIndexCondition(IndexCondition.get(Comparison.FALSE, l, ValueExpression.FALSE));
                } else {
                    TypeInfo colType = l.getType(), type = colType;
                    for (int i = 0; i < count; i++) {
                        type = TypeInfo.getHigherType(type, values[i].getType());
                    }
                    if (TypeInfo.haveSameOrdering(colType, type)) {
                        Expression[] valueList = new Expression[count];
                        for (int i = 0; i < count; i++) {
                            valueList[i] = ValueExpression.get(values[i]);
                        }
                        filter.addIndexCondition(IndexCondition.getInList(l, Arrays.asList(valueList)));
                    }
                }
            }
        } else {
            ExpressionVisitor visitor = ExpressionVisitor.getNotFromResolverVisitor(filter);
            if (right.isEverything(visitor)) {
                TypeInfo arrayType = right.getType();
                if (arrayType.getValueType() == Value.ARRAY) {
                    TypeInfo colType = l.getType();
                    if (TypeInfo.haveSameOrdering(colType,
                            TypeInfo.getHigherType(colType, (TypeInfo) arrayType.getExtTypeInfo()))) {
                        filter.addIndexCondition(IndexCondition.getInArray(l, right));
                    }
                }
            }
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        left.setEvaluatable(tableFilter, value);
        right.setEvaluatable(tableFilter, value);
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
        return right.getSQL(
                builder.append(' ').append(Comparison.COMPARE_TYPES[compareType]).append(all ? " ALL(" : " ANY("),
                sqlFlags).append(')');
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        left.updateAggregate(session, stage);
        right.updateAggregate(session, stage);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && right.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return left.getCost() + right.getCost() + 10;
    }

}
