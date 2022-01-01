/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import java.util.AbstractList;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.TypedValueExpression;
import org.h2.expression.ValueExpression;
import org.h2.index.IndexCondition;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * A condition with parameter as {@code = ANY(?)}.
 */
public final class ConditionInParameter extends Condition {
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

    private boolean not;

    private boolean whenOperand;

    private final Parameter parameter;

    /**
     * Gets evaluated condition value.
     *
     * @param session the session
     * @param l left value.
     * @param not whether the result should be negated
     * @param value parameter value.
     * @return Evaluated condition value.
     */
    static Value getValue(SessionLocal session, Value l, boolean not, Value value) {
        boolean hasNull = false;
        if (value.containsNull()) {
            hasNull = true;
        } else {
            for (Value r : value.convertToAnyArray(session).getList()) {
                Value cmp = Comparison.compare(session, l, r, Comparison.EQUAL);
                if (cmp == ValueNull.INSTANCE) {
                    hasNull = true;
                } else if (cmp == ValueBoolean.TRUE) {
                    return ValueBoolean.get(!not);
                }
            }
        }
        if (hasNull) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(not);
    }

    /**
     * Create a new {@code = ANY(?)} condition.
     *
     * @param left
     *            the expression before {@code = ANY(?)}
     * @param not whether the result should be negated
     * @param whenOperand whether this is a when operand
     * @param parameter
     *            parameter
     */
    public ConditionInParameter(Expression left, boolean not, boolean whenOperand, Parameter parameter) {
        this.left = left;
        this.not = not;
        this.whenOperand = whenOperand;
        this.parameter = parameter;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value l = left.getValue(session);
        if (l == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        return getValue(session, l, not, parameter.getValue(session));
    }

    @Override
    public boolean getWhenValue(SessionLocal session, Value left) {
        if (!whenOperand) {
            return super.getWhenValue(session, left);
        }
        if (left == ValueNull.INSTANCE) {
            return false;
        }
        return getValue(session, left, not, parameter.getValue(session)).isTrue();
    }

    @Override
    public boolean isWhenConditionOperand() {
        return whenOperand;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        if (!whenOperand && left.isNullConstant()) {
            return TypedValueExpression.UNKNOWN;
        }
        return this;
    }

    @Override
    public Expression getNotIfPossible(SessionLocal session) {
        if (whenOperand) {
            return null;
        }
        return new ConditionInParameter(left, !not, false, parameter);
    }

    @Override
    public void createIndexConditions(SessionLocal session, TableFilter filter) {
        if (not || whenOperand || !(left instanceof ExpressionColumn)) {
            return;
        }
        ExpressionColumn l = (ExpressionColumn) left;
        if (filter != l.getTableFilter()) {
            return;
        }
        filter.addIndexCondition(IndexCondition.getInList(l, new ParameterList(parameter)));
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
    }

    @Override
    public boolean needParentheses() {
        return true;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        if (not) {
            builder.append("NOT (");
        }
        left.getSQL(builder, sqlFlags, AUTO_PARENTHESES);
        parameter.getSQL(builder.append(" = ANY("), sqlFlags, AUTO_PARENTHESES).append(')');
        if (not) {
            builder.append(')');
        }
        return builder;
    }

    @Override
    public StringBuilder getWhenSQL(StringBuilder builder, int sqlFlags) {
        if (not) {
            builder.append(" NOT IN(UNNEST(");
            parameter.getSQL(builder, sqlFlags, AUTO_PARENTHESES).append("))");
        } else {
            builder.append(" = ANY(");
            parameter.getSQL(builder, sqlFlags, AUTO_PARENTHESES).append(')');
        }
        return builder;
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        left.updateAggregate(session, stage);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && parameter.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return left.getCost();
    }

}
