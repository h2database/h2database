/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import java.util.ArrayList;
import java.util.TreeSet;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.index.IndexCondition;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * Used for optimised IN(...) queries where the contents of the IN list are all
 * constant and of the same type.
 */
public final class ConditionInConstantSet extends Condition {

    private Expression left;
    private final boolean not;
    private final boolean whenOperand;
    private final ArrayList<Expression> valueList;
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
        this.left = left;
        this.not = not;
        this.whenOperand = whenOperand;
        this.valueList = valueList;
        this.valueSet = new TreeSet<>(session.getDatabase().getCompareMode());
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
    public Value getValue(SessionLocal session) {
        return getValue(left.getValue(session), session);
    }

    @Override
    public boolean getWhenValue(SessionLocal session, Value left) {
        if (!whenOperand) {
            return super.getWhenValue(session, left);
        }
        return getValue(left, session).isTrue();
    }

    private Value getValue(Value left, SessionLocal session) {
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
        return this;
    }

    @Override
    public Expression getNotIfPossible(SessionLocal session) {
        if (whenOperand) {
            return null;
        }
        return new ConditionInConstantSet(session, left, !not, false, valueList);
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
        if (session.getDatabase().getSettings().optimizeInList) {
            TypeInfo colType = l.getType();
            if (TypeInfo.haveSameOrdering(colType, TypeInfo.getHigherType(colType, type))) {
                filter.addIndexCondition(IndexCondition.getInList(l, valueList));
            }
        }
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
