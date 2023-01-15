/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.TypedValueExpression;
import org.h2.expression.ValueExpression;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * BETWEEN predicate.
 */
public final class BetweenPredicate extends Condition {

    private Expression left;

    private final boolean not;

    private final boolean whenOperand;

    private boolean symmetric;

    private Expression a, b;

    public BetweenPredicate(Expression left, boolean not, boolean whenOperand, boolean symmetric, Expression a,
            Expression b) {
        this.left = left;
        this.not = not;
        this.whenOperand = whenOperand;
        this.symmetric = symmetric;
        this.a = a;
        this.b = b;
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
        builder.append(" BETWEEN ");
        if (symmetric) {
            builder.append("SYMMETRIC ");
        }
        a.getSQL(builder, sqlFlags, AUTO_PARENTHESES).append(" AND ");
        return b.getSQL(builder, sqlFlags, AUTO_PARENTHESES);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        left = left.optimize(session);
        a = a.optimize(session);
        b = b.optimize(session);
        TypeInfo leftType = left.getType();
        TypeInfo.checkComparable(leftType, a.getType());
        TypeInfo.checkComparable(leftType, b.getType());
        if (whenOperand) {
            return this;
        }
        Value value = left.isConstant() ? left.getValue(session) : null,
                aValue = a.isConstant() ? a.getValue(session) : null,
                bValue = b.isConstant() ? b.getValue(session) : null;
        if (value != null) {
            if (value == ValueNull.INSTANCE) {
                return TypedValueExpression.UNKNOWN;
            }
            if (aValue != null && bValue != null) {
                return ValueExpression.getBoolean(getValue(session, value, aValue, bValue));
            }
        }
        if (symmetric) {
            if (aValue == ValueNull.INSTANCE || bValue == ValueNull.INSTANCE) {
                return TypedValueExpression.UNKNOWN;
            }
        } else if (aValue == ValueNull.INSTANCE && bValue == ValueNull.INSTANCE) {
            return TypedValueExpression.UNKNOWN;
        }
        if (aValue != null && bValue != null && session.compareWithNull(aValue, bValue, false) == 0) {
            return new Comparison(not ? Comparison.NOT_EQUAL : Comparison.EQUAL, left, a, false).optimize(session);
        }
        return this;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value value = left.getValue(session);
        if (value == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        return getValue(session, value, a.getValue(session), b.getValue(session));
    }

    @Override
    public boolean getWhenValue(SessionLocal session, Value left) {
        if (!whenOperand) {
            return super.getWhenValue(session, left);
        }
        if (left == ValueNull.INSTANCE) {
            return false;
        }
        return getValue(session, left, a.getValue(session), b.getValue(session)).isTrue();
    }

    private Value getValue(SessionLocal session, Value value, Value aValue, Value bValue) {
        int cmp1 = session.compareWithNull(aValue, value, false);
        int cmp2 = session.compareWithNull(value, bValue, false);
        if (cmp1 == Integer.MIN_VALUE) {
            return symmetric || cmp2 <= 0 ? ValueNull.INSTANCE : ValueBoolean.get(not);
        } else if (cmp2 == Integer.MIN_VALUE) {
            return symmetric || cmp1 <= 0 ? ValueNull.INSTANCE : ValueBoolean.get(not);
        } else {
            return ValueBoolean.get(not ^ //
                    (symmetric ? cmp1 <= 0 && cmp2 <= 0 || cmp1 >= 0 && cmp2 >= 0 : cmp1 <= 0 && cmp2 <= 0));
        }
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
        return new BetweenPredicate(left, !not, false, symmetric, a, b);
    }

    @Override
    public void createIndexConditions(SessionLocal session, TableFilter filter) {
        if (!not && !whenOperand && !symmetric) {
            Comparison.createIndexConditions(filter, a, left, Comparison.SMALLER_EQUAL);
            Comparison.createIndexConditions(filter, left, b, Comparison.SMALLER_EQUAL);
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        left.setEvaluatable(tableFilter, value);
        a.setEvaluatable(tableFilter, value);
        b.setEvaluatable(tableFilter, value);
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        left.updateAggregate(session, stage);
        a.updateAggregate(session, stage);
        b.updateAggregate(session, stage);
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
        a.mapColumns(resolver, level, state);
        b.mapColumns(resolver, level, state);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && a.isEverything(visitor) && b.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return left.getCost() + a.getCost() + b.getCost() + 1;
    }

    @Override
    public int getSubexpressionCount() {
        return 3;
    }

    @Override
    public Expression getSubexpression(int index) {
        switch (index) {
        case 0:
            return left;
        case 1:
            return a;
        case 2:
            return b;
        default:
            throw new IndexOutOfBoundsException();
        }
    }

}
