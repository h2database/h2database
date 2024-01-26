/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.SessionLocal;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A simple case.
 */
public final class SimpleCase extends Expression {

    public static final class SimpleWhen {

        Expression[] operands;

        Expression result;

        SimpleWhen next;

        public SimpleWhen(Expression operand, Expression result) {
            this(new Expression[] { operand }, result);
        }

        public SimpleWhen(Expression[] operands, Expression result) {
            this.operands = operands;
            this.result = result;
        }

        public void setWhen(SimpleWhen next) {
            this.next = next;
        }

    }

    private Expression operand;

    private SimpleWhen when;

    private Expression elseResult;

    private TypeInfo type;

    public SimpleCase(Expression operand, SimpleWhen when, Expression elseResult) {
        this.operand = operand;
        this.when = when;
        this.elseResult = elseResult;
    }

    @Override
    public Value getValue(SessionLocal session) {
        Value v = operand.getValue(session);
        for (SimpleWhen when = this.when; when != null; when = when.next) {
            for (Expression e : when.operands) {
                if (e.getWhenValue(session, v)) {
                    return when.result.getValue(session).convertTo(type, session);
                }
            }
        }
        if (elseResult != null) {
            return elseResult.getValue(session).convertTo(type, session);
        }
        return ValueNull.INSTANCE;
    }

    @Override
    public Expression optimize(SessionLocal session) {
        TypeInfo typeInfo = TypeInfo.TYPE_UNKNOWN;
        operand = operand.optimize(session);
        boolean allConst = operand.isConstant();
        Value v = null;
        if (allConst) {
            v = operand.getValue(session);
        }
        TypeInfo operandType = operand.getType();
        for (SimpleWhen when = this.when; when != null; when = when.next) {
            Expression[] operands = when.operands;
            for (int i = 0; i < operands.length; i++) {
                Expression e = operands[i].optimize(session);
                if (!e.isWhenConditionOperand()) {
                    TypeInfo.checkComparable(operandType, e.getType());
                }
                if (allConst) {
                    if (e.isConstant()) {
                        if (e.getWhenValue(session, v)) {
                            return when.result.optimize(session);
                        }
                    } else {
                        allConst = false;
                    }
                }
                operands[i] = e;
            }
            when.result = when.result.optimize(session);
            typeInfo = combineTypes(typeInfo, when.result);
        }
        if (elseResult != null) {
            elseResult = elseResult.optimize(session);
            if (allConst) {
                return elseResult;
            }
            typeInfo = combineTypes(typeInfo, elseResult);
        } else if (allConst) {
            return ValueExpression.NULL;
        }
        if (typeInfo.getValueType() == Value.UNKNOWN) {
            typeInfo = TypeInfo.TYPE_VARCHAR;
        }
        type = typeInfo;
        return this;
    }

    static TypeInfo combineTypes(TypeInfo typeInfo, Expression e) {
        if (!e.isNullConstant()) {
            TypeInfo type = e.getType();
            int valueType = type.getValueType();
            if (valueType != Value.UNKNOWN && valueType != Value.NULL) {
                typeInfo = TypeInfo.getHigherType(typeInfo, type);
            }
        }
        return typeInfo;
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        operand.getUnenclosedSQL(builder.append("CASE "), sqlFlags);
        for (SimpleWhen when = this.when; when != null; when = when.next) {
            builder.append(" WHEN");
            Expression[] operands = when.operands;
            for (int i = 0, len = operands.length; i < len; i++) {
                if (i > 0) {
                    builder.append(',');
                }
                operands[i].getWhenSQL(builder, sqlFlags);
            }
            when.result.getUnenclosedSQL(builder.append(" THEN "), sqlFlags);
        }
        if (elseResult != null) {
            elseResult.getUnenclosedSQL(builder.append(" ELSE "), sqlFlags);
        }
        return builder.append(" END");
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        operand.mapColumns(resolver, level, state);
        for (SimpleWhen when = this.when; when != null; when = when.next) {
            for (Expression e : when.operands) {
                e.mapColumns(resolver, level, state);
            }
            when.result.mapColumns(resolver, level, state);
        }
        if (elseResult != null) {
            elseResult.mapColumns(resolver, level, state);
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        operand.setEvaluatable(tableFilter, value);
        for (SimpleWhen when = this.when; when != null; when = when.next) {
            for (Expression e : when.operands) {
                e.setEvaluatable(tableFilter, value);
            }
            when.result.setEvaluatable(tableFilter, value);
        }
        if (elseResult != null) {
            elseResult.setEvaluatable(tableFilter, value);
        }
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        operand.updateAggregate(session, stage);
        for (SimpleWhen when = this.when; when != null; when = when.next) {
            for (Expression e : when.operands) {
                e.updateAggregate(session, stage);
            }
            when.result.updateAggregate(session, stage);
        }
        if (elseResult != null) {
            elseResult.updateAggregate(session, stage);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (!operand.isEverything(visitor)) {
            return false;
        }
        for (SimpleWhen when = this.when; when != null; when = when.next) {
            for (Expression e : when.operands) {
                if (!e.isEverything(visitor)) {
                    return false;
                }
            }
            if (!when.result.isEverything(visitor)) {
                return false;
            }
        }
        if (elseResult != null && !elseResult.isEverything(visitor)) {
            return false;
        }
        return true;
    }

    @Override
    public int getCost() {
        int cost = 1, resultCost = 0;
        cost += operand.getCost();
        for (SimpleWhen when = this.when; when != null; when = when.next) {
            for (Expression e : when.operands) {
                cost += e.getCost();
            }
            resultCost = Math.max(resultCost, when.result.getCost());
        }
        if (elseResult != null) {
            resultCost = Math.max(resultCost, elseResult.getCost());
        }
        return cost + resultCost;
    }

    @Override
    public int getSubexpressionCount() {
        int count = 1;
        for (SimpleWhen when = this.when; when != null; when = when.next) {
            count += when.operands.length + 1;
        }
        if (elseResult != null) {
            count++;
        }
        return count;
    }

    @Override
    public Expression getSubexpression(int index) {
        if (index >= 0) {
            if (index == 0) {
                return operand;
            }
            int ptr = 1;
            for (SimpleWhen when = this.when; when != null; when = when.next) {
                Expression[] operands = when.operands;
                int count = operands.length;
                int offset = index - ptr;
                if (offset < count) {
                    return operands[offset];
                }
                ptr += count;
                if (index == ptr++) {
                    return when.result;
                }
            }
            if (elseResult != null && index == ptr) {
                return elseResult;
            }
        }
        throw new IndexOutOfBoundsException();
    }

}
