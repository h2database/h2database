/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Session;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A simple case.
 */
public class SimpleCase extends Expression {

    public static abstract class SimpleWhen {

        Expression result;

        SimpleWhen next;

        SimpleWhen(Expression result) {
            this.result = result;
        }

        public void addWhen(SimpleWhen next) {
            this.next = next;
        }

    }

    public static final class SimpleWhen1 extends SimpleWhen {

        Expression operand;

        public SimpleWhen1(Expression operand, Expression result) {
            super(result);
            this.operand = operand;
        }

    }

    public static final class SimpleWhenN extends SimpleWhen {

        Expression[] operands;

        Expression result;

        public SimpleWhenN(Expression[] operands, Expression result) {
            super(result);
            this.operands = operands;
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

    /**
     * Adds an else clause.
     *
     * @param result
     *            the result
     */
    public void addElse(Expression result) {
        elseResult = result;
    }

    @Override
    public Value getValue(Session session) {
        Value v = operand.getValue(session);
        if (v != ValueNull.INSTANCE) {
            for (SimpleWhen when = this.when; when != null; when = when.next) {
                if (when instanceof SimpleWhen1) {
                    if (session.areEqual(v, ((SimpleWhen1) when).operand.getValue(session))) {
                        return when.result.getValue(session).convertTo(type, session);
                    }
                } else {
                    for (Expression e : ((SimpleWhenN) when).operands) {
                        if (session.areEqual(v, e.getValue(session))) {
                            return when.result.getValue(session).convertTo(type, session);
                        }
                    }
                }
            }
        }
        if (elseResult != null) {
            return elseResult.getValue(session).convertTo(type, session);
        }
        return ValueNull.INSTANCE;
    }

    @Override
    public Expression optimize(Session session) {
        TypeInfo typeInfo = TypeInfo.TYPE_UNKNOWN;
        operand = operand.optimize(session);
        boolean allConst = operand.isConstant();
        Value v = null;
        if (allConst) {
            v = operand.getValue(session);
            if (v == ValueNull.INSTANCE) {
                if (elseResult != null) {
                    return elseResult.optimize(session);
                }
                return ValueExpression.NULL;
            }
        }
        for (SimpleWhen when = this.when; when != null; when = when.next) {
            if (when instanceof SimpleWhen1) {
                SimpleWhen1 w = (SimpleWhen1) when;
                Expression e = w.operand.optimize(session);
                if (allConst) {
                    if (e.isConstant()) {
                        if (session.areEqual(v, e.getValue(session))) {
                            return when.result.optimize(session);
                        }
                    } else {
                        allConst = false;
                    }
                }
                w.operand = e;
            } else {
                Expression[] operands = ((SimpleWhenN) when).operands;
                for (int i = 0; i < operands.length; i++) {
                    Expression e = operands[i].optimize(session);
                    if (allConst) {
                        if (e.isConstant()) {
                            if (session.areEqual(v, e.getValue(session))) {
                                return when.result.optimize(session);
                            }
                        } else {
                            allConst = false;
                        }
                    }
                    operands[i] = e;
                }
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

    public static TypeInfo combineTypes(TypeInfo typeInfo, Expression e) {
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
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        operand.getSQL(builder.append("CASE "), sqlFlags);
        for (SimpleWhen when = this.when; when != null; when = when.next) {
            builder.append(" WHEN ");
            if (when instanceof SimpleWhen1) {
                ((SimpleWhen1) when).operand.getSQL(builder, sqlFlags);
            } else {
                Expression[] operands = ((SimpleWhenN) when).operands;
                for (int i = 0, len = operands.length; i < len; i++) {
                    if (i > 0) {
                        builder.append(", ");
                    }
                    operands[i].getSQL(builder, sqlFlags);
                }
            }
            when.result.getSQL(builder.append(" THEN "), sqlFlags);
        }
        if (elseResult != null) {
            elseResult.getSQL(builder.append(" ELSE "), sqlFlags);
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
            if (when instanceof SimpleWhen1) {
                ((SimpleWhen1) when).operand.mapColumns(resolver, level, state);
            } else {
                for (Expression e : ((SimpleWhenN) when).operands) {
                    e.mapColumns(resolver, level, state);
                }
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
            if (when instanceof SimpleWhen1) {
                ((SimpleWhen1) when).operand.setEvaluatable(tableFilter, value);
            } else {
                for (Expression e : ((SimpleWhenN) when).operands) {
                    e.setEvaluatable(tableFilter, value);
                }
            }
            when.result.setEvaluatable(tableFilter, value);
        }
        if (elseResult != null) {
            elseResult.setEvaluatable(tableFilter, value);
        }
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        operand.updateAggregate(session, stage);
        for (SimpleWhen when = this.when; when != null; when = when.next) {
            if (when instanceof SimpleWhen1) {
                ((SimpleWhen1) when).operand.updateAggregate(session, stage);
            } else {
                for (Expression e : ((SimpleWhenN) when).operands) {
                    e.updateAggregate(session, stage);
                }
            }
            when.result.updateAggregate(session, stage);
        }
        if (elseResult != null) {
            elseResult.updateAggregate(session, stage);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (operand.isEverything(visitor)) {
            return false;
        }
        for (SimpleWhen when = this.when; when != null; when = when.next) {
            if (when instanceof SimpleWhen1) {
                if (((SimpleWhen1) when).operand.isEverything(visitor)) {
                    return false;
                }
            } else {
                for (Expression e : ((SimpleWhenN) when).operands) {
                    if (e.isEverything(visitor)) {
                        return false;
                    }
                }
            }
            if (when.result.isEverything(visitor)) {
                return false;
            }
        }
        if (elseResult != null && elseResult.isEverything(visitor)) {
            return false;
        }
        return true;
    }

    @Override
    public int getCost() {
        int cost = 1, resultCost = 0;
        cost += operand.getCost();
        for (SimpleWhen when = this.when; when != null; when = when.next) {
            if (when instanceof SimpleWhen1) {
                cost += ((SimpleWhen1) when).operand.getCost();
            } else {
                for (Expression e : ((SimpleWhenN) when).operands) {
                    cost += e.getCost();
                }
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
            if (when instanceof SimpleWhen1) {
                count++;
            } else {
                count += ((SimpleWhenN) when).operands.length;
            }
            count++;
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
                if (when instanceof SimpleWhen1) {
                    if (index == ptr++) {
                        return ((SimpleWhen1) when).operand;
                    }
                } else {
                    Expression[] operands = ((SimpleWhenN) when).operands;
                    int count = operands.length;
                    int offset = index - ptr;
                    if (offset < count) {
                        return operands[offset];
                    }
                    ptr += count;
                }
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
