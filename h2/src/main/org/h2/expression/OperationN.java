/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.Arrays;
import java.util.function.Predicate;

import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;

/**
 * Operation with many arguments.
 */
public abstract class OperationN extends Expression implements ExpressionWithVariableParameters {

    /**
     * The array of arguments.
     */
    protected Expression[] args;

    /**
     * The number of arguments.
     */
    protected int argsCount;

    /**
     * The type of the result.
     */
    protected TypeInfo type;

    protected OperationN(Expression[] args) {
        this.args = args;
    }

    @Override
    public void addParameter(Expression param) {
        int capacity = args.length;
        if (argsCount >= capacity) {
            args = Arrays.copyOf(args, capacity * 2);
        }
        args[argsCount++] = param;
    }

    @Override
    public void doneWithParameters() throws DbException {
        if (args.length != argsCount) {
            args = Arrays.copyOf(args, argsCount);
        }
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        for (Expression e : args) {
            e.mapColumns(resolver, level, state);
        }
    }

    /**
     * Optimizes arguments.
     *
     * @param session
     *            the session
     * @param allConst
     *            whether operation is deterministic
     * @return whether operation is deterministic and all arguments are
     *         constants
     */
    protected boolean optimizeArguments(SessionLocal session, boolean allConst) {
        for (int i = 0, l = args.length; i < l; i++) {
            Expression e = args[i].optimize(session);
            args[i] = e;
            if (allConst && !e.isConstant()) {
                allConst = false;
            }
        }
        return allConst;
    }

    /**
     * Inlines subexpressions if possible.
     *
     * @param tester
     *            the predicate to check whether subexpression can be inlined
     */
    protected final void inlineSubexpressions(Predicate<Expression> tester) {
        for (int i = 0, sourceLength = args.length; i < sourceLength; i++) {
            Expression e = args[i];
            if (tester.test(e)) {
                inlineSubexpressions(tester, i, sourceLength, e);
                break;
            }
        }
    }

    private void inlineSubexpressions(Predicate<Expression> tester, int sourceOffset, int sourceLength,
            Expression match1) {
        int l1 = match1.getSubexpressionCount();
        boolean many = false, forceCopy = false;
        int targetLength = sourceLength;
        if (l1 != 1) {
            forceCopy = true;
            targetLength += l1 - 1;
        }
        for (int i = sourceOffset + 1; i < sourceLength; i++) {
            Expression e = args[i];
            if (tester.test(e)) {
                many = true;
                int l2 = e.getSubexpressionCount();
                if (l2 != 1) {
                    forceCopy = true;
                    targetLength += l2 - 1;
                }
            }
        }
        Expression[] source = args;
        if (forceCopy) {
            args = new Expression[targetLength];
            System.arraycopy(source, 0, args, 0, sourceOffset);
        }
        copyArgs(match1, sourceOffset, l1);
        if (many) {
            for (int targetOffset = sourceOffset + l1; ++sourceOffset < sourceLength;) {
                Expression e = source[sourceOffset];
                if (tester.test(e)) {
                    int l2 = e.getSubexpressionCount();
                    copyArgs(e, targetOffset, l2);
                    targetOffset += l2;
                } else {
                    args[targetOffset++] = e;
                }
            }
        } else if (forceCopy) {
            System.arraycopy(source, sourceOffset + 1, args, sourceOffset + l1, sourceLength - sourceOffset - 1);
        }
    }

    private void copyArgs(Expression e, int offset, int count) {
        if (e instanceof OperationN) {
            System.arraycopy(((OperationN) e).args, 0, args, offset, count);
        } else {
            for (int j = 0; j < count; j++) {
                args[offset + j] = e.getSubexpression(j);
            }
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        for (Expression e : args) {
            e.setEvaluatable(tableFilter, value);
        }
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        for (Expression e : args) {
            e.updateAggregate(session, stage);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        for (Expression e : args) {
            if (!e.isEverything(visitor)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int getCost() {
        int cost = args.length + 1;
        for (Expression e : args) {
            cost += e.getCost();
        }
        return cost;
    }

    @Override
    public int getSubexpressionCount() {
        return args.length;
    }

    @Override
    public Expression getSubexpression(int index) {
        return args[index];
    }

}
