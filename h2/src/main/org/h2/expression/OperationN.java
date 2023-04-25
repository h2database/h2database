/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.Arrays;

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
