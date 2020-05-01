/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.Arrays;

import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;

/**
 * Operation with many arguments.
 */
public abstract class OperationN extends Expression {

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

    /**
     * Adds the parameter expression.
     *
     * @param param
     *            the expression
     */
    public void addParameter(Expression param) {
        int capacity = args.length;
        if (argsCount >= capacity) {
            args = Arrays.copyOf(args, capacity * 2);
        }
        args[argsCount++] = param;
    }

    /**
     * This method must be called after all the parameters have been set. It
     * checks if the parameter count is correct when required by the
     * implementation.
     *
     * @throws DbException
     *             if the parameter count is incorrect.
     */
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

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        for (Expression e : args) {
            e.setEvaluatable(tableFilter, value);
        }
    }

    @Override
    public void updateAggregate(Session session, int stage) {
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
