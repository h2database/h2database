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

/**
 * Operation with one or two arguments.
 */
public abstract class Operation1_2 extends Expression {

    /**
     * The left part of the operation (the first argument).
     */
    protected Expression left;

    /**
     * The right part of the operation (the second argument).
     */
    protected Expression right;

    /**
     * The type of the result.
     */
    protected TypeInfo type;

    protected Operation1_2(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
        if (right != null) {
            right.mapColumns(resolver, level, state);
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        left.setEvaluatable(tableFilter, value);
        if (right != null) {
            right.setEvaluatable(tableFilter, value);
        }
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        left.updateAggregate(session, stage);
        if (right != null) {
            right.updateAggregate(session, stage);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && (right == null || right.isEverything(visitor));
    }

    @Override
    public int getCost() {
        int cost = left.getCost() + 1;
        if (right != null) {
            cost += right.getCost();
        }
        return cost;
    }

    @Override
    public int getSubexpressionCount() {
        return right != null ? 2 : 1;
    }

    @Override
    public Expression getSubexpression(int index) {
        if (index == 0) {
            return left;
        }
        if (index == 1 && right != null) {
            return right;
        }
        throw new IndexOutOfBoundsException();
    }

}
