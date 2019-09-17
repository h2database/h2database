/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.ValueExpression;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;

/**
 * Base class for simple predicates.
 */
public abstract class SimplePredicate extends Condition {

    /**
     * The left hand side of the expression.
     */
    Expression left;

    /**
     * Whether it is a "not" condition (e.g. "is not like").
     */
    final boolean not;

    SimplePredicate(Expression left, boolean not) {
        this.left = left;
        this.not = not;
    }

    @Override
    public Expression optimize(Session session) {
        left = left.optimize(session);
        if (left.isConstant()) {
            return ValueExpression.getBoolean(getValue(session));
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        left.updateAggregate(session, stage);
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return left.getCost() + 1;
    }

    @Override
    public int getSubexpressionCount() {
        return 1;
    }

    @Override
    public Expression getSubexpression(int index) {
        if (index == 0) {
            return left;
        }
        throw new IndexOutOfBoundsException();
    }

}
