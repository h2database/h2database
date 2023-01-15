/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.SessionLocal;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;

/**
 * Operation without subexpressions.
 */
public abstract class Operation0 extends Expression {

    protected Operation0() {
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        // Nothing to do
    }

    @Override
    public Expression optimize(SessionLocal session) {
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        // Nothing to do
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        // Nothing to do
    }

}
