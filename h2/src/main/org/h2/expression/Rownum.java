/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueInt;

/**
 * Represents the ROWNUM function.
 */
public class Rownum extends Expression {

    private Prepared prepared;

    public Rownum(Prepared prepared) {
        this.prepared = prepared;
    }

    public Value getValue(Session session) {
        return ValueInt.get(prepared.getCurrentRowNumber());
    }

    public int getType() {
        return Value.INT;
    }

    public void mapColumns(ColumnResolver resolver, int level) {
        // nothing to do
    }

    public Expression optimize(Session session) {
        return this;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        // nothing to do
    }

    public int getScale() {
        return 0;
    }

    public long getPrecision() {
        return ValueInt.PRECISION;
    }

    public int getDisplaySize() {
        return ValueInt.DISPLAY_SIZE;
    }

    public String getSQL() {
        return "ROWNUM()";
    }

    public void updateAggregate(Session session) {
        // nothing to do
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        switch(visitor.getType()) {
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
            return false;
        case ExpressionVisitor.DETERMINISTIC:
            return false;
        case ExpressionVisitor.INDEPENDENT:
            return false;
        case ExpressionVisitor.EVALUATABLE:
            return true;
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            // if everything else is the same, the rownum is the same
            return true;
        case ExpressionVisitor.READONLY:
            return true;
        case ExpressionVisitor.NOT_FROM_RESOLVER:
            return true;
        case ExpressionVisitor.GET_DEPENDENCIES:
            return true;
        default:
            throw Message.throwInternalError("type="+visitor.getType());
        }
    }

    public int getCost() {
        return 0;
    }

}
