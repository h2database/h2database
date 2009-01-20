/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.command.Parser;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;

/**
 * A user-defined variable, for example: @ID.
 */
public class Variable extends Expression {

    private final String name;
    private Value lastValue;

    public Variable(Session session, String name) {
        this.name = name;
        lastValue = session.getVariable(name);
    }

    public int getCost() {
        return 0;
    }

    public int getDisplaySize() {
        return lastValue.getDisplaySize();
    }

    public long getPrecision() {
        return lastValue.getPrecision();
    }

    public String getSQL() {
        return "@" + Parser.quoteIdentifier(name);
    }

    public int getScale() {
        return lastValue.getScale();
    }

    public int getType() {
        return lastValue.getType();
    }

    public Value getValue(Session session) {
        lastValue = session.getVariable(name);
        return lastValue;
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        switch(visitor.getType()) {
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
            return true;
        case ExpressionVisitor.DETERMINISTIC:
            return false;
        case ExpressionVisitor.READONLY:
            return true;
        case ExpressionVisitor.INDEPENDENT:
            return true;
        case ExpressionVisitor.EVALUATABLE:
            // the value will be evaluated at execute time
            return true;
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            // it is checked independently if the value is the same as the last time
            return true;
        case ExpressionVisitor.NOT_FROM_RESOLVER:
            return true;
        case ExpressionVisitor.GET_DEPENDENCIES:
            return true;
        default:
            throw Message.throwInternalError("type="+visitor.getType());
        }
    }

    public void mapColumns(ColumnResolver resolver, int level) {
        // nothing to do
    }

    public Expression optimize(Session session) {
        return this;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        // nothing to do
    }

    public void updateAggregate(Session session) {
        // nothing to do
    }

    public String getName() {
        return name;
    }

}
