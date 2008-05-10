/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

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

    public Value getValue(Session session) throws SQLException {
        lastValue = session.getVariable(name);
        return lastValue;
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        switch(visitor.type) {
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
            throw Message.getInternalError("type="+visitor.type);
        }
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
    }

    public Expression optimize(Session session) throws SQLException {
        return this;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean value) {
    }

    public void updateAggregate(Session session) throws SQLException {
    }

    public String getName() {
        return name;
    }

}
