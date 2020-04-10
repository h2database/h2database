/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.command.Parser;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * A user-defined variable, for example: @ID.
 */
public class Variable extends Operation0 {

    private final String name;
    private Value lastValue;

    public Variable(Session session, String name) {
        this.name = name;
        lastValue = session.getVariable(name);
    }

    @Override
    public int getCost() {
        return 0;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        builder.append('@');
        return Parser.quoteIdentifier(builder, name, sqlFlags);
    }

    @Override
    public TypeInfo getType() {
        return lastValue.getType();
    }

    @Override
    public Value getValue(Session session) {
        lastValue = session.getVariable(name);
        return lastValue;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.EVALUATABLE:
            // the value will be evaluated at execute time
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            // it is checked independently if the value is the same as the last
            // time
        case ExpressionVisitor.OPTIMIZABLE_AGGREGATE:
        case ExpressionVisitor.READONLY:
        case ExpressionVisitor.INDEPENDENT:
        case ExpressionVisitor.NOT_FROM_RESOLVER:
        case ExpressionVisitor.QUERY_COMPARABLE:
        case ExpressionVisitor.GET_DEPENDENCIES:
        case ExpressionVisitor.GET_COLUMNS1:
        case ExpressionVisitor.GET_COLUMNS2:
            return true;
        case ExpressionVisitor.DETERMINISTIC:
            return false;
        default:
            throw DbException.throwInternalError("type="+visitor.getType());
        }
    }

    public String getName() {
        return name;
    }

}
