/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;

/**
 * @author Thomas
 */
public class Parameter extends Expression implements ParameterInterface {

    private Value value;
    private int index;

    public Parameter(int index) {
        this.index = index;
    }

    public String getSQL() {
        return "?" + (index + 1);
    }

    public void setValue(Value v) {
        this.value = v;
    }

    public Value getParamValue() throws SQLException {
        return value == null ? null : value;
    }

    public Value getValue(Session session) throws SQLException {
        return getParamValue();
    }

    public int getType() {
        return value == null ? Value.UNKNOWN : value.getType();
    }

    public void mapColumns(ColumnResolver resolver, int level) {
        // can't map
    }

    public void checkMapped() {
        // always ok
    }

    public void checkSet() throws SQLException {
        if (value == null) {
            throw Message.getSQLException(Message.PARAMETER_NOT_SET_1, String.valueOf(index + 1));
        }
    }

    public Expression optimize(Session session) {
        return this;
    }

    public boolean isConstant() {
        return value != null;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        // not bound
    }

    public int getScale() {
        return value == null ? 0 : value.getScale();
    }

    public long getPrecision() {
        return value == null ? 0 : value.getPrecision();
    }

    public void updateAggregate(Session session) {
        // nothing to do
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        switch(visitor.type) {
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
            return true;
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.READONLY:
            return true;
        case ExpressionVisitor.INDEPENDENT:
            return value != null;
        case ExpressionVisitor.EVALUATABLE:
            // the parameter _will_be_ evaluatable at execute time
            return true;
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            // it is checked independently if the value is the same as the last time
            return true;
        default:
            throw Message.getInternalError("type="+visitor.type);
        }
    }
    
    public int getCost() {
        return 0;
    }

}
