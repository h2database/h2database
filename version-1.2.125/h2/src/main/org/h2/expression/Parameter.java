/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * A parameter of a prepared statement.
 */
public class Parameter extends Expression implements ParameterInterface {

    private Value value;
    private Column column;
    private int index;

    public Parameter(int index) {
        this.index = index;
    }

    public String getSQL() {
        return "?" + (index + 1);
    }

    public void setValue(Value v, boolean closeOld) {
        // don't need to close the old value as temporary files are anyway removed
        this.value = v;
    }

    public void setValue(Value v) {
        this.value = v;
    }

    public Value getParamValue() {
        if (value == null) {
            // to allow parameters in function tables
            return ValueNull.INSTANCE;
        }
        return value;
    }

    public Value getValue(Session session) {
        return getParamValue();
    }

    public int getType() {
        if (value != null) {
            return value.getType();
        }
        if (column != null) {
            return column.getType();
        }
        return Value.UNKNOWN;
    }

    public void mapColumns(ColumnResolver resolver, int level) {
        // can't map
    }

    public void checkSet() throws SQLException {
        if (value == null) {
            throw Message.getSQLException(ErrorCode.PARAMETER_NOT_SET_1, "#" + (index + 1));
        }
    }

    public Expression optimize(Session session) {
        return this;
    }

    public boolean isConstant() {
        return false;
    }

    public boolean isValueSet() {
        return value != null;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        // not bound
    }

    public int getScale() {
        if (value != null) {
            return value.getScale();
        }
        if (column != null) {
            return column.getScale();
        }
        return 0;
    }

    public long getPrecision() {
        if (value != null) {
            return value.getPrecision();
        }
        if (column != null) {
            return column.getPrecision();
        }
        return 0;
    }

    public int getDisplaySize() {
        if (value != null) {
            return value.getDisplaySize();
        }
        if (column != null) {
            return column.getDisplaySize();
        }
        return 0;
    }

    public void updateAggregate(Session session) {
        // nothing to do
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        switch(visitor.getType()) {
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

    public Expression getNotIfPossible(Session session) {
        return new Comparison(session, Comparison.EQUAL, this, ValueExpression.get(ValueBoolean.get(false)));
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public int getIndex() {
        return index;
    }

}
