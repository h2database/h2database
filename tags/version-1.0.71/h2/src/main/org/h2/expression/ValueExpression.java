/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.index.IndexCondition;
import org.h2.message.Message;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * An expression representing a constant value.
 */
public class ValueExpression extends Expression {
    private Value value;

    public static final ValueExpression NULL = new ValueExpression(ValueNull.INSTANCE);
    public static final ValueExpression DEFAULT = new ValueExpression(ValueNull.INSTANCE);

    public static ValueExpression get(Value v) {
        if (v == ValueNull.INSTANCE) {
            return ValueExpression.NULL;
        }
        return new ValueExpression(v);
    }

    private ValueExpression(Value value) {
        this.value = value;
    }

    public Value getValue(Session session) {
        return value;
    }

    public int getType() {
        return value.getType();
    }

    public void createIndexConditions(Session session, TableFilter filter) {
        if (value.getType() == Value.BOOLEAN) {
            boolean v = ((ValueBoolean) value).getBoolean().booleanValue();
            if (!v) {
                filter.addIndexCondition(new IndexCondition(Comparison.FALSE, null, this));
            }
        }
    }

    public Expression getNotIfPossible(Session session) {
        return new Comparison(session, Comparison.EQUAL, this, ValueExpression.get(ValueBoolean.get(false)));
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
    }

    public Expression optimize(Session session) throws SQLException {
        return this;
    }

    public boolean isConstant() {
        return true;
    }

    public boolean isValueSet() {
        return true;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
    }

    public int getScale() {
        return value.getScale();
    }

    public long getPrecision() {
        return value.getPrecision();
    }

    public int getDisplaySize() {
        return value.getDisplaySize();
    }

    public String getSQL() {
        if (this == DEFAULT) {
            return "DEFAULT";
        } else {
            return value.getSQL();
        }
    }

    public void updateAggregate(Session session) throws SQLException {
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.type) {
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
            return true;
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.READONLY:
            return true;
        case ExpressionVisitor.INDEPENDENT:
            return true;
        case ExpressionVisitor.EVALUATABLE:
            return true;
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            return true;
        case ExpressionVisitor.NOT_FROM_RESOLVER:
            return true;
        case ExpressionVisitor.GET_DEPENDENCIES:
            return true;
        default:
            throw Message.getInternalError("type=" + visitor.type);
        }
    }

    public int getCost() {
        return 0;
    }

}
