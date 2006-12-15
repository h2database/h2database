/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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

public class ValueExpression extends Expression {
    private Value value;
    
    public static ValueExpression NULL = new ValueExpression(ValueNull.INSTANCE);
    
    public static ValueExpression get(Value v) {
        if(v == ValueNull.INSTANCE) {
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
    
    public void createIndexConditions(TableFilter filter) {
        if(value.getType() == Value.BOOLEAN) {
            boolean v = ((ValueBoolean)value).getBoolean().booleanValue();
            if(!v) {
                filter.addIndexCondition(new IndexCondition(Comparison.FALSE, null, this));
            }
        }
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
    }

    public Expression optimize(Session session) throws SQLException {
        return this;
    }
    
    public boolean isConstant() {
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

    public String getSQL() {
        return value.getSQL();
    }

    public void updateAggregate(Session session) throws SQLException {
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        switch(visitor.type) {
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
            return true;
        case ExpressionVisitor.DETERMINISTIC:
            return true;
        case ExpressionVisitor.INDEPENDENT:
            return true;
        case ExpressionVisitor.EVALUATABLE:
            return true;
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            return true;
        default:
            throw Message.getInternalError("type="+visitor.type);
        }
    }

    public int getCost() {
        return 0;
    }

}
