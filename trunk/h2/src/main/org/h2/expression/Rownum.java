/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueInt;

public class Rownum extends Expression {
    
    private Prepared prepared;
    
    public Rownum(Prepared prepared) {
        this.prepared = prepared;
    }

    public Value getValue(Session session) throws SQLException {
        return ValueInt.get(prepared.getCurrentRowNumber());
    }

    public int getType() {
        return Value.INT;
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
    }

    public Expression optimize(Session session) throws SQLException {
        return this;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
    }

    public int getScale() {
        return 0;
    }

    public long getPrecision() {
        return ValueInt.PRECISION;
    }

    public String getSQL() {
        return "ROWNUM()";
    }

    public void updateAggregate(Session session) throws SQLException {
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        switch(visitor.type) {
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
        default:
            throw Message.getInternalError("type="+visitor.type);
        }
    }
    
    public int getCost() {
        return 0;
    }

}
