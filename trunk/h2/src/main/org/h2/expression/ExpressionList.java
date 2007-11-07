/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueArray;

public class ExpressionList extends Expression {

    private Expression[] list;

    public ExpressionList(Expression[] list) {
        this.list = list;
    }

    public Value getValue(Session session) throws SQLException {
        Value[] v = new Value[list.length];
        for (int i = 0; i < list.length; i++) {
            v[i] = list[i].getValue(session);
        }
        return ValueArray.get(v);
    }

    public int getType() {
        return Value.ARRAY;
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        for (int i = 0; i < list.length; i++) {
            list[i].mapColumns(resolver, level);
        }
    }

    public Expression optimize(Session session) throws SQLException {
        boolean allConst = true;
        for (int i = 0; i < list.length; i++) {
            Expression e = list[i].optimize(session);
            if (!e.isConstant()) {
                allConst = false;
            }
            list[i] = e;
        }
        if (allConst) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (int i = 0; i < list.length; i++) {
            list[i].setEvaluatable(tableFilter, b);
        }
    }

    public int getScale() {
        return 0;
    }

    public long getPrecision() {
        return 0;
    }
    
    public int getDisplaySize() {
        // TODO display size of a list
        return 255;
    }

    public String getSQL() {
        StringBuffer buff = new StringBuffer();
        buff.append('(');
        for (int i = 0; i < list.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(list[i].getSQL());
        }
        buff.append(')');
        return buff.toString();
    }

    public void updateAggregate(Session session) throws SQLException {
        for (int i = 0; i < list.length; i++) {
            list[i].updateAggregate(session);
        }
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        for (int i = 0; i < list.length; i++) {
            if (!list[i].isEverything(visitor)) {
                return false;
            }
        }
        return true;
    }

    public int getCost() {
        int cost = 1;
        for (int i = 0; i < list.length; i++) {
            cost += list[i].getCost();
        }
        return cost;
    }

}
