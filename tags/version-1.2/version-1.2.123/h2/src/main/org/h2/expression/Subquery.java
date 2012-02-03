/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.command.dml.Query;
import org.h2.constant.ErrorCode;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;

/**
 * A query returning a single value.
 * Subqueries are used inside other statements.
 */
public class Subquery extends Expression {

    private Query query;

    public Subquery(Query query) {
        this.query = query;
    }

    public Value getValue(Session session) throws SQLException {
        query.setSession(session);
        LocalResult result = query.query(2);
        try {
            int rowcount = result.getRowCount();
            if (rowcount > 1) {
                throw Message.getSQLException(ErrorCode.SCALAR_SUBQUERY_CONTAINS_MORE_THAN_ONE_ROW);
            }
            Value v;
            if (rowcount <= 0) {
                v = ValueNull.INSTANCE;
            } else {
                result.next();
                Value[] values = result.currentRow();
                if (result.getVisibleColumnCount() == 1) {
                    v = values[0];
                } else {
                    v = ValueArray.get(values);
                }
            }
            return v;
        } finally {
            result.close();
        }
    }

    public int getType() {
        return getExpression().getType();
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        query.mapColumns(resolver, level + 1);
    }

    public Expression optimize(Session session) throws SQLException {
        query.prepare();
        return this;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        query.setEvaluatable(tableFilter, b);
    }

    public int getScale() {
        return getExpression().getScale();
    }

    public long getPrecision() {
        return getExpression().getPrecision();
    }

    public int getDisplaySize() {
        return getExpression().getDisplaySize();
    }

    public String getSQL() {
        return "(" + query.getPlanSQL() + ")";
    }

    public void updateAggregate(Session session) throws SQLException {
        query.updateAggregate(session);
    }

    private Expression getExpression() {
        return query.getExpressions().get(0);
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        return query.isEverything(visitor);
    }

    public Query getQuery() {
        return query;
    }

    public int getCost() {
        return 10 + (int) (10 * query.getCost());
    }

}
