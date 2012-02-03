/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;

import org.h2.command.dml.Query;
import org.h2.engine.Session;
import org.h2.result.LocalResult;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;

/**
 * An 'exists' condition as in WHERE EXISTS(SELECT ...)
 */
public class ConditionExists extends Condition {

    private final Query query;

    public ConditionExists(Query query) {
        this.query = query;
    }

    public Value getValue(Session session) throws SQLException {
        query.setSession(session);
        LocalResult result = query.query(1);
        session.addTemporaryResult(result);
        boolean r = result.getRowCount() > 0;
        return ValueBoolean.get(r);
    }

    public Expression optimize(Session session) throws SQLException {
        query.prepare();
        return this;
    }

    public String getSQL() {
        return "EXISTS(" + query.getPlanSQL() + ")";
    }

    public void updateAggregate(Session session) {
        // TODO exists: is it allowed that the subquery contains aggregates?
        // probably not
        // select id from test group by id having exists (select * from test2
        // where id=count(test.id))
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        query.mapColumns(resolver, level + 1);
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        query.setEvaluatable(tableFilter, b);
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        return query.isEverything(visitor);
    }

    public int getCost() {
        return 10 + (int) (10 * query.getCost());
    }

}
