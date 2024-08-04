/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import org.h2.command.query.Query;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;

/**
 * Base class for predicates with a subquery.
 */
abstract class PredicateWithSubquery extends Condition {

    /**
     * The subquery.
     */
    final Query query;

    PredicateWithSubquery(Query query) {
        this.query = query;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        query.mapColumns(resolver, level + 1, true);
    }

    @Override
    public Expression optimize(SessionLocal session) {
        query.prepare();
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        query.setEvaluatable(tableFilter, value);
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        return StringUtils.indent(builder.append('('), query.getPlanSQL(sqlFlags), 4, false).append(')');
    }

    @Override
    public void updateAggregate(SessionLocal session, int stage) {
        query.updateAggregate(session, stage);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return query.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return query.getCostAsExpression();
    }

}
