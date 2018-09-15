/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.expression.Expression;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;

/**
 * A base class for aggregates.
 */
public abstract class AbstractAggregate extends Expression {

    protected Expression filterCondition;

    protected Window over;

    /**
     * Sets the FILTER condition.
     *
     * @param filterCondition
     *            FILTER condition
     */
    public void setFilterCondition(Expression filterCondition) {
        this.filterCondition = filterCondition;
    }

    /**
     * Sets the OVER condition.
     *
     * @param over
     *            OVER condition
     */
    public void setOverCondition(Window over) {
        this.over = over;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        if (filterCondition != null) {
            filterCondition.mapColumns(resolver, level);
        }
        if (over != null) {
            over.mapColumns(resolver, level);
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if (filterCondition != null) {
            filterCondition.setEvaluatable(tableFilter, b);
        }
        if (over != null) {
            over.setEvaluatable(tableFilter, b);
        }
    }

    protected StringBuilder appendTailConditions(StringBuilder builder) {
        if (filterCondition != null) {
            builder.append(" FILTER (WHERE ").append(filterCondition.getSQL()).append(')');
        }
        if (over != null) {
            builder.append(' ').append(over.getSQL());
        }
        return builder;
    }

}
