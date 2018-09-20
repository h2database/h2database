/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.ArrayList;

import org.h2.command.dml.Select;
import org.h2.command.dml.SelectGroups;
import org.h2.command.dml.SelectOrderBy;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;

/**
 * A base class for aggregate functions.
 */
public abstract class AbstractAggregate extends DataAnalysisOperation {

    protected final boolean distinct;

    protected Expression filterCondition;

    AbstractAggregate(Select select, boolean distinct) {
        super(select);
        this.distinct = distinct;
    }

    @Override
    public final boolean isAggregate() {
        return true;
    }

    /**
     * Sets the FILTER condition.
     *
     * @param filterCondition
     *            FILTER condition
     */
    public void setFilterCondition(Expression filterCondition) {
        this.filterCondition = filterCondition;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        if (filterCondition != null) {
            filterCondition.mapColumns(resolver, level);
        }
        super.mapColumns(resolver, level);
    }

    @Override
    public Expression optimize(Session session) {
        if (filterCondition != null) {
            filterCondition = filterCondition.optimize(session);
        }
        return super.optimize(session);
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if (filterCondition != null) {
            filterCondition.setEvaluatable(tableFilter, b);
        }
        super.setEvaluatable(tableFilter, b);
    }

    @Override
    protected void updateAggregate(Session session, SelectGroups groupData, int groupRowId) {
        if (filterCondition == null || filterCondition.getBooleanValue(session)) {
            ArrayList<SelectOrderBy> orderBy;
            if (over != null && (orderBy = over.getOrderBy()) != null) {
                updateOrderedAggregate(session, groupData, groupRowId, orderBy);
            } else {
                updateAggregate(session, getData(session, groupData, false, false));
            }
        }
    }

    /**
     * Updates an aggregate value.
     *
     * @param session
     *            the session
     * @param aggregateData
     *            aggregate data
     */
    protected abstract void updateAggregate(Session session, Object aggregateData);

    @Override
    protected void updateGroupAggregates(Session session, int stage) {
        if (filterCondition != null) {
            filterCondition.updateAggregate(session, stage);
        }
        super.updateGroupAggregates(session, stage);
    }

    @Override
    protected StringBuilder appendTailConditions(StringBuilder builder) {
        if (filterCondition != null) {
            builder.append(" FILTER (WHERE ").append(filterCondition.getSQL()).append(')');
        }
        return super.appendTailConditions(builder);
    }

}
