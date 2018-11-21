/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.h2.command.dml.Select;
import org.h2.command.dml.SelectGroups;
import org.h2.command.dml.SelectOrderBy;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.analysis.DataAnalysisOperation;
import org.h2.expression.analysis.WindowFrame;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;

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
    public void mapColumnsAnalysis(ColumnResolver resolver, int level, int innerState) {
        if (filterCondition != null) {
            filterCondition.mapColumns(resolver, level, innerState);
        }
        super.mapColumnsAnalysis(resolver, level, innerState);
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
    protected void getOrderedResultLoop(Session session, HashMap<Integer, Value> result, ArrayList<Value[]> ordered,
            int rowIdColumn) {
        WindowFrame frame = over.getWindowFrame();
        if (frame == null) {
            if (over.getOrderBy() == null) {
                aggregateWholePartition(session, result, ordered, rowIdColumn);
                return;
            }
        } else if (frame.isFullPartition()) {
            aggregateWholePartition(session, result, ordered, rowIdColumn);
            return;
        }
        // All other types of frames (slow)
        int size = ordered.size();
        for (int i = 0; i < size; i++) {
            Object aggregateData = createAggregateData();
            for (Iterator<Value[]> iter = WindowFrame.iterator(over, session, ordered, getOverOrderBySort(), i,
                    false); iter.hasNext();) {
                updateFromExpressions(session, aggregateData, iter.next());
            }
            result.put(ordered.get(i)[rowIdColumn].getInt(), getAggregatedValue(session, aggregateData));
        }
    }

    private void aggregateWholePartition(Session session, HashMap<Integer, Value> result, ArrayList<Value[]> ordered,
            int rowIdColumn) {
        // Aggregate values from the whole partition
        Object aggregateData = createAggregateData();
        for (Value[] row : ordered) {
            updateFromExpressions(session, aggregateData, row);
        }
        // All rows have the same value
        Value value = getAggregatedValue(session, aggregateData);
        for (Value[] row : ordered) {
            result.put(row[rowIdColumn].getInt(), value);
        }
    }

    /**
     * Updates the provided aggregate data from the remembered expressions.
     *
     * @param session
     *            the session
     * @param aggregateData
     *            aggregate data
     * @param array
     *            values of expressions
     */
    protected abstract void updateFromExpressions(Session session, Object aggregateData, Value[] array);

    @Override
    protected void updateAggregate(Session session, SelectGroups groupData, int groupRowId) {
        if (filterCondition == null || filterCondition.getBooleanValue(session)) {
            ArrayList<SelectOrderBy> orderBy;
            if (over != null) {
                if ((orderBy = over.getOrderBy()) != null) {
                    updateOrderedAggregate(session, groupData, groupRowId, orderBy);
                } else {
                    updateAggregate(session, getWindowData(session, groupData, false));
                }
            } else {
                updateAggregate(session, getGroupData(groupData, false));
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
            builder.append(" FILTER (WHERE ");
            filterCondition.getSQL(builder).append(')');
        }
        return super.appendTailConditions(builder);
    }

}
