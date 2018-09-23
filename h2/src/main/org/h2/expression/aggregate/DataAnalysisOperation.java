/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.h2.api.ErrorCode;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectGroups;
import org.h2.command.dml.SelectOrderBy;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.message.DbException;
import org.h2.result.SortOrder;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueInt;

/**
 * A base class for data analysis operations such as aggregates and window
 * functions.
 */
public abstract class DataAnalysisOperation extends Expression {

    protected final Select select;

    protected Window over;

    protected SortOrder overOrderBySort;

    private int lastGroupRowId;

    protected static SortOrder createOrder(Session session, ArrayList<SelectOrderBy> orderBy, int offset) {
        int size = orderBy.size();
        int[] index = new int[size];
        int[] sortType = new int[size];
        for (int i = 0; i < size; i++) {
            SelectOrderBy o = orderBy.get(i);
            index[i] = i + offset;
            sortType[i] = o.sortType;
        }
        return new SortOrder(session.getDatabase(), index, sortType, null);
    }

    DataAnalysisOperation(Select select) {
        this.select = select;
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

    /**
     * Checks whether this expression is an aggregate function.
     *
     * @return true if this is an aggregate function (including aggregates with
     *         OVER clause), false if this is a window function
     */
    public abstract boolean isAggregate();

    /**
     * Returns the sort order for OVER clause.
     *
     * @return the sort order for OVER clause
     */
    SortOrder getOverOrderBySort() {
        return overOrderBySort;
    }

    @Override
    public final void mapColumns(ColumnResolver resolver, int level, int state) {
        if (over != null) {
            if (state != MAP_INITIAL) {
                throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getSQL());
            }
            state = MAP_IN_WINDOW;
        } else {
            if (state == MAP_IN_AGGREGATE) {
                throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getSQL());
            }
            state = MAP_IN_AGGREGATE;
        }
        mapColumnsAnalysis(resolver, level, state);
    }

    protected void mapColumnsAnalysis(ColumnResolver resolver, int level, int innerState) {
        if (over != null) {
            over.mapColumns(resolver, level);
        }
    }

    @Override
    public Expression optimize(Session session) {
        if (over != null) {
            over.optimize(session);
            ArrayList<SelectOrderBy> orderBy = over.getOrderBy();
            if (orderBy != null) {
                overOrderBySort = createOrder(session, orderBy, getNumExpressions());
            } else if (!isAggregate()) {
                overOrderBySort = new SortOrder(session.getDatabase(), new int[getNumExpressions()], new int[0], null);
            }
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if (over != null) {
            over.setEvaluatable(tableFilter, b);
        }
    }

    @Override
    public final void updateAggregate(Session session, int stage) {
        if (stage == Aggregate.STAGE_RESET) {
            updateGroupAggregates(session, Aggregate.STAGE_RESET);
            lastGroupRowId = 0;
            return;
        }
        boolean window = stage == Aggregate.STAGE_WINDOW;
        if (window != (over != null)) {
            if (!window && select.isWindowQuery()) {
                updateGroupAggregates(session, stage);
            }
            return;
        }
        SelectGroups groupData = select.getGroupDataIfCurrent(window);
        if (groupData == null) {
            // this is a different level (the enclosing query)
            return;
        }

        int groupRowId = groupData.getCurrentGroupRowId();
        if (lastGroupRowId == groupRowId) {
            // already visited
            return;
        }
        lastGroupRowId = groupRowId;

        if (over != null) {
            if (!select.isGroupQuery()) {
                over.updateAggregate(session, stage);
            }
        }
        updateAggregate(session, groupData, groupRowId);
    }

    protected abstract void updateAggregate(Session session, SelectGroups groupData, int groupRowId);

    /**
     * Invoked when processing group stage of grouped window queries to update
     * arguments of this aggregate.
     *
     * @param session
     *            the session
     * @param stage
     *            select stage
     */
    protected void updateGroupAggregates(Session session, int stage) {
        if (over != null) {
            over.updateAggregate(session, stage);
        }
    }

    /**
     * Returns the number of expressions, excluding FILTER and OVER clauses.
     *
     * @return the number of expressions
     */
    protected abstract int getNumExpressions();

    /**
     * Stores current values of expressions into the specified array.
     *
     * @param session
     *            the session
     * @param array
     *            array to store values of expressions
     */
    protected abstract void rememberExpressions(Session session, Value[] array);

    protected Object getWindowData(Session session, SelectGroups groupData, boolean forOrderBy) {
        Object data;
        Value key = over.getCurrentKey(session);
        PartitionData partition = groupData.getWindowExprData(this, key);
        if (partition == null) {
            data = forOrderBy ? new ArrayList<>() : createAggregateData();
            groupData.setWindowExprData(this, key, new PartitionData(data));
        } else {
            data = partition.getData();
        }
        return data;
    }

    protected Object getGroupData(SelectGroups groupData, boolean ifExists) {
        Object data;
        data = groupData.getCurrentGroupExprData(this);
        if (data == null) {
            if (ifExists) {
                return null;
            }
            data = createAggregateData();
            groupData.setCurrentGroupExprData(this, data);
        }
        return data;
    }

    protected abstract Object createAggregateData();

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (over == null) {
            return true;
        }
        switch (visitor.getType()) {
        case ExpressionVisitor.QUERY_COMPARABLE:
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.INDEPENDENT:
            return false;
        case ExpressionVisitor.EVALUATABLE:
        case ExpressionVisitor.READONLY:
        case ExpressionVisitor.NOT_FROM_RESOLVER:
        case ExpressionVisitor.GET_DEPENDENCIES:
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
        case ExpressionVisitor.GET_COLUMNS1:
        case ExpressionVisitor.GET_COLUMNS2:
            return true;
        default:
            throw DbException.throwInternalError("type=" + visitor.getType());
        }
    }

    @Override
    public Value getValue(Session session) {
        SelectGroups groupData = select.getGroupDataIfCurrent(over != null);
        if (groupData == null) {
            throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getSQL());
        }
        return over == null ? getAggregatedValue(session, getGroupData(groupData, true))
                : getWindowResult(session, groupData);
    }

    /**
     * Returns result of this window function or window aggregate. This method
     * is not used for plain aggregates.
     *
     * @param session
     *            the session
     * @param groupData
     *            the group data
     * @return result of this function
     */
    private Value getWindowResult(Session session, SelectGroups groupData) {
        PartitionData partition;
        Object data;
        boolean forOrderBy = over.getOrderBy() != null;
        Value key = over.getCurrentKey(session);
        partition = groupData.getWindowExprData(this, key);
        if (partition == null) {
            // Window aggregates with FILTER clause may have no collected values
            data = forOrderBy ? new ArrayList<>() : createAggregateData();
            partition = new PartitionData(data);
            groupData.setWindowExprData(this, key, partition);
        } else {
            data = partition.getData();
        }
        if (forOrderBy || !isAggregate()) {
            return getOrderedResult(session, groupData, partition, data);
        }
        // Window aggregate without ORDER BY clause in window specification
        Value result = partition.getResult();
        if (result == null) {
            result = getAggregatedValue(session, data);
            partition.setResult(result);
        }
        return result;
    }

    /***
     * Returns aggregated value.
     *
     * @param session
     *            the session
     * @param aggregateData
     *            the aggregate data
     * @return aggregated value.
     */
    protected abstract Value getAggregatedValue(Session session, Object aggregateData);

    protected void updateOrderedAggregate(Session session, SelectGroups groupData, int groupRowId,
            ArrayList<SelectOrderBy> orderBy) {
        int ne = getNumExpressions();
        int size = orderBy != null ? orderBy.size() : 0;
        Value[] array = new Value[ne + size + 1];
        rememberExpressions(session, array);
        for (int i = 0; i < size; i++) {
            @SuppressWarnings("null")
            SelectOrderBy o = orderBy.get(i);
            array[ne++] = o.expression.getValue(session);
        }
        array[ne] = ValueInt.get(groupRowId);
        @SuppressWarnings("unchecked")
        ArrayList<Value[]> data = (ArrayList<Value[]>) getWindowData(session, groupData, true);
        data.add(array);
    }

    private Value getOrderedResult(Session session, SelectGroups groupData, PartitionData partition, Object data) {
        HashMap<Integer, Value> result = partition.getOrderedResult();
        if (result == null) {
            result = new HashMap<>();
            @SuppressWarnings("unchecked")
            ArrayList<Value[]> orderedData = (ArrayList<Value[]>) data;
            int rowIdColumn = getNumExpressions();
            ArrayList<SelectOrderBy> orderBy = over.getOrderBy();
            if (orderBy != null) {
                rowIdColumn += orderBy.size();
                Collections.sort(orderedData, overOrderBySort);
            }
            getOrderedResultLoop(session, result, orderedData, rowIdColumn);
            partition.setOrderedResult(result);
        }
        return result.get(groupData.getCurrentGroupRowId());
    }

    /**
     * @param session
     *            the session
     * @param result
     *            the map to append result to
     * @param ordered
     *            ordered data
     * @param rowIdColumn
     *            the index of row id value
     */
    protected abstract void getOrderedResultLoop(Session session, HashMap<Integer, Value> result,
            ArrayList<Value[]> ordered, int rowIdColumn);

    protected StringBuilder appendTailConditions(StringBuilder builder) {
        if (over != null) {
            builder.append(' ').append(over.getSQL());
        }
        return builder;
    }

}
