/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.analysis;

import java.util.ArrayList;
import java.util.HashMap;

import org.h2.api.ErrorCode;
import org.h2.command.query.QueryOrderBy;
import org.h2.command.query.Select;
import org.h2.command.query.SelectGroups;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.message.DbException;
import org.h2.result.SortOrder;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueInteger;

/**
 * A base class for data analysis operations such as aggregates and window
 * functions.
 */
public abstract class DataAnalysisOperation extends Expression {

    /**
     * Reset stage. Used to reset internal data to its initial state.
     */
    public static final int STAGE_RESET = 0;

    /**
     * Group stage, used for explicit or implicit GROUP BY operation.
     */
    public static final int STAGE_GROUP = 1;

    /**
     * Window processing stage.
     */
    public static final int STAGE_WINDOW = 2;

    /**
     * SELECT
     */
    protected final Select select;

    /**
     * OVER clause
     */
    protected Window over;

    /**
     * Sort order for OVER
     */
    protected SortOrder overOrderBySort;

    private int numFrameExpressions;

    private int lastGroupRowId;

    /**
     * Create sort order.
     *
     * @param session
     *            database session
     * @param orderBy
     *            array of order by expressions
     * @param offset
     *            index offset
     * @return the SortOrder
     */
    protected static SortOrder createOrder(SessionLocal session, ArrayList<QueryOrderBy> orderBy, int offset) {
        int size = orderBy.size();
        int[] index = new int[size];
        int[] sortType = new int[size];
        for (int i = 0; i < size; i++) {
            QueryOrderBy o = orderBy.get(i);
            index[i] = i + offset;
            sortType[i] = o.sortType;
        }
        return new SortOrder(session, index, sortType, null);
    }

    protected DataAnalysisOperation(Select select) {
        this.select = select;
    }

    /**
     * Returns the OVER condition.
     *
     * @return the OVER condition
     */
    public Window getOverCondition() {
        return over;
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
    protected SortOrder getOverOrderBySort() {
        return overOrderBySort;
    }

    @Override
    public final void mapColumns(ColumnResolver resolver, int level, int state) {
        if (over != null) {
            if (state != MAP_INITIAL) {
                throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getTraceSQL());
            }
            state = MAP_IN_WINDOW;
        } else {
            if (state == MAP_IN_AGGREGATE) {
                throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getTraceSQL());
            }
            state = MAP_IN_AGGREGATE;
        }
        mapColumnsAnalysis(resolver, level, state);
    }

    /**
     * Map the columns of the resolver to expression columns.
     *
     * @param resolver
     *            the column resolver
     * @param level
     *            the subquery nesting level
     * @param innerState
     *            one of the Expression MAP_IN_* values
     */
    protected void mapColumnsAnalysis(ColumnResolver resolver, int level, int innerState) {
        if (over != null) {
            over.mapColumns(resolver, level);
        }
    }

    @Override
    public Expression optimize(SessionLocal session) {
        if (over != null) {
            over.optimize(session);
            ArrayList<QueryOrderBy> orderBy = over.getOrderBy();
            if (orderBy != null) {
                overOrderBySort = createOrder(session, orderBy, getNumExpressions());
            } else if (!isAggregate()) {
                overOrderBySort = new SortOrder(session, new int[getNumExpressions()]);
            }
            WindowFrame frame = over.getWindowFrame();
            if (frame != null) {
                int index = getNumExpressions();
                int orderBySize = 0;
                if (orderBy != null) {
                    orderBySize = orderBy.size();
                    index += orderBySize;
                }
                int n = 0;
                WindowFrameBound bound = frame.getStarting();
                if (bound.isParameterized()) {
                    checkOrderBy(frame.getUnits(), orderBySize);
                    if (bound.isVariable()) {
                        bound.setExpressionIndex(index);
                        n++;
                    }
                }
                bound = frame.getFollowing();
                if (bound != null && bound.isParameterized()) {
                    checkOrderBy(frame.getUnits(), orderBySize);
                    if (bound.isVariable()) {
                        bound.setExpressionIndex(index + n);
                        n++;
                    }
                }
                numFrameExpressions = n;
            }
        }
        return this;
    }

    private void checkOrderBy(WindowFrameUnits units, int orderBySize) {
        switch (units) {
        case RANGE:
            if (orderBySize != 1) {
                String sql = getTraceSQL();
                throw DbException.getSyntaxError(sql, sql.length() - 1,
                        "exactly one sort key is required for RANGE units");
            }
            break;
        case GROUPS:
            if (orderBySize < 1) {
                String sql = getTraceSQL();
                throw DbException.getSyntaxError(sql, sql.length() - 1,
                        "a sort key is required for GROUPS units");
            }
            break;
        default:
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if (over != null) {
            over.setEvaluatable(tableFilter, b);
        }
    }

    @Override
    public final void updateAggregate(SessionLocal session, int stage) {
        if (stage == STAGE_RESET) {
            updateGroupAggregates(session, STAGE_RESET);
            lastGroupRowId = 0;
            return;
        }
        boolean window = stage == STAGE_WINDOW;
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

    /**
     * Update a row of an aggregate.
     *
     * @param session
     *            the database session
     * @param groupData
     *            data for the aggregate group
     * @param groupRowId
     *            row id of group
     */
    protected abstract void updateAggregate(SessionLocal session, SelectGroups groupData, int groupRowId);

    /**
     * Invoked when processing group stage of grouped window queries to update
     * arguments of this aggregate.
     *
     * @param session
     *            the session
     * @param stage
     *            select stage
     */
    protected void updateGroupAggregates(SessionLocal session, int stage) {
        if (over != null) {
            over.updateAggregate(session, stage);
        }
    }

    /**
     * Returns the number of expressions, excluding OVER clause.
     *
     * @return the number of expressions
     */
    protected abstract int getNumExpressions();

    /**
     * Returns the number of window frame expressions.
     *
     * @return the number of window frame expressions
     */
    private int getNumFrameExpressions() {
        return numFrameExpressions;
    }

    /**
     * Stores current values of expressions into the specified array.
     *
     * @param session
     *            the session
     * @param array
     *            array to store values of expressions
     */
    protected abstract void rememberExpressions(SessionLocal session, Value[] array);

    /**
     * Get the aggregate data for a window clause.
     *
     * @param session
     *            database session
     * @param groupData
     *            aggregate group data
     * @param forOrderBy
     *            true if this is for ORDER BY
     * @return the aggregate data object, specific to each kind of aggregate.
     */
    protected Object getWindowData(SessionLocal session, SelectGroups groupData, boolean forOrderBy) {
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

    /**
     * Get the aggregate group data object from the collector object.
     *
     * @param groupData
     *            the collector object
     * @param ifExists
     *            if true, return null if object not found, if false, return new
     *            object if nothing found
     * @return group data object
     */
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

    /**
     * Create aggregate data object specific to the subclass.
     *
     * @return aggregate-specific data object.
     */
    protected abstract Object createAggregateData();

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (over == null) {
            return true;
        }
        switch (visitor.getType()) {
        case ExpressionVisitor.QUERY_COMPARABLE:
        case ExpressionVisitor.OPTIMIZABLE_AGGREGATE:
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.INDEPENDENT:
        case ExpressionVisitor.DECREMENT_QUERY_LEVEL:
            return false;
        default:
            return true;
        }
    }

    @Override
    public Value getValue(SessionLocal session) {
        SelectGroups groupData = select.getGroupDataIfCurrent(over != null);
        if (groupData == null) {
            throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getTraceSQL());
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
    private Value getWindowResult(SessionLocal session, SelectGroups groupData) {
        PartitionData partition;
        Object data;
        boolean isOrdered = over.isOrdered();
        Value key = over.getCurrentKey(session);
        partition = groupData.getWindowExprData(this, key);
        if (partition == null) {
            // Window aggregates with FILTER clause may have no collected values
            data = isOrdered ? new ArrayList<>() : createAggregateData();
            partition = new PartitionData(data);
            groupData.setWindowExprData(this, key, partition);
        } else {
            data = partition.getData();
        }
        if (isOrdered || !isAggregate()) {
            Value result = getOrderedResult(session, groupData, partition, data);
            if (result == null) {
                return getAggregatedValue(session, null);
            }
            return result;
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
    protected abstract Value getAggregatedValue(SessionLocal session, Object aggregateData);

    /**
     * Update a row of an ordered aggregate.
     *
     * @param session
     *            the database session
     * @param groupData
     *            data for the aggregate group
     * @param groupRowId
     *            row id of group
     * @param orderBy
     *            list of order by expressions
     */
    protected void updateOrderedAggregate(SessionLocal session, SelectGroups groupData, int groupRowId,
            ArrayList<QueryOrderBy> orderBy) {
        int ne = getNumExpressions();
        int size = orderBy != null ? orderBy.size() : 0;
        int frameSize = getNumFrameExpressions();
        Value[] array = new Value[ne + size + frameSize + 1];
        rememberExpressions(session, array);
        for (int i = 0; i < size; i++) {
            @SuppressWarnings("null")
            QueryOrderBy o = orderBy.get(i);
            array[ne++] = o.expression.getValue(session);
        }
        if (frameSize > 0) {
            WindowFrame frame = over.getWindowFrame();
            WindowFrameBound bound = frame.getStarting();
            if (bound.isVariable()) {
                array[ne++] = bound.getValue().getValue(session);
            }
            bound = frame.getFollowing();
            if (bound != null && bound.isVariable()) {
                array[ne++] = bound.getValue().getValue(session);
            }
        }
        array[ne] = ValueInteger.get(groupRowId);
        @SuppressWarnings("unchecked")
        ArrayList<Value[]> data = (ArrayList<Value[]>) getWindowData(session, groupData, true);
        data.add(array);
    }

    private Value getOrderedResult(SessionLocal session, SelectGroups groupData, PartitionData partition, //
            Object data) {
        HashMap<Integer, Value> result = partition.getOrderedResult();
        if (result == null) {
            result = new HashMap<>();
            @SuppressWarnings("unchecked")
            ArrayList<Value[]> orderedData = (ArrayList<Value[]>) data;
            int rowIdColumn = getNumExpressions();
            ArrayList<QueryOrderBy> orderBy = over.getOrderBy();
            if (orderBy != null) {
                rowIdColumn += orderBy.size();
                orderedData.sort(overOrderBySort);
            }
            rowIdColumn += getNumFrameExpressions();
            getOrderedResultLoop(session, result, orderedData, rowIdColumn);
            partition.setOrderedResult(result);
        }
        return result.get(groupData.getCurrentGroupRowId());
    }

    /**
     * Returns result of this window function or window aggregate. This method
     * may not be called on window aggregate without window order clause.
     *
     * @param session
     *            the session
     * @param result
     *            the map to append result to
     * @param ordered
     *            ordered data
     * @param rowIdColumn
     *            the index of row id value
     */
    protected abstract void getOrderedResultLoop(SessionLocal session, HashMap<Integer, Value> result,
            ArrayList<Value[]> ordered, int rowIdColumn);

    /**
     * Used to create SQL for the OVER and FILTER clauses.
     *
     * @param builder
     *            string builder
     * @param sqlFlags
     *            formatting flags
     * @param forceOrderBy
     *            whether synthetic ORDER BY clause should be generated when it
     *            is missing
     * @return the builder object
     */
    protected StringBuilder appendTailConditions(StringBuilder builder, int sqlFlags, boolean forceOrderBy) {
        if (over != null) {
            builder.append(' ');
            over.getSQL(builder, sqlFlags, forceOrderBy);
        }
        return builder;
    }

}
