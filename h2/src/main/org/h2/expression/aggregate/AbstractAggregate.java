/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.ArrayList;
import java.util.HashMap;

import org.h2.api.ErrorCode;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectGroups;
import org.h2.command.dml.SelectOrderBy;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.result.SortOrder;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.ValueHashMap;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueInt;

/**
 * A base class for aggregates.
 */
public abstract class AbstractAggregate extends Expression {

    protected final Select select;

    protected final boolean distinct;

    protected Expression filterCondition;

    protected Window over;

    private SortOrder overOrderBySort;

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

    AbstractAggregate(Select select, boolean distinct) {
        this.select = select;
        this.distinct = distinct;
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
    public Expression optimize(Session session) {
        if (over != null) {
            ArrayList<SelectOrderBy> orderBy = over.getOrderBy();
            if (orderBy != null) {
                overOrderBySort = createOrder(session, orderBy, getNumExpressions());
            }
        }
        return this;
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

    @Override
    public void updateAggregate(Session session, boolean window) {
        if (window != (over != null)) {
            if (!window && select.isWindowQuery()) {
                updateGroupAggregates(session);
                if (filterCondition != null) {
                    filterCondition.updateAggregate(session, false);
                }
                over.updateAggregate(session, false);
            }
            return;
        }
        // TODO aggregates: check nested MIN(MAX(ID)) and so on
        // if (on != null) {
        // on.updateAggregate();
        // }
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
                over.updateAggregate(session, true);
            }
        }
        if (filterCondition != null) {
            if (!filterCondition.getBooleanValue(session)) {
                return;
            }
        }
        if (over != null) {
            ArrayList<SelectOrderBy> orderBy = over.getOrderBy();
            if (orderBy != null) {
                updateOrderedAggregate(session, groupData, groupRowId, orderBy);
                return;
            }
        }
        updateAggregate(session, getData(session, groupData, false, false));
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

    /**
     * Invoked when processing group stage of grouped window queries to update
     * arguments of this aggregate.
     *
     * @param session
     *            the session
     */
    protected abstract void updateGroupAggregates(Session session);

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

    protected Object getData(Session session, SelectGroups groupData, boolean ifExists, boolean forOrderBy) {
        Object data;
        if (over != null) {
            ValueArray key = over.getCurrentKey(session);
            if (key != null) {
                @SuppressWarnings("unchecked")
                ValueHashMap<Object> map = (ValueHashMap<Object>) groupData.getCurrentGroupExprData(this, true);
                if (map == null) {
                    if (ifExists) {
                        return null;
                    }
                    map = new ValueHashMap<>();
                    groupData.setCurrentGroupExprData(this, map, true);
                }
                PartitionData partition = (PartitionData) map.get(key);
                if (partition == null) {
                    if (ifExists) {
                        return null;
                    }
                    data = forOrderBy ? new ArrayList<>() : createAggregateData();
                    map.put(key, new PartitionData(data));
                } else {
                    data = partition.getData();
                }
            } else {
                PartitionData partition = (PartitionData) groupData.getCurrentGroupExprData(this, true);
                if (partition == null) {
                    if (ifExists) {
                        return null;
                    }
                    data = forOrderBy ? new ArrayList<>() : createAggregateData();
                    groupData.setCurrentGroupExprData(this, new PartitionData(data), true);
                } else {
                    data = partition.getData();
                }
            }
        } else {
            data = groupData.getCurrentGroupExprData(this, false);
            if (data == null) {
                if (ifExists) {
                    return null;
                }
                data = forOrderBy ? new ArrayList<>() : createAggregateData();
                groupData.setCurrentGroupExprData(this, data, false);
            }
        }
        return data;
    }

    protected abstract Object createAggregateData();

    @Override
    public Value getValue(Session session) {
        SelectGroups groupData = select.getGroupDataIfCurrent(over != null);
        if (groupData == null) {
            throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getSQL());
        }
        return over == null ? getAggregatedValue(session, getData(session, groupData, true, false))
                : getWindowResult(session, groupData);
    }

    private Value getWindowResult(Session session, SelectGroups groupData) {
        PartitionData partition;
        Object data;
        boolean forOrderBy = over.getOrderBy() != null;
        ValueArray key = over.getCurrentKey(session);
        if (key != null) {
            @SuppressWarnings("unchecked")
            ValueHashMap<Object> map = (ValueHashMap<Object>) groupData.getCurrentGroupExprData(this, true);
            if (map == null) {
                map = new ValueHashMap<>();
                groupData.setCurrentGroupExprData(this, map, true);
            }
            partition = (PartitionData) map.get(key);
            if (partition == null) {
                data = forOrderBy ? new ArrayList<>() : createAggregateData();
                partition = new PartitionData(data);
                map.put(key, partition);
            } else {
                data = partition.getData();
            }
        } else {
            partition = (PartitionData) groupData.getCurrentGroupExprData(this, true);
            if (partition == null) {
                data = forOrderBy ? new ArrayList<>() : createAggregateData();
                partition = new PartitionData(data);
                groupData.setCurrentGroupExprData(this, partition, true);
            } else {
                data = partition.getData();
            }
        }
        if (over.getOrderBy() != null) {
            return getOrderedResult(session, groupData, partition, data);
        }
        Value result = partition.getResult();
        if (result == null) {
            result = getAggregatedValue(session, data);
            partition.setResult(result);
        }
        return result;
    }

    protected abstract Value getAggregatedValue(Session session, Object aggregateData);

    private void updateOrderedAggregate(Session session, SelectGroups groupData, int groupRowId,
            ArrayList<SelectOrderBy> orderBy) {
        int ne = getNumExpressions();
        int size = orderBy.size();
        Value[] array = new Value[ne + size + 1];
        rememberExpressions(session, array);
        for (int i = 0; i < size; i++) {
            SelectOrderBy o = orderBy.get(i);
            array[ne++] = o.expression.getValue(session);
        }
        array[ne] = ValueInt.get(groupRowId);
        @SuppressWarnings("unchecked")
        ArrayList<Value[]> data = (ArrayList<Value[]>) getData(session, groupData, false, true);
        data.add(array);
        return;
    }

    private Value getOrderedResult(Session session, SelectGroups groupData, PartitionData partition, Object data) {
        HashMap<Integer, Value> result = partition.getOrderedResult();
        if (result == null) {
            result = new HashMap<>();
            @SuppressWarnings("unchecked")
            ArrayList<Value[]> orderedData = (ArrayList<Value[]>) data;
            int ne = getNumExpressions();
            int last = ne + over.getOrderBy().size();
            orderedData.sort(overOrderBySort);
            Object aggregateData = createAggregateData();
            for (Value[] row : orderedData) {
                updateFromExpressions(session, aggregateData, row);
                result.put(row[last].getInt(), getAggregatedValue(session, aggregateData));
            }
        }
        return result.get(groupData.getCurrentGroupRowId());
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
