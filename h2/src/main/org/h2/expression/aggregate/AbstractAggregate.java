/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import org.h2.api.ErrorCode;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectGroups;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.ValueHashMap;
import org.h2.value.Value;
import org.h2.value.ValueArray;

/**
 * A base class for aggregates.
 */
public abstract class AbstractAggregate extends Expression {

    protected final Select select;

    protected final boolean distinct;

    protected Expression filterCondition;

    protected Window over;

    private int lastGroupRowId;

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
        updateAggregate(session, getData(session, groupData, false));
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

    protected Object getData(Session session, SelectGroups groupData, boolean ifExists) {
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
                    data = createAggregateData();
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
                    data = createAggregateData();
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
                data = createAggregateData();
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
        return over == null ? getAggregatedValue(session, getData(session, groupData, true))
                : getWindowResult(session, groupData);
    }

    private Value getWindowResult(Session session, SelectGroups groupData) {
        PartitionData partition;
        Object data;
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
                data = createAggregateData();
                partition = new PartitionData(data);
                map.put(key, partition);
            } else {
                data = partition.getData();
            }
        } else {
            partition = (PartitionData) groupData.getCurrentGroupExprData(this, true);
            if (partition == null) {
                data = createAggregateData();
                partition = new PartitionData(data);
                groupData.setCurrentGroupExprData(this, partition, true);
            } else {
                data = partition.getData();
            }
        }
        Value result = partition.getResult();
        if (result == null) {
            result = getAggregatedValue(session, data);
            partition.setResult(result);
        }
        return result;
    }

    protected abstract Value getAggregatedValue(Session session, Object aggregateData);

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
