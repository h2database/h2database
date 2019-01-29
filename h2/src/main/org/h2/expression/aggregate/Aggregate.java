/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.h2.api.ErrorCode;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectOrderBy;
import org.h2.engine.Database;
import org.h2.engine.Mode;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Subquery;
import org.h2.expression.analysis.Window;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.mvstore.db.MVSpatialIndex;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StatementBuilder;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * Implements the integrated aggregate functions, such as COUNT, MAX, SUM.
 */
public class Aggregate extends AbstractAggregate {

    private static final HashMap<String, AggregateType> AGGREGATES = new HashMap<>(64);

    private final AggregateType aggregateType;

    private Expression on;
    private Expression groupConcatSeparator;
    private ArrayList<SelectOrderBy> orderByList;
    private SortOrder orderBySort;
    private TypeInfo type;

    /**
     * Create a new aggregate object.
     *
     * @param aggregateType
     *            the aggregate type
     * @param on
     *            the aggregated expression
     * @param select
     *            the select statement
     * @param distinct
     *            if distinct is used
     */
    public Aggregate(AggregateType aggregateType, Expression on, Select select, boolean distinct) {
        super(select, distinct);
        if (distinct && aggregateType == AggregateType.COUNT_ALL) {
            throw DbException.throwInternalError();
        }
        this.aggregateType = aggregateType;
        this.on = on;
    }

    static {
        /*
         * Update initial size of AGGREGATES after editing the following list.
         */
        addAggregate("COUNT", AggregateType.COUNT);
        addAggregate("SUM", AggregateType.SUM);
        addAggregate("MIN", AggregateType.MIN);
        addAggregate("MAX", AggregateType.MAX);
        addAggregate("AVG", AggregateType.AVG);
        addAggregate("GROUP_CONCAT", AggregateType.GROUP_CONCAT);
        // PostgreSQL compatibility: string_agg(expression, delimiter)
        addAggregate("STRING_AGG", AggregateType.GROUP_CONCAT);
        addAggregate("STDDEV_SAMP", AggregateType.STDDEV_SAMP);
        addAggregate("STDDEV", AggregateType.STDDEV_SAMP);
        addAggregate("STDDEV_POP", AggregateType.STDDEV_POP);
        addAggregate("STDDEVP", AggregateType.STDDEV_POP);
        addAggregate("VAR_POP", AggregateType.VAR_POP);
        addAggregate("VARP", AggregateType.VAR_POP);
        addAggregate("VAR_SAMP", AggregateType.VAR_SAMP);
        addAggregate("VAR", AggregateType.VAR_SAMP);
        addAggregate("VARIANCE", AggregateType.VAR_SAMP);
        addAggregate("ANY", AggregateType.ANY);
        addAggregate("SOME", AggregateType.ANY);
        // PostgreSQL compatibility
        addAggregate("BOOL_OR", AggregateType.ANY);
        addAggregate("EVERY", AggregateType.EVERY);
        // PostgreSQL compatibility
        addAggregate("BOOL_AND", AggregateType.EVERY);
        addAggregate("SELECTIVITY", AggregateType.SELECTIVITY);
        addAggregate("HISTOGRAM", AggregateType.HISTOGRAM);
        addAggregate("BIT_OR", AggregateType.BIT_OR);
        addAggregate("BIT_AND", AggregateType.BIT_AND);
        addAggregate("PERCENTILE_DISC", AggregateType.PERCENTILE_DISC);
        addAggregate("MEDIAN", AggregateType.MEDIAN);
        addAggregate("ARRAY_AGG", AggregateType.ARRAY_AGG);
        addAggregate("MODE", AggregateType.MODE);
        // Oracle compatibility
        addAggregate("STATS_MODE", AggregateType.MODE);
        addAggregate("ENVELOPE", AggregateType.ENVELOPE);
    }

    private static void addAggregate(String name, AggregateType type) {
        AGGREGATES.put(name, type);
    }

    /**
     * Get the aggregate type for this name, or -1 if no aggregate has been
     * found.
     *
     * @param name
     *            the aggregate function name
     * @return null if no aggregate function has been found, or the aggregate
     *         type
     */
    public static AggregateType getAggregateType(String name) {
        return AGGREGATES.get(name);
    }

    /**
     * Set the order for ARRAY_AGG() or GROUP_CONCAT() aggregate.
     *
     * @param orderByList
     *            the order by list
     */
    public void setOrderByList(ArrayList<SelectOrderBy> orderByList) {
        this.orderByList = orderByList;
    }

    /**
     * Set the separator for the GROUP_CONCAT() aggregate.
     *
     * @param separator
     *            the separator expression
     */
    public void setGroupConcatSeparator(Expression separator) {
        this.groupConcatSeparator = separator;
    }

    /**
     * Returns the type of this aggregate.
     *
     * @return the type of this aggregate
     */
    public AggregateType getAggregateType() {
        return aggregateType;
    }

    private void sortWithOrderBy(Value[] array) {
        final SortOrder sortOrder = orderBySort;
        if (sortOrder != null) {
            Arrays.sort(array, new Comparator<Value>() {
                @Override
                public int compare(Value v1, Value v2) {
                    return sortOrder.compare(((ValueArray) v1).getList(), ((ValueArray) v2).getList());
                }
            });
        } else {
            Arrays.sort(array, select.getSession().getDatabase().getCompareMode());
        }
    }

    @Override
    protected void updateAggregate(Session session, Object aggregateData) {
        AggregateData data = (AggregateData) aggregateData;
        Value v = on == null ? null : on.getValue(session);
        updateData(session, data, v, null);
    }

    private void updateData(Session session, AggregateData data, Value v, Value[] remembered) {
        switch (aggregateType) {
        case GROUP_CONCAT:
            if (v != ValueNull.INSTANCE) {
                v = updateCollecting(session, v.convertTo(Value.STRING), remembered);
            }
            break;
        case ARRAY_AGG:
            if (v != ValueNull.INSTANCE) {
                v = updateCollecting(session, v, remembered);
            }
            break;
        case PERCENTILE_DISC:
            ((AggregateDataCollecting) data).setSharedArgument(v);
            v = remembered != null ? remembered[1] : orderByList.get(0).expression.getValue(session);
            break;
        case MODE:
            v = remembered != null ? remembered[0] : orderByList.get(0).expression.getValue(session);
            break;
        default:
            // Use argument as is
        }
        data.add(session.getDatabase(), v);
    }

    @Override
    protected void updateGroupAggregates(Session session, int stage) {
        super.updateGroupAggregates(session, stage);
        if (on != null) {
            on.updateAggregate(session, stage);
        }
        if (orderByList != null) {
            for (SelectOrderBy orderBy : orderByList) {
                orderBy.expression.updateAggregate(session, stage);
            }
        }
    }

    private Value updateCollecting(Session session, Value v, Value[] remembered) {
        if (orderByList != null) {
            int size = orderByList.size();
            Value[] array = new Value[1 + size];
            array[0] = v;
            if (remembered == null) {
                for (int i = 0; i < size; i++) {
                    SelectOrderBy o = orderByList.get(i);
                    array[i + 1] = o.expression.getValue(session);
                }
            } else {
                System.arraycopy(remembered, 1, array, 1, size);
            }
            v = ValueArray.get(array);
        }
        return v;
    }

    @Override
    protected int getNumExpressions() {
        int n = on != null ? 1 : 0;
        if (orderByList != null) {
            n += orderByList.size();
        }
        if (filterCondition != null) {
            n++;
        }
        return n;
    }

    @Override
    protected void rememberExpressions(Session session, Value[] array) {
        int offset = 0;
        if (on != null) {
            array[offset++] = on.getValue(session);
        }
        if (orderByList != null) {
            for (SelectOrderBy o : orderByList) {
                array[offset++] = o.expression.getValue(session);
            }
        }
        if (filterCondition != null) {
            array[offset] = ValueBoolean.get(filterCondition.getBooleanValue(session));
        }
    }

    @Override
    protected void updateFromExpressions(Session session, Object aggregateData, Value[] array) {
        if (filterCondition == null || array[getNumExpressions() - 1].getBoolean()) {
            AggregateData data = (AggregateData) aggregateData;
            Value v = on == null ? null : array[0];
            updateData(session, data, v, array);
        }
    }

    @Override
    protected Object createAggregateData() {
        return AggregateData.create(aggregateType, distinct, type.getValueType());
    }

    @Override
    public Value getValue(Session session) {
        return select.isQuickAggregateQuery() ? getValueQuick(session) : super.getValue(session);
    }

    private Value getValueQuick(Session session) {
        switch (aggregateType) {
        case COUNT:
        case COUNT_ALL:
            Table table = select.getTopTableFilter().getTable();
            return ValueLong.get(table.getRowCount(session));
        case MIN:
        case MAX: {
            boolean first = aggregateType == AggregateType.MIN;
            Index index = getMinMaxColumnIndex();
            int sortType = index.getIndexColumns()[0].sortType;
            if ((sortType & SortOrder.DESCENDING) != 0) {
                first = !first;
            }
            Cursor cursor = index.findFirstOrLast(session, first);
            SearchRow row = cursor.getSearchRow();
            Value v;
            if (row == null) {
                v = ValueNull.INSTANCE;
            } else {
                v = row.getValue(index.getColumns()[0].getColumnId());
            }
            return v;
        }
        case PERCENTILE_DISC: {
            Value v = on.getValue(session);
            if (v == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            double arg = v.getDouble();
            if (arg >= 0d && arg <= 1d) {
                return Percentile.getFromIndex(session, orderByList.get(0).expression, type.getValueType(),
                        orderByList, arg, false);
            } else {
                throw DbException.getInvalidValueException("PERCENTILE_DISC argument", arg);
            }
        }
        case MEDIAN:
            return Percentile.getFromIndex(session, on, type.getValueType(), orderByList, 0.5d, true);
        case ENVELOPE:
            return ((MVSpatialIndex) AggregateDataEnvelope.getGeometryColumnIndex(on)).getBounds(session);
        default:
            throw DbException.throwInternalError("type=" + aggregateType);
        }
    }

    @Override
    public Value getAggregatedValue(Session session, Object aggregateData) {
        AggregateData data = (AggregateData) aggregateData;
        if (data == null) {
            data = (AggregateData) createAggregateData();
        }
        switch (aggregateType) {
        case COUNT:
            if (distinct) {
                return ValueLong.get(((AggregateDataCollecting) data).getCount());
            }
            break;
        case SUM:
        case AVG:
        case STDDEV_POP:
        case STDDEV_SAMP:
        case VAR_POP:
        case VAR_SAMP:
            if (distinct) {
                AggregateDataCollecting c = ((AggregateDataCollecting) data);
                if (c.getCount() == 0) {
                    return ValueNull.INSTANCE;
                }
                AggregateDataDefault d = new AggregateDataDefault(aggregateType, type.getValueType());
                Database db = session.getDatabase();
                int dataType = type.getValueType();
                for (Value v : c) {
                    d.add(db, v);
                }
                return d.getValue(db, dataType);
            }
            break;
        case HISTOGRAM:
            return getHistogram(session, data);
        case GROUP_CONCAT:
            return getGroupConcat(session, data);
        case ARRAY_AGG: {
            Value[] array = ((AggregateDataCollecting) data).getArray();
            if (array == null) {
                return ValueNull.INSTANCE;
            }
            if (orderByList != null || distinct) {
                sortWithOrderBy(array);
            }
            if (orderByList != null) {
                for (int i = 0; i < array.length; i++) {
                    array[i] = ((ValueArray) array[i]).getList()[0];
                }
            }
            return ValueArray.get(array);
        }
        case PERCENTILE_DISC: {
            AggregateDataCollecting collectingData = (AggregateDataCollecting) data;
            Value[] array = collectingData.getArray();
            if (array == null) {
                return ValueNull.INSTANCE;
            }
            Value v = collectingData.getSharedArgument();
            if (v == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            double arg = v.getDouble();
            if (arg >= 0d && arg <= 1d) {
                return Percentile.getValue(session.getDatabase(), array, type.getValueType(), orderByList, arg, false);
            } else {
                throw DbException.getInvalidValueException("PERCENTILE_DISC argument", arg);
            }
        }
        case MEDIAN: {
            Value[] array = ((AggregateDataCollecting) data).getArray();
            if (array == null) {
                return ValueNull.INSTANCE;
            }
            return Percentile.getValue(session.getDatabase(), array, type.getValueType(), orderByList, 0.5d, true);
        }
        case MODE:
            return getMode(session, data);
        default:
            // Avoid compiler warning
        }
        return data.getValue(session.getDatabase(), type.getValueType());
    }

    private Value getGroupConcat(Session session, AggregateData data) {
        Value[] array = ((AggregateDataCollecting) data).getArray();
        if (array == null) {
            return ValueNull.INSTANCE;
        }
        if (orderByList != null || distinct) {
            sortWithOrderBy(array);
        }
        StatementBuilder buff = new StatementBuilder();
        String sep = groupConcatSeparator == null ? "," : groupConcatSeparator.getValue(session).getString();
        for (Value val : array) {
            String s;
            if (val.getValueType() == Value.ARRAY) {
                s = ((ValueArray) val).getList()[0].getString();
            } else {
                s = val.getString();
            }
            if (s == null) {
                continue;
            }
            if (sep != null) {
                buff.appendExceptFirst(sep);
            }
            buff.append(s);
        }
        return ValueString.get(buff.toString());
    }

    private Value getHistogram(Session session, AggregateData data) {
        TreeMap<Value, LongDataCounter> distinctValues = ((AggregateDataDistinctWithCounts) data).getValues();
        if (distinctValues == null) {
            return ValueArray.getEmpty();
        }
        ValueArray[] values = new ValueArray[distinctValues.size()];
        int i = 0;
        for (Entry<Value, LongDataCounter> entry : distinctValues.entrySet()) {
            LongDataCounter d = entry.getValue();
            values[i] = ValueArray.get(new Value[] { entry.getKey(), ValueLong.get(distinct ? 1L : d.count) });
            i++;
        }
        Database db = session.getDatabase();
        final Mode mode = db.getMode();
        final CompareMode compareMode = db.getCompareMode();
        Arrays.sort(values, new Comparator<ValueArray>() {
            @Override
            public int compare(ValueArray v1, ValueArray v2) {
                Value a1 = v1.getList()[0];
                Value a2 = v2.getList()[0];
                return a1.compareTo(a2, mode, compareMode);
            }
        });
        return ValueArray.get(values);
    }

    private Value getMode(Session session, AggregateData data) {
        Value v = ValueNull.INSTANCE;
        TreeMap<Value, LongDataCounter> distinctValues = ((AggregateDataDistinctWithCounts) data).getValues();
        if (distinctValues == null) {
            return v;
        }
        long count = 0L;
        if (orderByList != null) {
            boolean desc = (orderByList.get(0).sortType & SortOrder.DESCENDING) != 0;
            for (Entry<Value, LongDataCounter> entry : distinctValues.entrySet()) {
                long c = entry.getValue().count;
                if (c > count) {
                    v = entry.getKey();
                    count = c;
                } else if (c == count) {
                    Value v2 = entry.getKey();
                    int cmp = session.getDatabase().compareTypeSafe(v, v2);
                    if (desc) {
                        if (cmp >= 0) {
                            continue;
                        }
                    } else if (cmp <= 0) {
                        continue;
                    }
                    v = v2;
                }
            }
        } else {
            for (Entry<Value, LongDataCounter> entry : distinctValues.entrySet()) {
                long c = entry.getValue().count;
                if (c > count) {
                    v = entry.getKey();
                    count = c;
                }
            }
        }
        return v.convertTo(type.getValueType());
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    @Override
    public void mapColumnsAnalysis(ColumnResolver resolver, int level, int innerState) {
        if (on != null) {
            on.mapColumns(resolver, level, innerState);
        }
        if (orderByList != null) {
            for (SelectOrderBy o : orderByList) {
                o.expression.mapColumns(resolver, level, innerState);
            }
        }
        if (groupConcatSeparator != null) {
            groupConcatSeparator.mapColumns(resolver, level, innerState);
        }
        super.mapColumnsAnalysis(resolver, level, innerState);
    }

    @Override
    public Expression optimize(Session session) {
        super.optimize(session);
        if (on != null) {
            on = on.optimize(session);
            type = on.getType();
        }
        if (orderByList != null) {
            for (SelectOrderBy o : orderByList) {
                o.expression = o.expression.optimize(session);
            }
            orderBySort = createOrder(session, orderByList, 1);
        }
        if (groupConcatSeparator != null) {
            groupConcatSeparator = groupConcatSeparator.optimize(session);
        }
        switch (aggregateType) {
        case GROUP_CONCAT:
            type = TypeInfo.TYPE_STRING_DEFAULT;
            break;
        case COUNT_ALL:
        case COUNT:
            type = TypeInfo.TYPE_LONG;
            break;
        case SELECTIVITY:
            type = TypeInfo.TYPE_INT;
            break;
        case HISTOGRAM:
            type = TypeInfo.TYPE_ARRAY;
            break;
        case SUM: {
            int dataType = type.getValueType();
            if (dataType == Value.BOOLEAN) {
                // example: sum(id > 3) (count the rows)
                type = TypeInfo.TYPE_LONG;
            } else if (!DataType.supportsAdd(dataType)) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            } else {
                type = TypeInfo.getTypeInfo(DataType.getAddProofType(dataType));
            }
            break;
        }
        case AVG:
            if (!DataType.supportsAdd(type.getValueType())) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            }
            break;
        case MIN:
        case MAX:
        case MEDIAN:
            break;
        case PERCENTILE_DISC:
        case MODE:
            type = orderByList.get(0).expression.getType();
            break;
        case STDDEV_POP:
        case STDDEV_SAMP:
        case VAR_POP:
        case VAR_SAMP:
            type = TypeInfo.TYPE_DOUBLE;
            break;
        case EVERY:
        case ANY:
            type = TypeInfo.TYPE_BOOLEAN;
            break;
        case BIT_AND:
        case BIT_OR:
            if (!DataType.supportsAdd(type.getValueType())) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            }
            break;
        case ARRAY_AGG:
            type = TypeInfo.TYPE_ARRAY;
            break;
        case ENVELOPE:
            type = TypeInfo.TYPE_GEOMETRY;
            break;
        default:
            DbException.throwInternalError("type=" + aggregateType);
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if (on != null) {
            on.setEvaluatable(tableFilter, b);
        }
        if (orderByList != null) {
            for (SelectOrderBy o : orderByList) {
                o.expression.setEvaluatable(tableFilter, b);
            }
        }
        if (groupConcatSeparator != null) {
            groupConcatSeparator.setEvaluatable(tableFilter, b);
        }
        super.setEvaluatable(tableFilter, b);
    }

    private StringBuilder getSQLGroupConcat(StringBuilder builder) {
        builder.append("GROUP_CONCAT(");
        if (distinct) {
            builder.append("DISTINCT ");
        }
        on.getSQL(builder);
        Window.appendOrderBy(builder, orderByList);
        if (groupConcatSeparator != null) {
            builder.append(" SEPARATOR ");
            groupConcatSeparator.getSQL(builder);
        }
        builder.append(')');
        return appendTailConditions(builder);
    }

    private StringBuilder getSQLArrayAggregate(StringBuilder builder) {
        builder.append("ARRAY_AGG(");
        if (distinct) {
            builder.append("DISTINCT ");
        }
        on.getSQL(builder);
        Window.appendOrderBy(builder, orderByList);
        builder.append(')');
        return appendTailConditions(builder);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        String text;
        switch (aggregateType) {
        case GROUP_CONCAT:
            return getSQLGroupConcat(builder);
        case COUNT_ALL:
            return appendTailConditions(builder.append("COUNT(*)"));
        case COUNT:
            text = "COUNT";
            break;
        case SELECTIVITY:
            text = "SELECTIVITY";
            break;
        case HISTOGRAM:
            text = "HISTOGRAM";
            break;
        case SUM:
            text = "SUM";
            break;
        case MIN:
            text = "MIN";
            break;
        case MAX:
            text = "MAX";
            break;
        case AVG:
            text = "AVG";
            break;
        case STDDEV_POP:
            text = "STDDEV_POP";
            break;
        case STDDEV_SAMP:
            text = "STDDEV_SAMP";
            break;
        case VAR_POP:
            text = "VAR_POP";
            break;
        case VAR_SAMP:
            text = "VAR_SAMP";
            break;
        case EVERY:
            text = "EVERY";
            break;
        case ANY:
            text = "ANY";
            break;
        case BIT_AND:
            text = "BIT_AND";
            break;
        case BIT_OR:
            text = "BIT_OR";
            break;
        case PERCENTILE_DISC:
            text = "PERCENTILE_DISC";
            break;
        case MEDIAN:
            text = "MEDIAN";
            break;
        case ARRAY_AGG:
            return getSQLArrayAggregate(builder);
        case MODE:
            text = "MODE";
            break;
        case ENVELOPE:
            text = "ENVELOPE";
            break;
        default:
            throw DbException.throwInternalError("type=" + aggregateType);
        }
        builder.append(text);
        if (distinct) {
            builder.append("(DISTINCT ");
            on.getSQL(builder).append(')');
        } else {
            builder.append('(');
            if (on != null) {
                if (on instanceof Subquery) {
                    on.getSQL(builder);
                } else {
                    on.getUnenclosedSQL(builder);
                }
            }
            builder.append(')');
        }
        if (orderByList != null) {
            builder.append(" WITHIN GROUP (");
            Window.appendOrderBy(builder, orderByList);
            builder.append(')');
        }
        return appendTailConditions(builder);
    }

    private Index getMinMaxColumnIndex() {
        if (on instanceof ExpressionColumn) {
            ExpressionColumn col = (ExpressionColumn) on;
            Column column = col.getColumn();
            TableFilter filter = col.getTableFilter();
            if (filter != null) {
                Table table = filter.getTable();
                return table.getIndexForColumn(column, true, false);
            }
        }
        return null;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (!super.isEverything(visitor)) {
            return false;
        }
        if (filterCondition != null && !filterCondition.isEverything(visitor)) {
            return false;
        }
        if (visitor.getType() == ExpressionVisitor.OPTIMIZABLE_AGGREGATE) {
            switch (aggregateType) {
            case COUNT:
                if (!distinct && on.getNullable() == Column.NOT_NULLABLE) {
                    return visitor.getTable().canGetRowCount();
                }
                return false;
            case COUNT_ALL:
                return visitor.getTable().canGetRowCount();
            case MIN:
            case MAX:
                Index index = getMinMaxColumnIndex();
                return index != null;
            case PERCENTILE_DISC:
                return on.isConstant() && Percentile.getColumnIndex(orderByList.get(0).expression) != null;
            case MEDIAN:
                if (distinct) {
                    return false;
                }
                return Percentile.getColumnIndex(on) != null;
            case ENVELOPE:
                return AggregateDataEnvelope.getGeometryColumnIndex(on) != null;
            default:
                return false;
            }
        }
        if (on != null && !on.isEverything(visitor)) {
            return false;
        }
        if (groupConcatSeparator != null && !groupConcatSeparator.isEverything(visitor)) {
            return false;
        }
        if (orderByList != null) {
            for (SelectOrderBy o : orderByList) {
                if (!o.expression.isEverything(visitor)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int getCost() {
        int cost = 1;
        if (on != null) {
            cost += on.getCost();
        }
        if (filterCondition != null) {
            cost += filterCondition.getCost();
        }
        return cost;
    }

}
