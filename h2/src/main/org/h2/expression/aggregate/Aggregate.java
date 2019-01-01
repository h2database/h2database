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
import org.h2.util.ValueHashMap;
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * Implements the integrated aggregate functions, such as COUNT, MAX, SUM.
 */
public class Aggregate extends AbstractAggregate {

    public enum AggregateType {
        /**
         * The aggregate type for COUNT(*).
         */
        COUNT_ALL,

        /**
         * The aggregate type for COUNT(expression).
         */
        COUNT,

        /**
         * The aggregate type for GROUP_CONCAT(...).
         */
        GROUP_CONCAT,

        /**
         * The aggregate type for SUM(expression).
         */
        SUM,

        /**
         * The aggregate type for MIN(expression).
         */
        MIN,

        /**
         * The aggregate type for MAX(expression).
         */
        MAX,

        /**
         * The aggregate type for AVG(expression).
         */
        AVG,

        /**
         * The aggregate type for STDDEV_POP(expression).
         */
        STDDEV_POP,

        /**
         * The aggregate type for STDDEV_SAMP(expression).
         */
        STDDEV_SAMP,

        /**
         * The aggregate type for VAR_POP(expression).
         */
        VAR_POP,

        /**
         * The aggregate type for VAR_SAMP(expression).
         */
        VAR_SAMP,

        /**
         * The aggregate type for ANY(expression).
         */
        ANY,

        /**
         * The aggregate type for EVERY(expression).
         */
        EVERY,

        /**
         * The aggregate type for BOOL_OR(expression).
         */
        BIT_OR,

        /**
         * The aggregate type for BOOL_AND(expression).
         */
        BIT_AND,

        /**
         * The aggregate type for SELECTIVITY(expression).
         */
        SELECTIVITY,

        /**
         * The aggregate type for HISTOGRAM(expression).
         */
        HISTOGRAM,

        /**
         * The aggregate type for MEDIAN(expression).
         */
        MEDIAN,

        /**
         * The aggregate type for ARRAY_AGG(expression).
         */
        ARRAY_AGG,

        /**
         * The aggregate type for MODE(expression).
         */
        MODE,

        /**
         * The aggregate type for ENVELOPE(expression).
         */
        ENVELOPE,
    }

    private static final HashMap<String, AggregateType> AGGREGATES = new HashMap<>(64);

    private final AggregateType type;

    private Expression on;
    private Expression groupConcatSeparator;
    private ArrayList<SelectOrderBy> orderByList;
    private SortOrder orderBySort;
    private int dataType, scale;
    private long precision;
    private int displaySize;

    /**
     * Create a new aggregate object.
     *
     * @param type
     *            the aggregate type
     * @param on
     *            the aggregated expression
     * @param select
     *            the select statement
     * @param distinct
     *            if distinct is used
     */
    public Aggregate(AggregateType type, Expression on, Select select, boolean distinct) {
        super(select, distinct);
        if (distinct && type == AggregateType.COUNT_ALL) {
            throw DbException.throwInternalError();
        }
        this.type = type;
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
        return type;
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
        if (type == AggregateType.GROUP_CONCAT) {
            if (v != ValueNull.INSTANCE) {
                v = updateCollecting(session, v.convertTo(Value.STRING), remembered);
            }
        } else if (type == AggregateType.ARRAY_AGG) {
            if (v != ValueNull.INSTANCE) {
                v = updateCollecting(session, v, remembered);
            }
        }
        data.add(session.getDatabase(), dataType, v);
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
    }

    @Override
    protected void updateFromExpressions(Session session, Object aggregateData, Value[] array) {
        AggregateData data = (AggregateData) aggregateData;
        Value v = on == null ? null : array[0];
        updateData(session, data, v, array);
    }

    @Override
    protected Object createAggregateData() {
        return AggregateData.create(type, distinct);
    }

    @Override
    public Value getValue(Session session) {
        return select.isQuickAggregateQuery() ? getValueQuick(session) : super.getValue(session);
    }

    private Value getValueQuick(Session session) {
        switch (type) {
        case COUNT:
        case COUNT_ALL:
            Table table = select.getTopTableFilter().getTable();
            return ValueLong.get(table.getRowCount(session));
        case MIN:
        case MAX: {
            boolean first = type == AggregateType.MIN;
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
        case MEDIAN:
            return AggregateMedian.medianFromIndex(session, on, dataType);
        case ENVELOPE:
            return ((MVSpatialIndex) AggregateDataEnvelope.getGeometryColumnIndex(on)).getBounds(session);
        default:
            throw DbException.throwInternalError("type=" + type);
        }
    }

    @Override
    public Value getAggregatedValue(Session session, Object aggregateData) {
        AggregateData data = (AggregateData) aggregateData;
        if (data == null) {
            data = (AggregateData) createAggregateData();
        }
        switch (type) {
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
                AggregateDataDefault d = new AggregateDataDefault(type);
                Database db = session.getDatabase();
                for (Value v : c) {
                    d.add(db, dataType, v);
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
        case MEDIAN: {
            Value[] array = ((AggregateDataCollecting) data).getArray();
            if (array == null) {
                return ValueNull.INSTANCE;
            }
            return AggregateMedian.median(session.getDatabase(), array, dataType);
        }
        case MODE:
            return getMode(session, data);
        default:
            // Avoid compiler warning
        }
        return data.getValue(session.getDatabase(), dataType);
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
            if (val.getType() == Value.ARRAY) {
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
        ValueHashMap<LongDataCounter> distinctValues = ((AggregateDataDistinctWithCounts) data).getValues();
        if (distinctValues == null) {
            return ValueArray.getEmpty();
        }
        ValueArray[] values = new ValueArray[distinctValues.size()];
        int i = 0;
        for (Entry<Value, LongDataCounter> entry : distinctValues.entries()) {
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
        ValueHashMap<LongDataCounter> distinctValues = ((AggregateDataDistinctWithCounts) data).getValues();
        if (distinctValues == null) {
            return v;
        }
        long count = 0L;
        if (orderByList != null) {
            boolean desc = (orderByList.get(0).sortType & SortOrder.DESCENDING) != 0;
            for (Entry<Value, LongDataCounter> entry : distinctValues.entries()) {
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
            for (Entry<Value, LongDataCounter> entry : distinctValues.entries()) {
                long c = entry.getValue().count;
                if (c > count) {
                    v = entry.getKey();
                    count = c;
                }
            }
        }
        return v.convertTo(dataType);
    }

    @Override
    public int getType() {
        return dataType;
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
            dataType = on.getType();
            scale = on.getScale();
            precision = on.getPrecision();
            displaySize = on.getDisplaySize();
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
        switch (type) {
        case GROUP_CONCAT:
            dataType = Value.STRING;
            scale = 0;
            precision = displaySize = Integer.MAX_VALUE;
            break;
        case COUNT_ALL:
        case COUNT:
            dataType = Value.LONG;
            scale = 0;
            precision = ValueLong.PRECISION;
            displaySize = ValueLong.DISPLAY_SIZE;
            break;
        case SELECTIVITY:
            dataType = Value.INT;
            scale = 0;
            precision = ValueInt.PRECISION;
            displaySize = ValueInt.DISPLAY_SIZE;
            break;
        case HISTOGRAM:
            dataType = Value.ARRAY;
            scale = 0;
            precision = displaySize = Integer.MAX_VALUE;
            break;
        case SUM:
            if (dataType == Value.BOOLEAN) {
                // example: sum(id > 3) (count the rows)
                dataType = Value.LONG;
            } else if (!DataType.supportsAdd(dataType)) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            } else {
                dataType = DataType.getAddProofType(dataType);
            }
            break;
        case AVG:
            if (!DataType.supportsAdd(dataType)) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            }
            break;
        case MIN:
        case MAX:
        case MEDIAN:
        case MODE:
            break;
        case STDDEV_POP:
        case STDDEV_SAMP:
        case VAR_POP:
        case VAR_SAMP:
            dataType = Value.DOUBLE;
            precision = ValueDouble.PRECISION;
            displaySize = ValueDouble.DISPLAY_SIZE;
            scale = 0;
            break;
        case EVERY:
        case ANY:
            dataType = Value.BOOLEAN;
            precision = ValueBoolean.PRECISION;
            displaySize = ValueBoolean.DISPLAY_SIZE;
            scale = 0;
            break;
        case BIT_AND:
        case BIT_OR:
            if (!DataType.supportsAdd(dataType)) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            }
            break;
        case ARRAY_AGG:
            dataType = Value.ARRAY;
            scale = 0;
            precision = displaySize = Integer.MAX_VALUE;
            break;
        case ENVELOPE:
            dataType = Value.GEOMETRY;
            scale = 0;
            precision = displaySize = Integer.MAX_VALUE;
            break;
        default:
            DbException.throwInternalError("type=" + type);
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

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public long getPrecision() {
        return precision;
    }

    @Override
    public int getDisplaySize() {
        return displaySize;
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
        switch (type) {
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
            throw DbException.throwInternalError("type=" + type);
        }
        builder.append(text);
        if (distinct) {
            builder.append("(DISTINCT ");
            on.getSQL(builder).append(')');
        } else {
            builder.append('(');
            if (on instanceof Subquery) {
                on.getSQL(builder);
            } else {
                on.getUnenclosedSQL(builder);
            }
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
            switch (type) {
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
            case MEDIAN:
                if (distinct) {
                    return false;
                }
                return AggregateMedian.getMedianColumnIndex(on) != null;
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
