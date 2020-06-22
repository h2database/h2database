/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.h2.api.ErrorCode;
import org.h2.command.query.QueryOrderBy;
import org.h2.command.query.Select;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.ExpressionWithFlags;
import org.h2.expression.ValueExpression;
import org.h2.expression.analysis.Window;
import org.h2.expression.function.JsonConstructorFunction;
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
import org.h2.value.CompareMode;
import org.h2.value.DataType;
import org.h2.value.ExtTypeInfoRow;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBigint;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInteger;
import org.h2.value.ValueJson;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;
import org.h2.value.ValueVarchar;

/**
 * Implements the integrated aggregate functions, such as COUNT, MAX, SUM.
 */
public class Aggregate extends AbstractAggregate implements ExpressionWithFlags {

    private static final HashMap<String, AggregateType> AGGREGATES = new HashMap<>(64);

    private final AggregateType aggregateType;

    private ArrayList<QueryOrderBy> orderByList;
    private SortOrder orderBySort;

    private int flags;

    /**
     * Create a new aggregate object.
     *
     * @param aggregateType
     *            the aggregate type
     * @param args
     *            the aggregated expressions
     * @param select
     *            the select statement
     * @param distinct
     *            if distinct is used
     */
    public Aggregate(AggregateType aggregateType, Expression[] args, Select select, boolean distinct) {
        super(select, args, distinct);
        if (distinct && aggregateType == AggregateType.COUNT_ALL) {
            throw DbException.throwInternalError();
        }
        this.aggregateType = aggregateType;
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
        addAggregate("LISTAGG", AggregateType.LISTAGG);
        // MySQL compatibility: group_concat(expression, delimiter)
        addAggregate("GROUP_CONCAT", AggregateType.LISTAGG);
        // PostgreSQL compatibility: string_agg(expression, delimiter)
        addAggregate("STRING_AGG", AggregateType.LISTAGG);
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
        addAggregate("HISTOGRAM", AggregateType.HISTOGRAM);
        addAggregate("BIT_OR", AggregateType.BIT_OR);
        addAggregate("BIT_AND", AggregateType.BIT_AND);
        addAggregate("BIT_XOR", AggregateType.BIT_XOR);

        addAggregate("RANK", AggregateType.RANK);
        addAggregate("DENSE_RANK", AggregateType.DENSE_RANK);
        addAggregate("PERCENT_RANK", AggregateType.PERCENT_RANK);
        addAggregate("CUME_DIST", AggregateType.CUME_DIST);

        addAggregate("PERCENTILE_CONT", AggregateType.PERCENTILE_CONT);
        addAggregate("PERCENTILE_DISC", AggregateType.PERCENTILE_DISC);
        addAggregate("MEDIAN", AggregateType.MEDIAN);

        addAggregate("ARRAY_AGG", AggregateType.ARRAY_AGG);
        addAggregate("MODE", AggregateType.MODE);
        // Oracle compatibility
        addAggregate("STATS_MODE", AggregateType.MODE);
        addAggregate("ENVELOPE", AggregateType.ENVELOPE);

        addAggregate("JSON_OBJECTAGG", AggregateType.JSON_OBJECTAGG);
        addAggregate("JSON_ARRAYAGG", AggregateType.JSON_ARRAYAGG);
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
    public void setOrderByList(ArrayList<QueryOrderBy> orderByList) {
        this.orderByList = orderByList;
    }

    /**
     * Returns the type of this aggregate.
     *
     * @return the type of this aggregate
     */
    public AggregateType getAggregateType() {
        return aggregateType;
    }

    @Override
    public void setFlags(int flags) {
        this.flags = flags;
    }

    @Override
    public int getFlags() {
        return flags;
    }

    private void sortWithOrderBy(Value[] array) {
        final SortOrder sortOrder = orderBySort;
        Arrays.sort(array,
                sortOrder != null
                        ? (v1, v2) -> sortOrder.compare(((ValueRow) v1).getList(), ((ValueRow) v2).getList())
                        : select.getSession().getDatabase().getCompareMode());
    }

    @Override
    protected void updateAggregate(Session session, Object aggregateData) {
        AggregateData data = (AggregateData) aggregateData;
        Value v = args.length == 0 ? null : args[0].getValue(session);
        updateData(session, data, v, null);
    }

    private void updateData(Session session, AggregateData data, Value v, Value[] remembered) {
        switch (aggregateType) {
        case LISTAGG:
            if (v != ValueNull.INSTANCE) {
                v = updateCollecting(session, v.convertTo(TypeInfo.TYPE_VARCHAR), remembered);
            }
            if (args.length >= 2) {
                ((AggregateDataCollecting) data).setSharedArgument(
                        remembered != null ? remembered[1] : args[1].getValue(session));
            }
            break;
        case ARRAY_AGG:
            v = updateCollecting(session, v, remembered);
            break;
        case RANK:
        case DENSE_RANK:
        case PERCENT_RANK:
        case CUME_DIST: {
            int count = args.length;
            Value[] a = new Value[count];
            for (int i = 0; i < count; i++) {
                a[i] = remembered != null ? remembered[i] : args[i].getValue(session);
            }
            ((AggregateDataCollecting) data).setSharedArgument(ValueRow.get(a));
            a = new Value[count];
            for (int i = 0; i < count; i++) {
                a[i] = remembered != null ? remembered[count + i] :orderByList.get(i).expression.getValue(session);
            }
            v = ValueRow.get(a);
            break;
        }
        case PERCENTILE_CONT:
        case PERCENTILE_DISC:
            ((AggregateDataCollecting) data).setSharedArgument(v);
            v = remembered != null ? remembered[1] : orderByList.get(0).expression.getValue(session);
            break;
        case MODE:
            v = remembered != null ? remembered[0] : orderByList.get(0).expression.getValue(session);
            break;
        case JSON_ARRAYAGG:
            if (v != ValueNull.INSTANCE) {
                v = updateCollecting(session, v, remembered);
            } else if ((flags & JsonConstructorFunction.JSON_ABSENT_ON_NULL) == 0) {
                v = updateCollecting(session, ValueJson.NULL, remembered);
            } else {
                return;
            }
            break;
        case JSON_OBJECTAGG: {
            Value key = v;
            Value value = remembered != null ? remembered[1] : args[1].getValue(session);
            if (key == ValueNull.INSTANCE) {
                throw DbException.getInvalidValueException("JSON_OBJECTAGG key", "NULL");
            }
            if (value != ValueNull.INSTANCE) {
                v = ValueRow.get(new Value[] { key, value });
            } else if ((flags & JsonConstructorFunction.JSON_ABSENT_ON_NULL) == 0) {
                v = ValueRow.get(new Value[] { key, ValueJson.NULL });
            } else {
                return;
            }
            break;
        }
        default:
            // Use argument as is
        }
        data.add(session, v);
    }

    @Override
    protected void updateGroupAggregates(Session session, int stage) {
        super.updateGroupAggregates(session, stage);
        for (Expression arg : args) {
            arg.updateAggregate(session, stage);
        }
        if (orderByList != null) {
            for (QueryOrderBy orderBy : orderByList) {
                orderBy.expression.updateAggregate(session, stage);
            }
        }
    }

    private Value updateCollecting(Session session, Value v, Value[] remembered) {
        if (orderByList != null) {
            int size = orderByList.size();
            Value[] row = new Value[1 + size];
            row[0] = v;
            if (remembered == null) {
                for (int i = 0; i < size; i++) {
                    QueryOrderBy o = orderByList.get(i);
                    row[i + 1] = o.expression.getValue(session);
                }
            } else {
                System.arraycopy(remembered, 1, row, 1, size);
            }
            v = ValueRow.get(row);
        }
        return v;
    }

    @Override
    protected int getNumExpressions() {
        int n = args.length;
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
        for (Expression arg : args) {
            array[offset++] = arg.getValue(session);
        }
        if (orderByList != null) {
            for (QueryOrderBy o : orderByList) {
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
            Value v = args.length == 0 ? null : array[0];
            updateData(session, data, v, array);
        }
    }

    @Override
    protected Object createAggregateData() {
        return AggregateData.create(aggregateType, distinct, type.getValueType(), orderByList != null);
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
            return ValueBigint.get(table.getRowCount(session));
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
        case PERCENTILE_CONT:
        case PERCENTILE_DISC: {
            Value v = args[0].getValue(session);
            if (v == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            BigDecimal arg = v.getBigDecimal();
            if (arg.signum() >= 0 && arg.compareTo(BigDecimal.ONE) <= 0) {
                return Percentile.getFromIndex(session, orderByList.get(0).expression, type.getValueType(),
                        orderByList, arg, aggregateType == AggregateType.PERCENTILE_CONT);
            } else {
                throw DbException.getInvalidValueException(aggregateType == AggregateType.PERCENTILE_CONT ?
                        "PERCENTILE_CONT argument" : "PERCENTILE_DISC argument", arg);
            }
        }
        case MEDIAN:
            return Percentile.getFromIndex(session, args[0], type.getValueType(), orderByList, Percentile.HALF, true);
        case ENVELOPE:
            return ((MVSpatialIndex) AggregateDataEnvelope.getGeometryColumnIndex(args[0])).getBounds(session);
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
                return ValueBigint.get(((AggregateDataCollecting) data).getCount());
            }
            break;
        case SUM:
        case AVG:
        case STDDEV_POP:
        case STDDEV_SAMP:
        case VAR_POP:
        case VAR_SAMP:
        case BIT_XOR:
            if (distinct) {
                AggregateDataCollecting c = ((AggregateDataCollecting) data);
                if (c.getCount() == 0) {
                    return ValueNull.INSTANCE;
                }
                AggregateDataDefault d = new AggregateDataDefault(aggregateType, type.getValueType());
                for (Value v : c) {
                    d.add(session, v);
                }
                return d.getValue(session);
            }
            break;
        case HISTOGRAM:
            return getHistogram(session, data);
        case LISTAGG:
            return getListagg(session, data);
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
                    array[i] = ((ValueRow) array[i]).getList()[0];
                }
            }
            return ValueArray.get(array, session);
        }
        case RANK:
        case DENSE_RANK:
        case PERCENT_RANK:
        case CUME_DIST:
            return getHypotheticalSet(session, data);
        case PERCENTILE_CONT:
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
            BigDecimal arg = v.getBigDecimal();
            if (arg.signum() >= 0 && arg.compareTo(BigDecimal.ONE) <= 0) {
                return Percentile.getValue(session, array, type.getValueType(), orderByList, arg,
                        aggregateType == AggregateType.PERCENTILE_CONT);
            } else {
                throw DbException.getInvalidValueException(aggregateType == AggregateType.PERCENTILE_CONT ?
                        "PERCENTILE_CONT argument" : "PERCENTILE_DISC argument", arg);
            }
        }
        case MEDIAN: {
            Value[] array = ((AggregateDataCollecting) data).getArray();
            if (array == null) {
                return ValueNull.INSTANCE;
            }
            return Percentile.getValue(session, array, type.getValueType(), orderByList, Percentile.HALF, true);
        }
        case MODE:
            return getMode(session, data);
        case JSON_ARRAYAGG: {
            Value[] array = ((AggregateDataCollecting) data).getArray();
            if (array == null) {
                return ValueNull.INSTANCE;
            }
            if (orderByList != null) {
                sortWithOrderBy(array);
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            baos.write('[');
            for (Value v : array) {
                if (orderByList != null) {
                    v = ((ValueRow) v).getList()[0];
                }
                JsonConstructorFunction.jsonArrayAppend(baos, v, flags);
            }
            baos.write(']');
            return ValueJson.getInternal(baos.toByteArray());
        }
        case JSON_OBJECTAGG: {
            Value[] array = ((AggregateDataCollecting) data).getArray();
            if (array == null) {
                return ValueNull.INSTANCE;
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            baos.write('{');
            for (Value v : array) {
                Value[] row = ((ValueRow) v).getList();
                String key = row[0].getString();
                if (key == null) {
                    throw DbException.getInvalidValueException("JSON_OBJECTAGG key", "NULL");
                }
                JsonConstructorFunction.jsonObjectAppend(baos, key, row[1]);
            }
            return JsonConstructorFunction.jsonObjectFinish(baos, flags);
        }
        default:
            // Avoid compiler warning
        }
        return data.getValue(session);
    }

    private Value getHypotheticalSet(Session session, AggregateData data) {
        AggregateDataCollecting collectingData = (AggregateDataCollecting) data;
        Value arg = collectingData.getSharedArgument();
        if (arg == null) {
            switch (aggregateType) {
            case RANK:
            case DENSE_RANK:
                return ValueInteger.get(1);
            case PERCENT_RANK:
                return ValueDouble.ZERO;
            case CUME_DIST:
                return ValueDouble.ONE;
            default:
                throw DbException.getUnsupportedException("aggregateType=" + aggregateType);
            }
        }
        collectingData.add(session, arg);
        Value[] array = collectingData.getArray();
        Comparator<Value> sort = orderBySort.getRowValueComparator();
        Arrays.sort(array, sort);
        return aggregateType == AggregateType.CUME_DIST ? getCumeDist(array, arg, sort) : getRank(array, arg, sort);
    }

    private Value getRank(Value[] ordered, Value arg, Comparator<Value> sort) {
        int size = ordered.length;
        int number = 0;
        for (int i = 0; i < size; i++) {
            Value row = ordered[i];
            if (i == 0) {
                number = 1;
            } else if (sort.compare(ordered[i - 1], row) != 0) {
                if (aggregateType == AggregateType.DENSE_RANK) {
                    number++;
                } else {
                    number = i + 1;
                }
            }
            Value v;
            if (aggregateType == AggregateType.PERCENT_RANK) {
                int nm = number - 1;
                v = nm == 0 ? ValueDouble.ZERO : ValueDouble.get((double) nm / (size - 1));
            } else {
                v = ValueBigint.get(number);
            }
            if (sort.compare(row, arg) == 0) {
                return v;
            }
        }
        throw DbException.throwInternalError();
    }

    private static Value getCumeDist(Value[] ordered, Value arg, Comparator<Value> sort) {
        int size = ordered.length;
        for (int start = 0; start < size;) {
            Value array = ordered[start];
            int end = start + 1;
            while (end < size && sort.compare(array, ordered[end]) == 0) {
                end++;
            }
            ValueDouble v = ValueDouble.get((double) end / size);
            for (int i = start; i < end; i++) {
                if (sort.compare(ordered[i], arg) == 0) {
                    return v;
                }
            }
            start = end;
        }
        throw DbException.throwInternalError();
    }

    private Value getListagg(Session session, AggregateData data) {
        AggregateDataCollecting collectingData = (AggregateDataCollecting) data;
        Value[] array = collectingData.getArray();
        if (array == null) {
            return ValueNull.INSTANCE;
        }
        if (orderByList != null || distinct) {
            sortWithOrderBy(array);
        }
        StringBuilder builder = new StringBuilder();
        String sep = args.length < 2 ? "," : collectingData.getSharedArgument().getString();
        for (int i = 0, length = array.length; i < length; i++) {
            Value val = array[i];
            String s;
            if (orderByList != null) {
                s = ((ValueRow) val).getList()[0].getString();
            } else {
                s = val.getString();
            }
            if (sep != null && i > 0) {
                builder.append(sep);
            }
            builder.append(s);
        }
        return ValueVarchar.get(builder.toString());
    }

    private static Value getHistogram(Session session, AggregateData data) {
        TreeMap<Value, LongDataCounter> distinctValues = ((AggregateDataDistinctWithCounts) data).getValues();
        if (distinctValues == null) {
            return ValueArray.EMPTY;
        }
        ValueRow[] values = new ValueRow[distinctValues.size()];
        int i = 0;
        for (Entry<Value, LongDataCounter> entry : distinctValues.entrySet()) {
            LongDataCounter d = entry.getValue();
            values[i] = ValueRow.get(new Value[] { entry.getKey(), ValueBigint.get(d.count) });
            i++;
        }
        Database db = session.getDatabase();
        CompareMode compareMode = db.getCompareMode();
        Arrays.sort(values, (v1, v2) -> v1.getList()[0].compareTo(v2.getList()[0], session, compareMode));
        return ValueArray.get(values, session);
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
                    int cmp = session.compareTypeSafe(v, v2);
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
        return v;
    }

    @Override
    public void mapColumnsAnalysis(ColumnResolver resolver, int level, int innerState) {
        if (orderByList != null) {
            for (QueryOrderBy o : orderByList) {
                o.expression.mapColumns(resolver, level, innerState);
            }
        }
        super.mapColumnsAnalysis(resolver, level, innerState);
    }

    @Override
    public Expression optimize(Session session) {
        super.optimize(session);
        if (args.length == 1) {
            type = args[0].getType();
        }
        if (orderByList != null) {
            int offset;
            switch (aggregateType) {
            case ARRAY_AGG:
            case LISTAGG:
            case JSON_ARRAYAGG:
                offset = 1;
                break;
            default:
                offset = 0;
            }
            for (Iterator<QueryOrderBy> i = orderByList.iterator(); i.hasNext();) {
                QueryOrderBy o = i.next();
                Expression e = o.expression.optimize(session);
                if (offset != 0 && e.isConstant()) {
                    i.remove();
                } else {
                    o.expression = e;
                }
            }
            if (orderByList.isEmpty()) {
                orderByList = null;
            } else {
                orderBySort = createOrder(session, orderByList, offset);
            }
        }
        switch (aggregateType) {
        case LISTAGG:
            type = TypeInfo.TYPE_VARCHAR;
            break;
        case COUNT_ALL:
            type = TypeInfo.TYPE_BIGINT;
            break;
        case COUNT:
            if (args[0].isConstant()) {
                if (args[0].getValue(session) == ValueNull.INSTANCE) {
                    return ValueExpression.get(ValueBigint.get(0L));
                }
                if (!distinct) {
                    Aggregate aggregate = new Aggregate(AggregateType.COUNT_ALL, new Expression[0], select, false);
                    aggregate.setFilterCondition(filterCondition);
                    aggregate.setOverCondition(over);
                    return aggregate.optimize(session);
                }
            }
            type = TypeInfo.TYPE_BIGINT;
            break;
        case HISTOGRAM: {
            LinkedHashMap<String, TypeInfo> fields = new LinkedHashMap<>(4);
            fields.put("VALUE", type);
            fields.put("COUNT", TypeInfo.TYPE_BIGINT);
            type = TypeInfo.getTypeInfo(Value.ARRAY, -1, 0,
                    TypeInfo.getTypeInfo(Value.ROW, -1, -1, new ExtTypeInfoRow(fields)));
            break;
        }
        case SUM: {
            int dataType = type.getValueType();
            if (dataType == Value.BOOLEAN) {
                // example: sum(id > 3) (count the rows)
                type = TypeInfo.TYPE_BIGINT;
            } else if (!DataType.supportsAdd(dataType)) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getTraceSQL());
            } else {
                type = TypeInfo.getTypeInfo(DataType.getAddProofType(dataType));
            }
            break;
        }
        case AVG:
            if (!DataType.supportsAdd(type.getValueType())) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getTraceSQL());
            }
            break;
        case MIN:
        case MAX:
            break;
        case RANK:
        case DENSE_RANK:
            type = TypeInfo.TYPE_BIGINT;
            break;
        case PERCENT_RANK:
        case CUME_DIST:
            type = TypeInfo.TYPE_DOUBLE;
            break;
        case PERCENTILE_CONT:
            type = orderByList.get(0).expression.getType();
            //$FALL-THROUGH$
        case MEDIAN:
            switch (type.getValueType()) {
            case Value.TINYINT:
            case Value.SMALLINT:
            case Value.INTEGER:
            case Value.BIGINT:
            case Value.NUMERIC:
            case Value.REAL:
            case Value.DOUBLE:
            case Value.DECFLOAT:
                type = TypeInfo.TYPE_NUMERIC_FLOATING_POINT;
                break;
            }
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
        case BIT_XOR:
            if (!DataType.supportsAdd(type.getValueType())) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getTraceSQL());
            }
            break;
        case ARRAY_AGG:
            type = TypeInfo.getTypeInfo(Value.ARRAY, -1, 0, args[0].getType());
            break;
        case ENVELOPE:
            type = TypeInfo.TYPE_GEOMETRY;
            break;
        case JSON_OBJECTAGG:
        case JSON_ARRAYAGG:
            type = TypeInfo.TYPE_JSON;
            break;
        default:
            DbException.throwInternalError("type=" + aggregateType);
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if (orderByList != null) {
            for (QueryOrderBy o : orderByList) {
                o.expression.setEvaluatable(tableFilter, b);
            }
        }
        super.setEvaluatable(tableFilter, b);
    }

    @Override
    public StringBuilder getUnenclosedSQL(StringBuilder builder, int sqlFlags) {
        String text;
        switch (aggregateType) {
        case COUNT_ALL:
            return appendTailConditions(builder.append("COUNT(*)"), sqlFlags, false);
        case COUNT:
            text = "COUNT";
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
        case BIT_XOR:
            text = "BIT_XOR";
            break;
        case RANK:
            text = "RANK";
            break;
        case DENSE_RANK:
            text = "DENSE_RANK";
            break;
        case PERCENT_RANK:
            text = "PERCENT_RANK";
            break;
        case CUME_DIST:
            text = "CUME_DIST";
            break;
        case PERCENTILE_CONT:
            text = "PERCENTILE_CONT";
            break;
        case PERCENTILE_DISC:
            text = "PERCENTILE_DISC";
            break;
        case MEDIAN:
            text = "MEDIAN";
            break;
        case LISTAGG:
            text = "LISTAGG";
            break;
        case ARRAY_AGG:
            return getSQLArrayAggregate(builder, sqlFlags);
        case MODE:
            text = "MODE";
            break;
        case ENVELOPE:
            text = "ENVELOPE";
            break;
        case JSON_OBJECTAGG:
            return getSQLJsonObjectAggregate(builder, sqlFlags);
        case JSON_ARRAYAGG:
            return getSQLJsonArrayAggregate(builder, sqlFlags);
        default:
            throw DbException.throwInternalError("type=" + aggregateType);
        }
        builder.append(text);
        if (distinct) {
            builder.append("(DISTINCT ");
        } else {
            builder.append('(');
        }
        writeExpressions(builder, args, sqlFlags).append(')');
        boolean forceOrderBy = aggregateType == AggregateType.LISTAGG;
        if (forceOrderBy || orderByList != null) {
            builder.append(" WITHIN GROUP (");
            Window.appendOrderBy(builder, orderByList, sqlFlags, forceOrderBy);
            builder.append(')');
        }
        return appendTailConditions(builder, sqlFlags, false);
    }

    private StringBuilder getSQLArrayAggregate(StringBuilder builder, int sqlFlags) {
        builder.append("ARRAY_AGG(");
        if (distinct) {
            builder.append("DISTINCT ");
        }
        args[0].getUnenclosedSQL(builder, sqlFlags);
        Window.appendOrderBy(builder, orderByList, sqlFlags, false);
        builder.append(')');
        return appendTailConditions(builder, sqlFlags, false);
    }

    private StringBuilder getSQLJsonObjectAggregate(StringBuilder builder, int sqlFlags) {
        builder.append("JSON_OBJECTAGG(");
        args[0].getUnenclosedSQL(builder, sqlFlags).append(": ");
        args[1].getUnenclosedSQL(builder, sqlFlags);
        JsonConstructorFunction.getJsonFunctionFlagsSQL(builder, flags, false).append(')');
        return appendTailConditions(builder, sqlFlags, false);
    }

    private StringBuilder getSQLJsonArrayAggregate(StringBuilder builder, int sqlFlags) {
        builder.append("JSON_ARRAYAGG(");
        args[0].getUnenclosedSQL(builder, sqlFlags);
        JsonConstructorFunction.getJsonFunctionFlagsSQL(builder, flags, true);
        Window.appendOrderBy(builder, orderByList, sqlFlags, false);
        builder.append(')');
        return appendTailConditions(builder, sqlFlags, false);
    }

    private Index getMinMaxColumnIndex() {
        Expression arg = args[0];
        if (arg instanceof ExpressionColumn) {
            ExpressionColumn col = (ExpressionColumn) arg;
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
                if (!distinct && args[0].getNullable() == Column.NOT_NULLABLE) {
                    return visitor.getTable().canGetRowCount();
                }
                return false;
            case COUNT_ALL:
                return visitor.getTable().canGetRowCount();
            case MIN:
            case MAX:
                Index index = getMinMaxColumnIndex();
                return index != null;
            case PERCENTILE_CONT:
            case PERCENTILE_DISC:
                return args[0].isConstant() && Percentile.getColumnIndex(orderByList.get(0).expression) != null;
            case MEDIAN:
                if (distinct) {
                    return false;
                }
                return Percentile.getColumnIndex(args[0]) != null;
            case ENVELOPE:
                return AggregateDataEnvelope.getGeometryColumnIndex(args[0]) != null;
            default:
                return false;
            }
        }
        for (Expression arg : args) {
            if (!arg.isEverything(visitor)) {
                return false;
            }
        }
        if (orderByList != null) {
            for (QueryOrderBy o : orderByList) {
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
        for (Expression arg : args) {
            cost += arg.getCost();
        }
        if (orderByList != null) {
            for (QueryOrderBy o : orderByList) {
                cost += o.expression.getCost();
            }
        }
        if (filterCondition != null) {
            cost += filterCondition.getCost();
        }
        return cost;
    }

}
