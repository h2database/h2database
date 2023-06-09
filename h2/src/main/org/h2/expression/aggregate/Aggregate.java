/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
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
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.ExpressionWithFlags;
import org.h2.expression.ValueExpression;
import org.h2.expression.aggregate.AggregateDataCollecting.NullCollectionMode;
import org.h2.expression.analysis.Window;
import org.h2.expression.function.BitFunction;
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
import org.h2.util.StringUtils;
import org.h2.util.json.JsonConstructorUtils;
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
import org.h2.value.ValueInterval;
import org.h2.value.ValueJson;
import org.h2.value.ValueNull;
import org.h2.value.ValueNumeric;
import org.h2.value.ValueRow;
import org.h2.value.ValueVarchar;

/**
 * Implements the integrated aggregate functions, such as COUNT, MAX, SUM.
 */
public class Aggregate extends AbstractAggregate implements ExpressionWithFlags {

    /**
     * The additional result precision in decimal digits for a SUM aggregate function.
     */
    private static final int ADDITIONAL_SUM_PRECISION = 10;

    /**
     * The additional precision and scale in decimal digits for an AVG aggregate function.
     */
    private static final int ADDITIONAL_AVG_SCALE = 10;

    private static final HashMap<String, AggregateType> AGGREGATES = new HashMap<>(128);

    private final AggregateType aggregateType;

    private ArrayList<QueryOrderBy> orderByList;
    private SortOrder orderBySort;

    private Object extraArguments;

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
            throw DbException.getInternalError();
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
        addAggregate("ANY_VALUE", AggregateType.ANY_VALUE);
        addAggregate("ANY", AggregateType.ANY);
        addAggregate("SOME", AggregateType.ANY);
        // PostgreSQL compatibility
        addAggregate("BOOL_OR", AggregateType.ANY);
        addAggregate("EVERY", AggregateType.EVERY);
        // PostgreSQL compatibility
        addAggregate("BOOL_AND", AggregateType.EVERY);
        addAggregate("HISTOGRAM", AggregateType.HISTOGRAM);
        addAggregate("BIT_AND_AGG", AggregateType.BIT_AND_AGG);
        addAggregate("BIT_AND", AggregateType.BIT_AND_AGG);
        addAggregate("BIT_OR_AGG", AggregateType.BIT_OR_AGG);
        addAggregate("BIT_OR", AggregateType.BIT_OR_AGG);
        addAggregate("BIT_XOR_AGG", AggregateType.BIT_XOR_AGG);
        addAggregate("BIT_NAND_AGG", AggregateType.BIT_NAND_AGG);
        addAggregate("BIT_NOR_AGG", AggregateType.BIT_NOR_AGG);
        addAggregate("BIT_XNOR_AGG", AggregateType.BIT_XNOR_AGG);

        addAggregate("COVAR_POP", AggregateType.COVAR_POP);
        addAggregate("COVAR_SAMP", AggregateType.COVAR_SAMP);
        addAggregate("CORR", AggregateType.CORR);
        addAggregate("REGR_SLOPE", AggregateType.REGR_SLOPE);
        addAggregate("REGR_INTERCEPT", AggregateType.REGR_INTERCEPT);
        addAggregate("REGR_COUNT", AggregateType.REGR_COUNT);
        addAggregate("REGR_R2", AggregateType.REGR_R2);
        addAggregate("REGR_AVGX", AggregateType.REGR_AVGX);
        addAggregate("REGR_AVGY", AggregateType.REGR_AVGY);
        addAggregate("REGR_SXX", AggregateType.REGR_SXX);
        addAggregate("REGR_SYY", AggregateType.REGR_SYY);
        addAggregate("REGR_SXY", AggregateType.REGR_SXY);

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

    /**
     * Sets the additional arguments.
     *
     * @param extraArguments the additional arguments
     */
    public void setExtraArguments(Object extraArguments) {
        this.extraArguments = extraArguments;
    }

    /**
     * Returns the additional arguments.
     *
     * @return the additional arguments
     */
    public Object getExtraArguments() {
        return extraArguments;
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
    protected void updateAggregate(SessionLocal session, Object aggregateData) {
        AggregateData data = (AggregateData) aggregateData;
        Value v = args.length == 0 ? null : args[0].getValue(session);
        updateData(session, data, v, null);
    }

    private void updateData(SessionLocal session, AggregateData data, Value v, Value[] remembered) {
        switch (aggregateType) {
        case COVAR_POP:
        case COVAR_SAMP:
        case CORR:
        case REGR_SLOPE:
        case REGR_INTERCEPT:
        case REGR_R2:
        case REGR_SXY: {
            Value x;
            if (v == ValueNull.INSTANCE || (x = getSecondValue(session, remembered)) == ValueNull.INSTANCE) {
                return;
            }
            ((AggregateDataBinarySet) data).add(session, v, x);
            return;
        }
        case REGR_COUNT:
        case REGR_AVGY:
        case REGR_SYY:
            if (v == ValueNull.INSTANCE || getSecondValue(session, remembered) == ValueNull.INSTANCE) {
                return;
            }
            break;
        case REGR_AVGX:
        case REGR_SXX:
            if (v == ValueNull.INSTANCE || (v = getSecondValue(session, remembered)) == ValueNull.INSTANCE) {
                return;
            }
            break;
        case LISTAGG:
            if (v == ValueNull.INSTANCE) {
                return;
            }
            v = updateCollecting(session, v.convertTo(TypeInfo.TYPE_VARCHAR), remembered);
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
                a[i] = remembered != null ? remembered[count + i] : orderByList.get(i).expression.getValue(session);
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
            v = updateCollecting(session, v, remembered);
            break;
        case JSON_OBJECTAGG: {
            Value key = v;
            Value value = getSecondValue(session, remembered);
            if (key == ValueNull.INSTANCE) {
                throw DbException.getInvalidValueException("JSON_OBJECTAGG key", "NULL");
            }
            v = ValueRow.get(new Value[] { key, value });
            break;
        }
        default:
            // Use argument as is
        }
        data.add(session, v);
    }

    private Value getSecondValue(SessionLocal session, Value[] remembered) {
        return remembered != null ? remembered[1] : args[1].getValue(session);
    }

    @Override
    protected void updateGroupAggregates(SessionLocal session, int stage) {
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

    private Value updateCollecting(SessionLocal session, Value v, Value[] remembered) {
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
    protected void rememberExpressions(SessionLocal session, Value[] array) {
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
    protected void updateFromExpressions(SessionLocal session, Object aggregateData, Value[] array) {
        if (filterCondition == null || array[getNumExpressions() - 1].isTrue()) {
            AggregateData data = (AggregateData) aggregateData;
            Value v = args.length == 0 ? null : array[0];
            updateData(session, data, v, array);
        }
    }

    @Override
    protected Object createAggregateData() {
        switch (aggregateType) {
        case COUNT_ALL:
        case REGR_COUNT:
            return new AggregateDataCount(true);
        case COUNT:
            if (!distinct) {
                return new AggregateDataCount(false);
            }
            break;
        case RANK:
        case DENSE_RANK:
        case PERCENT_RANK:
        case CUME_DIST:
        case PERCENTILE_CONT:
        case PERCENTILE_DISC:
        case MEDIAN:
            break;
        case SUM:
        case BIT_XOR_AGG:
        case BIT_XNOR_AGG:
            if (distinct) {
                break;
            }
            //$FALL-THROUGH$
        case MIN:
        case MAX:
        case BIT_AND_AGG:
        case BIT_OR_AGG:
        case BIT_NAND_AGG:
        case BIT_NOR_AGG:
        case ANY:
        case EVERY:
            return new AggregateDataDefault(aggregateType, type);
        case AVG:
            if (distinct) {
                break;
            }
            //$FALL-THROUGH$
        case REGR_AVGX:
        case REGR_AVGY:
            return new AggregateDataAvg(type);
        case STDDEV_POP:
        case STDDEV_SAMP:
        case VAR_POP:
        case VAR_SAMP:
            if (distinct) {
                break;
            }
            //$FALL-THROUGH$
        case REGR_SXX:
        case REGR_SYY:
            return new AggregateDataStdVar(aggregateType);
        case HISTOGRAM:
            return new AggregateDataDistinctWithCounts(false, Constants.SELECTIVITY_DISTINCT_COUNT);
        case COVAR_POP:
        case COVAR_SAMP:
        case REGR_SXY:
            return new AggregateDataCovar(aggregateType);
        case CORR:
        case REGR_SLOPE:
        case REGR_INTERCEPT:
        case REGR_R2:
            return new AggregateDataCorr(aggregateType);
        case ANY_VALUE:
            if (!distinct) {
                return new AggregateDataAnyValue();
            }
            break;
        case LISTAGG: // NULL values are excluded by Aggregate
        case ARRAY_AGG:
            return new AggregateDataCollecting(distinct, orderByList != null, NullCollectionMode.USED_OR_IMPOSSIBLE);
        case MODE:
            return new AggregateDataDistinctWithCounts(true, Integer.MAX_VALUE);
        case ENVELOPE:
            return new AggregateDataEnvelope();
        case JSON_ARRAYAGG:
            return new AggregateDataCollecting(distinct, orderByList != null,
                    (flags & JsonConstructorUtils.JSON_ABSENT_ON_NULL) != 0 ? NullCollectionMode.EXCLUDED
                            : NullCollectionMode.USED_OR_IMPOSSIBLE);
        case JSON_OBJECTAGG:
            // ROW(key, value) are collected, so NULL values can't be passed
            return new AggregateDataCollecting(distinct, false, NullCollectionMode.USED_OR_IMPOSSIBLE);
        default:
            throw DbException.getInternalError("type=" + aggregateType);
        }
        return new AggregateDataCollecting(distinct, false, NullCollectionMode.IGNORED);
    }

    @Override
    public Value getValue(SessionLocal session) {
        return select.isQuickAggregateQuery() ? getValueQuick(session) : super.getValue(session);
    }

    private Value getValueQuick(SessionLocal session) {
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
            throw DbException.getInternalError("type=" + aggregateType);
        }
    }

    @Override
    public Value getAggregatedValue(SessionLocal session, Object aggregateData) {
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
        case BIT_XOR_AGG:
        case BIT_XNOR_AGG:
            if (distinct) {
                AggregateDataCollecting c = ((AggregateDataCollecting) data);
                if (c.getCount() == 0) {
                    return ValueNull.INSTANCE;
                }
                return collect(session, c, new AggregateDataDefault(aggregateType, type));
            }
            break;
        case AVG:
            if (distinct) {
                AggregateDataCollecting c = ((AggregateDataCollecting) data);
                if (c.getCount() == 0) {
                    return ValueNull.INSTANCE;
                }
                return collect(session, c, new AggregateDataAvg(type));
            }
            break;
        case STDDEV_POP:
        case STDDEV_SAMP:
        case VAR_POP:
        case VAR_SAMP:
            if (distinct) {
                AggregateDataCollecting c = ((AggregateDataCollecting) data);
                if (c.getCount() == 0) {
                    return ValueNull.INSTANCE;
                }
                return collect(session, c, new AggregateDataStdVar(aggregateType));
            }
            break;
        case ANY_VALUE:
            if (distinct) {
                Value[] values = ((AggregateDataCollecting) data).getArray();
                if (values == null) {
                    return ValueNull.INSTANCE;
                }
                return values[session.getRandom().nextInt(values.length)];
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
            return ValueArray.get((TypeInfo) type.getExtTypeInfo(), array, session);
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
                JsonConstructorUtils.jsonArrayAppend(baos, v != ValueNull.INSTANCE ? v : ValueJson.NULL, flags);
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
                Value value = row[1];
                if (value == ValueNull.INSTANCE) {
                    if ((flags & JsonConstructorUtils.JSON_ABSENT_ON_NULL) != 0) {
                        continue;
                    }
                    value = ValueJson.NULL;
                }
                JsonConstructorUtils.jsonObjectAppend(baos, key, value);
            }
            return JsonConstructorUtils.jsonObjectFinish(baos, flags);
        }
        default:
            // Avoid compiler warning
        }
        return data.getValue(session);
    }

    private static Value collect(SessionLocal session, AggregateDataCollecting c, AggregateData d) {
        for (Value v : c) {
            d.add(session, v);
        }
        return d.getValue(session);
    }

    private Value getHypotheticalSet(SessionLocal session, AggregateData data) {
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
        throw DbException.getInternalError();
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
        throw DbException.getInternalError();
    }

    private Value getListagg(SessionLocal session, AggregateData data) {
        AggregateDataCollecting collectingData = (AggregateDataCollecting) data;
        Value[] array = collectingData.getArray();
        if (array == null) {
            return ValueNull.INSTANCE;
        }
        if (array.length == 1) {
            Value v = array[0];
            if (orderByList != null) {
                v = ((ValueRow) v).getList()[0];
            }
            return v.convertTo(Value.VARCHAR, session);
        }
        if (orderByList != null || distinct) {
            sortWithOrderBy(array);
        }
        ListaggArguments arguments = (ListaggArguments) extraArguments;
        String separator = arguments.getEffectiveSeparator();
        return ValueVarchar
                .get((arguments.getOnOverflowTruncate()
                        ? getListaggTruncate(array, separator, arguments.getEffectiveFilter(),
                                arguments.isWithoutCount())
                        : getListaggError(array, separator)).toString(), session);
    }

    private StringBuilder getListaggError(Value[] array, String separator) {
        StringBuilder builder = new StringBuilder(getListaggItem(array[0]));
        for (int i = 1, count = array.length; i < count; i++) {
            String s = getListaggItem(array[i]);
            long length = (long) builder.length() + separator.length() + s.length();
            if (length > Constants.MAX_STRING_LENGTH) {
                int limit = 81;
                StringUtils.appendToLength(builder, separator, limit);
                StringUtils.appendToLength(builder, s, limit);
                throw DbException.getValueTooLongException("CHARACTER VARYING", builder.substring(0, limit), -1L);
            }
            builder.append(separator).append(s);
        }
        return builder;
    }

    private StringBuilder getListaggTruncate(Value[] array, String separator, String filter,
            boolean withoutCount) {
        int count = array.length;
        String[] strings = new String[count];
        String s = getListaggItem(array[0]);
        strings[0] = s;
        final int estimatedLength = (int) Math.min(Constants.MAX_STRING_LENGTH, s.length() * (long) count);
        final StringBuilder builder = new StringBuilder(estimatedLength);
        builder.append(s);
        loop: for (int i = 1; i < count; i++) {
            strings[i] = s = getListaggItem(array[i]);
            int length = builder.length();
            long longLength = (long) length + separator.length() + s.length();
            if (longLength > Constants.MAX_STRING_LENGTH) {
                if (longLength - s.length() >= Constants.MAX_STRING_LENGTH) {
                    i--;
                } else {
                    builder.append(separator);
                    length = (int) longLength;
                }
                for (; i > 0; i--) {
                    length -= strings[i].length();
                    builder.setLength(length);
                    StringUtils.appendToLength(builder, filter, Constants.MAX_STRING_LENGTH + 1);
                    if (!withoutCount) {
                        builder.append('(').append(count - i).append(')');
                    }
                    if (builder.length() <= Constants.MAX_STRING_LENGTH) {
                        break loop;
                    }
                    length -= separator.length();
                }
                builder.setLength(0);
                builder.append(filter).append('(').append(count).append(')');
                break;
            }
            builder.append(separator).append(s);
        }
        return builder;
    }

    private String getListaggItem(Value v) {
        if (orderByList != null) {
            v = ((ValueRow) v).getList()[0];
        }
        return v.getString();
    }

    private Value getHistogram(SessionLocal session, AggregateData data) {
        TreeMap<Value, LongDataCounter> distinctValues = ((AggregateDataDistinctWithCounts) data).getValues();
        TypeInfo rowType = (TypeInfo) type.getExtTypeInfo();
        if (distinctValues == null) {
            return ValueArray.get(rowType, Value.EMPTY_VALUES, session);
        }
        ValueRow[] values = new ValueRow[distinctValues.size()];
        int i = 0;
        for (Entry<Value, LongDataCounter> entry : distinctValues.entrySet()) {
            LongDataCounter d = entry.getValue();
            values[i] = ValueRow.get(rowType, new Value[] { entry.getKey(), ValueBigint.get(d.count) });
            i++;
        }
        Database db = session.getDatabase();
        CompareMode compareMode = db.getCompareMode();
        Arrays.sort(values, (v1, v2) -> v1.getList()[0].compareTo(v2.getList()[0], session, compareMode));
        return ValueArray.get(rowType, values, session);
    }

    private Value getMode(SessionLocal session, AggregateData data) {
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
    public Expression optimize(SessionLocal session) {
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
            //$FALL-THROUGH$
        case COUNT_ALL:
        case REGR_COUNT:
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
        case SUM:
            if ((type = getSumType(type)) == null) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getTraceSQL());
            }
            break;
        case AVG:
            if ((type = getAvgType(type)) == null) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getTraceSQL());
            }
            break;
        case MIN:
        case MAX:
        case ANY_VALUE:
            break;
        case STDDEV_POP:
        case STDDEV_SAMP:
        case VAR_POP:
        case VAR_SAMP:
        case COVAR_POP:
        case COVAR_SAMP:
        case CORR:
        case REGR_SLOPE:
        case REGR_INTERCEPT:
        case REGR_R2:
        case REGR_SXX:
        case REGR_SYY:
        case REGR_SXY:
            type = TypeInfo.TYPE_DOUBLE;
            break;
        case REGR_AVGX:
            if ((type = getAvgType(args[1].getType())) == null) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getTraceSQL());
            }
            break;
        case REGR_AVGY:
            if ((type = getAvgType(args[0].getType())) == null) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getTraceSQL());
            }
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
        case EVERY:
        case ANY:
            type = TypeInfo.TYPE_BOOLEAN;
            break;
        case BIT_AND_AGG:
        case BIT_OR_AGG:
        case BIT_XOR_AGG:
        case BIT_NAND_AGG:
        case BIT_NOR_AGG:
        case BIT_XNOR_AGG:
            BitFunction.checkArgType(args[0]);
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
            throw DbException.getInternalError("type=" + aggregateType);
        }
        return this;
    }

    private static TypeInfo getSumType(TypeInfo type) {
        int valueType = type.getValueType();
        switch (valueType) {
        case Value.BOOLEAN:
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
            return TypeInfo.TYPE_BIGINT;
        case Value.BIGINT:
            return TypeInfo.getTypeInfo(Value.NUMERIC, ValueBigint.DECIMAL_PRECISION + ADDITIONAL_SUM_PRECISION, -1,
                    null);
        case Value.NUMERIC:
            return TypeInfo.getTypeInfo(Value.NUMERIC, type.getPrecision() + ADDITIONAL_SUM_PRECISION,
                    type.getDeclaredScale(), null);
        case Value.REAL:
            return TypeInfo.TYPE_DOUBLE;
        case Value.DOUBLE:
            return TypeInfo.getTypeInfo(Value.DECFLOAT, ValueDouble.DECIMAL_PRECISION + ADDITIONAL_SUM_PRECISION, -1,
                    null);
        case Value.DECFLOAT:
            return TypeInfo.getTypeInfo(Value.DECFLOAT, type.getPrecision() + ADDITIONAL_SUM_PRECISION, -1, null);
        default:
            if (DataType.isIntervalType(valueType)) {
                return TypeInfo.getTypeInfo(valueType, ValueInterval.MAXIMUM_PRECISION, type.getDeclaredScale(), null);
            }
            return null;
        }
    }

    private static TypeInfo getAvgType(TypeInfo type) {
        switch (type.getValueType()) {
        case Value.TINYINT:
        case Value.SMALLINT:
        case Value.INTEGER:
        case Value.REAL:
            return TypeInfo.TYPE_DOUBLE;
        case Value.BIGINT:
            return TypeInfo.getTypeInfo(Value.NUMERIC, ValueBigint.DECIMAL_PRECISION + ADDITIONAL_AVG_SCALE,
                    ADDITIONAL_AVG_SCALE, null);
        case Value.NUMERIC: {
            int additionalScale = Math.min(ValueNumeric.MAXIMUM_SCALE - type.getScale(),
                    Math.min(Constants.MAX_NUMERIC_PRECISION - (int) type.getPrecision(), ADDITIONAL_AVG_SCALE));
            return TypeInfo.getTypeInfo(Value.NUMERIC, type.getPrecision() + additionalScale,
                    type.getScale() + additionalScale, null);
        }
        case Value.DOUBLE:
            return TypeInfo.getTypeInfo(Value.DECFLOAT, ValueDouble.DECIMAL_PRECISION + ADDITIONAL_AVG_SCALE, -1, //
                    null);
        case Value.DECFLOAT:
            return TypeInfo.getTypeInfo(Value.DECFLOAT, type.getPrecision() + ADDITIONAL_AVG_SCALE, -1, null);
        case Value.INTERVAL_YEAR:
        case Value.INTERVAL_YEAR_TO_MONTH:
            return TypeInfo.getTypeInfo(Value.INTERVAL_YEAR_TO_MONTH, type.getDeclaredPrecision(), 0, null);
        case Value.INTERVAL_MONTH:
            return TypeInfo.getTypeInfo(Value.INTERVAL_MONTH, type.getDeclaredPrecision(), 0, null);
        case Value.INTERVAL_DAY:
        case Value.INTERVAL_DAY_TO_HOUR:
        case Value.INTERVAL_DAY_TO_MINUTE:
        case Value.INTERVAL_DAY_TO_SECOND:
            return TypeInfo.getTypeInfo(Value.INTERVAL_DAY_TO_SECOND, type.getDeclaredPrecision(),
                    ValueInterval.MAXIMUM_SCALE, null);
        case Value.INTERVAL_HOUR:
        case Value.INTERVAL_HOUR_TO_MINUTE:
        case Value.INTERVAL_HOUR_TO_SECOND:
            return TypeInfo.getTypeInfo(Value.INTERVAL_HOUR_TO_SECOND, type.getDeclaredPrecision(),
                    ValueInterval.MAXIMUM_SCALE, null);
        case Value.INTERVAL_MINUTE:
        case Value.INTERVAL_MINUTE_TO_SECOND:
            return TypeInfo.getTypeInfo(Value.INTERVAL_MINUTE_TO_SECOND, type.getDeclaredPrecision(),
                    ValueInterval.MAXIMUM_SCALE, null);
        case Value.INTERVAL_SECOND:
            return TypeInfo.getTypeInfo(Value.INTERVAL_SECOND, type.getDeclaredPrecision(), //
                    ValueInterval.MAXIMUM_SCALE, null);
        default:
            return null;
        }
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
        switch (aggregateType) {
        case COUNT_ALL:
            return appendTailConditions(builder.append("COUNT(*)"), sqlFlags, false);
        case LISTAGG:
            return getSQLListagg(builder, sqlFlags);
        case ARRAY_AGG:
            return getSQLArrayAggregate(builder, sqlFlags);
        case JSON_OBJECTAGG:
            return getSQLJsonObjectAggregate(builder, sqlFlags);
        case JSON_ARRAYAGG:
            return getSQLJsonArrayAggregate(builder, sqlFlags);
        default:
        }
        builder.append(aggregateType.name());
        if (distinct) {
            builder.append("(DISTINCT ");
        } else {
            builder.append('(');
        }
        writeExpressions(builder, args, sqlFlags).append(')');
        if (orderByList != null) {
            builder.append(" WITHIN GROUP (");
            Window.appendOrderBy(builder, orderByList, sqlFlags, false);
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

    private StringBuilder getSQLListagg(StringBuilder builder, int sqlFlags) {
        builder.append("LISTAGG(");
        if (distinct) {
            builder.append("DISTINCT ");
        }
        args[0].getUnenclosedSQL(builder, sqlFlags);
        ListaggArguments arguments = (ListaggArguments) extraArguments;
        Expression e = arguments.getSeparator();
        if (e != null) {
            e.getUnenclosedSQL(builder.append(", "), sqlFlags);
        }
        if (arguments.getOnOverflowTruncate()) {
            builder.append(" ON OVERFLOW TRUNCATE ");
            e = arguments.getFilter();
            if (e != null) {
                e.getUnenclosedSQL(builder, sqlFlags).append(' ');
            }
            builder.append(arguments.isWithoutCount() ? "WITHOUT" : "WITH").append(" COUNT");
        }
        builder.append(')');
        builder.append(" WITHIN GROUP (");
        Window.appendOrderBy(builder, orderByList, sqlFlags, true);
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
        if (distinct) {
            builder.append("DISTINCT ");
        }
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
        switch (visitor.getType()) {
        case ExpressionVisitor.OPTIMIZABLE_AGGREGATE:
            switch (aggregateType) {
            case COUNT:
                if (distinct || args[0].getNullable() != Column.NOT_NULLABLE) {
                    return false;
                }
                //$FALL-THROUGH$
            case COUNT_ALL:
                return visitor.getTable().canGetRowCount(select.getSession());
            case MIN:
            case MAX:
                return getMinMaxColumnIndex() != null;
            case PERCENTILE_CONT:
            case PERCENTILE_DISC:
                return args[0].isConstant() && Percentile.getColumnIndex(select.getSession().getDatabase(),
                        orderByList.get(0).expression) != null;
            case MEDIAN:
                if (distinct) {
                    return false;
                }
                return Percentile.getColumnIndex(select.getSession().getDatabase(), args[0]) != null;
            case ENVELOPE:
                return AggregateDataEnvelope.getGeometryColumnIndex(args[0]) != null;
            default:
                return false;
            }
        case ExpressionVisitor.DETERMINISTIC:
            if (aggregateType == AggregateType.ANY_VALUE) {
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

    /**
     * Returns the select statement.
     * @return the select statement
     */
    public Select getSelect() {
        return select;
    }

    /**
     * Returns if distinct is used.
     *
     * @return if distinct is used
     */
    public boolean isDistinct() {
        return distinct;
    }

}
