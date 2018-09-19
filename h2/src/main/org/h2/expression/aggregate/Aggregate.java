/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.aggregate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import org.h2.api.ErrorCode;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectOrderBy;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
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
import org.h2.util.StringUtils;
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
         * The aggregate type for BOOL_OR(expression).
         */
        BOOL_OR,

        /**
         * The aggregate type for BOOL_AND(expression).
         */
        BOOL_AND,

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
        addAggregate("BOOL_OR", AggregateType.BOOL_OR);
        // HSQLDB compatibility, but conflicts with x > EVERY(...)
        addAggregate("SOME", AggregateType.BOOL_OR);
        addAggregate("BOOL_AND", AggregateType.BOOL_AND);
        // HSQLDB compatibility, but conflicts with x > SOME(...)
        addAggregate("EVERY", AggregateType.BOOL_AND);
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

    @Override
    public boolean isAggregate() {
        return true;
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
        data.add(session.getDatabase(), dataType, distinct, v);
    }

    @Override
    protected void updateGroupAggregates(Session session, int stage) {
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
                for (int i = 1; i <= size; i++) {
                    array[i] = remembered[i];
                }
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
        return select.getSession().getDatabase().getAggregateDataFactory().create(select,this);
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
            return AggregateDataMedian.getResultFromIndex(session, on, dataType);
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

        return data.getValue(session.getDatabase(), dataType, distinct);
    }

    @Override
    public int getType() {
        return dataType;
    }

    public AggregateType getAggregateType() {
        return type;
    }

    public SortOrder getOrderBySort() {
        return orderBySort;
    }

    public ArrayList<SelectOrderBy> getOrderByList() {
        return orderByList;
    }

    public String getGroupConcatSeparator() {
        return groupConcatSeparator == null ? "," : groupConcatSeparator.getValue(select.getSession()).getString();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        if (on != null) {
            on.mapColumns(resolver, level);
        }
        if (orderByList != null) {
            for (SelectOrderBy o : orderByList) {
                o.expression.mapColumns(resolver, level);
            }
        }
        if (groupConcatSeparator != null) {
            groupConcatSeparator.mapColumns(resolver, level);
        }
        super.mapColumns(resolver, level);
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
        if (filterCondition != null) {
            filterCondition = filterCondition.optimize(session);
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
        case BOOL_AND:
        case BOOL_OR:
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

    private String getSQLGroupConcat() {
        StringBuilder buff = new StringBuilder("GROUP_CONCAT(");
        if (distinct) {
            buff.append("DISTINCT ");
        }
        buff.append(on.getSQL());
        Window.appendOrderBy(buff, orderByList);
        if (groupConcatSeparator != null) {
            buff.append(" SEPARATOR ").append(groupConcatSeparator.getSQL());
        }
        buff.append(')');
        return appendTailConditions(buff).toString();
    }

    private String getSQLArrayAggregate() {
        StringBuilder buff = new StringBuilder("ARRAY_AGG(");
        if (distinct) {
            buff.append("DISTINCT ");
        }
        buff.append(on.getSQL());
        Window.appendOrderBy(buff, orderByList);
        buff.append(')');
        return appendTailConditions(buff).toString();
    }

    @Override
    public String getSQL() {
        String text;
        switch (type) {
        case GROUP_CONCAT:
            return getSQLGroupConcat();
        case COUNT_ALL:
            return appendTailConditions(new StringBuilder().append("COUNT(*)")).toString();
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
        case BOOL_AND:
            text = "BOOL_AND";
            break;
        case BOOL_OR:
            text = "BOOL_OR";
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
            return getSQLArrayAggregate();
        case MODE:
            text = "MODE";
            break;
        case ENVELOPE:
            text = "ENVELOPE";
            break;
        default:
            throw DbException.throwInternalError("type=" + type);
        }
        StringBuilder builder = new StringBuilder().append(text);
        if (distinct) {
            builder.append("(DISTINCT ").append(on.getSQL()).append(')');
        } else {
            builder.append(StringUtils.enclose(on.getSQL()));
        }
        return appendTailConditions(builder).toString();
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
        if (visitor.getType() == ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL) {
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
                return AggregateDataMedian.getMedianColumnIndex(on) != null;
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
