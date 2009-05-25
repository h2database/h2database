/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectOrderBy;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.New;
import org.h2.util.ObjectArray;
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
public class Aggregate extends Expression {

    /**
     * The aggregate type for COUNT(*).
     */
    public static final int COUNT_ALL = 0;

    /**
     * The aggregate type for COUNT(expression).
     */
    public static final int COUNT = 1;

    /**
     * The aggregate type for GROUP_CONCAT(...).
     */
    public static final int GROUP_CONCAT = 2;

    /**
     * The aggregate type for SUM(expression).
     */
    static final int SUM = 3;

    /**
     * The aggregate type for MIN(expression).
     */
    static final int MIN = 4;

    /**
     * The aggregate type for MAX(expression).
     */
    static final int MAX = 5;

    /**
     * The aggregate type for AVG(expression).
     */
    static final int AVG = 6;

    /**
     * The aggregate type for STDDEV_POP(expression).
     */
    static final int STDDEV_POP = 7;

    /**
     * The aggregate type for STDDEV_SAMP(expression).
     */
    static final int STDDEV_SAMP = 8;

    /**
     * The aggregate type for VAR_POP(expression).
     */
    static final int VAR_POP = 9;

    /**
     * The aggregate type for VAR_SAMP(expression).
     */
    static final int VAR_SAMP = 10;

    /**
     * The aggregate type for BOOL_OR(expression).
     */
    static final int BOOL_OR = 11;

    /**
     * The aggregate type for BOOL_AND(expression).
     */
    static final int BOOL_AND = 12;

    /**
     * The aggregate type for SELECTIVITY(expression).
     */
    static final int SELECTIVITY = 13;

    private static final HashMap<String, Integer> AGGREGATES = New.hashMap();

    private final int type;
    private final Select select;
    private final boolean distinct;

    private Expression on;
    private Expression separator;
    private ObjectArray<SelectOrderBy> orderList;
    private SortOrder sort;
    private int dataType, scale;
    private long precision;
    private int displaySize;
    private int lastGroupRowId;

    /**
     * Create a new aggregate object.
     *
     * @param type the aggregate type
     * @param on the aggregated expression
     * @param select the select statement
     * @param distinct if distinct is used
     */
    public Aggregate(int type, Expression on, Select select, boolean distinct) {
        this.type = type;
        this.on = on;
        this.select = select;
        this.distinct = distinct;
    }

    static {
        addAggregate("COUNT", COUNT);
        addAggregate("SUM", SUM);
        addAggregate("MIN", MIN);
        addAggregate("MAX", MAX);
        addAggregate("AVG", AVG);
        addAggregate("GROUP_CONCAT", GROUP_CONCAT);
        addAggregate("STDDEV_SAMP", STDDEV_SAMP);
        addAggregate("STDDEV", STDDEV_SAMP);
        addAggregate("STDDEV_POP", STDDEV_POP);
        addAggregate("STDDEVP", STDDEV_POP);
        addAggregate("VAR_POP", VAR_POP);
        addAggregate("VARP", VAR_POP);
        addAggregate("VAR_SAMP", VAR_SAMP);
        addAggregate("VAR", VAR_SAMP);
        addAggregate("VARIANCE", VAR_SAMP);
        addAggregate("BOOL_OR", BOOL_OR);
        // HSQLDB compatibility, but conflicts with >EVERY(...)
        addAggregate("SOME", BOOL_OR);
        addAggregate("BOOL_AND", BOOL_AND);
        // HSQLDB compatibility, but conflicts with >SOME(...)
        addAggregate("EVERY", BOOL_AND);
        addAggregate("SELECTIVITY", SELECTIVITY);
    }

    private static void addAggregate(String name, int type) {
        AGGREGATES.put(name, type);
    }

    /**
     * Get the aggregate type for this name, or -1 if no aggregate has been
     * found.
     *
     * @param name the aggregate function name
     * @return -1 if no aggregate function has been found, or the aggregate type
     */
    public static int getAggregateType(String name) {
        Integer type = AGGREGATES.get(name);
        return type == null ? -1 : type.intValue();
    }

    /**
     * Set the order for GROUP_CONCAT.
     *
     * @param orderBy the order by list
     */
    public void setOrder(ObjectArray<SelectOrderBy> orderBy) {
        this.orderList = orderBy;
    }

    /**
     * Set the separator for GROUP_CONCAT.
     *
     * @param separator the separator expression
     */
    public void setSeparator(Expression separator) {
        this.separator = separator;
    }

    private SortOrder initOrder(Session session) {
        int[] index = new int[orderList.size()];
        int[] sortType = new int[orderList.size()];
        for (int i = 0; i < orderList.size(); i++) {
            SelectOrderBy o = orderList.get(i);
            index[i] = i + 1;
            int order = o.descending ? SortOrder.DESCENDING : SortOrder.ASCENDING;
            sortType[i] = order;
        }
        return new SortOrder(session.getDatabase(), index, sortType);
    }

    public void updateAggregate(Session session) throws SQLException {
        // TODO aggregates: check nested MIN(MAX(ID)) and so on
        // if(on != null) {
        // on.updateAggregate();
        // }
        HashMap<Expression, Object> group = select.getCurrentGroup();
        if (group == null) {
            // this is a different level (the enclosing query)
            return;
        }

        int groupRowId = select.getCurrentGroupRowId();
        if (lastGroupRowId == groupRowId) {
            // already visited
            return;
        }
        lastGroupRowId = groupRowId;

        AggregateData data = (AggregateData) group.get(this);
        if (data == null) {
            data = new AggregateData(type, dataType);
            group.put(this, data);
        }
        Value v = on == null ? null : on.getValue(session);
        if (type == GROUP_CONCAT) {
            if (v != ValueNull.INSTANCE) {
                v = v.convertTo(Value.STRING);
                if (orderList != null) {
                    Value[] array = new Value[1 + orderList.size()];
                    array[0] = v;
                    for (int i = 0; i < orderList.size(); i++) {
                        SelectOrderBy o = orderList.get(i);
                        array[i + 1] = o.expression.getValue(session);
                    }
                    v = ValueArray.get(array);
                }
            }
        }
        data.add(session.getDatabase(), distinct, v);
    }

    public Value getValue(Session session) throws SQLException {
        if (select.isQuickAggregateQuery()) {
            switch (type) {
            case COUNT_ALL:
                Table table = select.getTopTableFilter().getTable();
                return ValueLong.get(table.getRowCount(session));
            case MIN:
            case MAX:
                boolean first = type == MIN;
                Index index = getColumnIndex(first);
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
            default:
                Message.throwInternalError("type=" + type);
            }
        }
        HashMap<Expression, Object> group = select.getCurrentGroup();
        if (group == null) {
            throw Message.getSQLException(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getSQL());
        }
        AggregateData data = (AggregateData) group.get(this);
        if (data == null) {
            data = new AggregateData(type, dataType);
        }
        Value v = data.getValue(session.getDatabase(), distinct);
        if (type == GROUP_CONCAT) {
            ObjectArray<Value> list = data.getList();
            if (list == null || list.size() == 0) {
                return ValueNull.INSTANCE;
            }
            if (orderList != null) {
                try {
                    final SortOrder sortOrder = sort;
                    list.sort(new Comparator<Value>() {
                        public int compare(Value v1, Value v2) {
                            try {
                                Value[] a1 = ((ValueArray) v1).getList();
                                Value[] a2 = ((ValueArray) v2).getList();
                                return sortOrder.compare(a1, a2);
                            } catch (SQLException e) {
                                throw Message.convertToInternal(e);
                            }
                        }
                    });
                } catch (Exception e) {
                    throw Message.convert(e);
                }
            }
            StatementBuilder buff = new StatementBuilder();
            String sep = separator == null ? "," : separator.getValue(session).getString();
            for (Value val : list) {
                String s;
                if (val.getType() == Value.ARRAY) {
                    s = ((ValueArray) val).getList()[0].getString();
                } else {
                    s = val.convertTo(Value.STRING).getString();
                }
                if (s == null) {
                    continue;
                }
                if (sep != null) {
                    buff.appendExceptFirst(sep);
                }
                buff.append(s);
            }
            v = ValueString.get(buff.toString());
        }
        return v;
    }

    public int getType() {
        return dataType;
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        if (on != null) {
            on.mapColumns(resolver, level);
        }
        if (orderList != null) {
            for (SelectOrderBy o : orderList) {
                o.expression.mapColumns(resolver, level);
            }
        }
        if (separator != null) {
            separator.mapColumns(resolver, level);
        }
    }

    public Expression optimize(Session session) throws SQLException {
        if (on != null) {
            on = on.optimize(session);
            dataType = on.getType();
            scale = on.getScale();
            precision = on.getPrecision();
            displaySize = on.getDisplaySize();
        }
        if (orderList != null) {
            for (SelectOrderBy o : orderList) {
                o.expression = o.expression.optimize(session);
            }
            sort = initOrder(session);
        }
        if (separator != null) {
            separator = separator.optimize(session);
        }
        switch (type) {
        case GROUP_CONCAT:
            dataType = Value.STRING;
            scale = 0;
            precision = Integer.MAX_VALUE;
            displaySize = Integer.MAX_VALUE;
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
        case SUM:
            if (!DataType.supportsAdd(dataType)) {
                throw Message.getSQLException(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            }
            dataType = DataType.getAddProofType(dataType);
            break;
        case AVG:
            if (!DataType.supportsAdd(dataType)) {
                throw Message.getSQLException(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            }
            break;
        case MIN:
        case MAX:
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
        default:
            Message.throwInternalError("type=" + type);
        }
        return this;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if (on != null) {
            on.setEvaluatable(tableFilter, b);
        }
        if (orderList != null) {
            for (SelectOrderBy o : orderList) {
                o.expression.setEvaluatable(tableFilter, b);
            }
        }
        if (separator != null) {
            separator.setEvaluatable(tableFilter, b);
        }
    }

    public int getScale() {
        return scale;
    }

    public long getPrecision() {
        return precision;
    }

    public int getDisplaySize() {
        return displaySize;
    }

    private String getSQLGroupConcat() {
        StatementBuilder buff = new StatementBuilder("GROUP_CONCAT(");
        buff.append(on.getSQL());
        if (orderList != null) {
            buff.append(" ORDER BY ");
            for (SelectOrderBy o : orderList) {
                buff.appendExceptFirst(", ");
                buff.append(o.expression.getSQL());
                if (o.descending) {
                    buff.append(" DESC");
                }
            }
        }
        if (separator != null) {
            buff.append(" SEPARATOR ").append(separator.getSQL());
        }
        return buff.append(')').toString();
    }

    public String getSQL() {
        String text;
        switch (type) {
        case GROUP_CONCAT:
            return getSQLGroupConcat();
        case COUNT_ALL:
            return "COUNT(*)";
        case COUNT:
            text = "COUNT";
            break;
        case SELECTIVITY:
            text = "SELECTIVITY";
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
        default:
            throw Message.throwInternalError("type=" + type);
        }
        if (distinct) {
            return text + "(DISTINCT " + on.getSQL() + ")";
        }
        return text + StringUtils.enclose(on.getSQL());
    }

    private Index getColumnIndex(boolean first) {
        if (on instanceof ExpressionColumn) {
            ExpressionColumn col = (ExpressionColumn) on;
            Column column = col.getColumn();
            TableFilter filter = col.getTableFilter();
            if (filter != null) {
                Table table = filter.getTable();
                Index index = table.getIndexForColumn(column, first);
                return index;
            }
        }
        return null;
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        if (visitor.getType() == ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL) {
            switch (type) {
            case COUNT_ALL:
                return visitor.getTable().canGetRowCount();
            case MIN:
            case MAX:
                if (!SysProperties.OPTIMIZE_MIN_MAX) {
                    return false;
                }
                boolean first = type == MIN;
                Index index = getColumnIndex(first);
                return index != null;
            default:
                return false;
            }
        }
        if (on != null && !on.isEverything(visitor)) {
            return false;
        }
        if (separator != null && !separator.isEverything(visitor)) {
            return false;
        }
        for (int i = 0; orderList != null && i < orderList.size(); i++) {
            SelectOrderBy o = orderList.get(i);
            if (!o.expression.isEverything(visitor)) {
                return false;
            }
        }
        return true;
    }

    public int getCost() {
        return (on == null) ? 1 : on.getCost() + 1;
    }

}
