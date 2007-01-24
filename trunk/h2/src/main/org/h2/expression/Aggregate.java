/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;

import org.h2.command.dml.Select;
import org.h2.command.dml.SelectOrderBy;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueDouble;
import org.h2.value.ValueInt;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * @author Thomas
 */

public class Aggregate extends Expression {
    // TODO aggregates: make them 'pluggable'
    // TODO incompatibility to hsqldb: aggregates: hsqldb uses automatic data type for sum if value is too big, 
    // h2 uses the same type as the data
    public static final int COUNT_ALL = 0, COUNT = 1, SUM = 2, MIN = 3, MAX = 4, AVG = 5;
    public static final int GROUP_CONCAT = 6, STDDEV_POP = 7, STDDEV_SAMP = 8;
    public static final int VAR_POP = 9, VAR_SAMP = 10, SOME = 11, EVERY = 12, SELECTIVITY = 13;
    private int type;
    private Expression on;
    private Expression separator;
    private ObjectArray orderList;
    private SortOrder sort;
    private int dataType, scale;
    private long precision;
    private Select select;
    private Database database;
    private boolean distinct;
    
    private static HashMap aggregates = new HashMap();
    
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
        addAggregate("SOME", SOME);
        addAggregate("EVERY", EVERY);
        addAggregate("SELECTIVITY", SELECTIVITY);
    }
    
    private static void addAggregate(String name, int type) {
        aggregates.put(name, new Integer(type));
    }
    
    public static int getAggregateType(String name) {
        Integer type = (Integer) aggregates.get(name);
        return type == null ? -1 : type.intValue();
    }    

    public Aggregate(Database database, int type, Expression on, Select select, boolean distinct) {
        this.database = database;
        this.type = type;
        this.on = on;
        this.select = select;
        this.distinct = distinct;
    }
    
    public void setOrder(ObjectArray orderBy) {
        this.orderList = orderBy;
    }
    
    public void setSeparator(Expression separator) {
        this.separator = separator;
    }    
    
    private SortOrder initOrder(Session session) throws SQLException {
        int[] index = new int[orderList.size()];
        int[] sortType = new int[orderList.size()];
        for(int i=0; i<orderList.size(); i++) {
            SelectOrderBy o = (SelectOrderBy) orderList.get(i);
            index[i] = i+1;
            int type = o.descending ? SortOrder.DESCENDING : SortOrder.ASCENDING;
            sortType[i] = type;
        }
        return new SortOrder(session.getDatabase(), index, sortType);
    }

    public void updateAggregate(Session session) throws SQLException {
        // TODO aggregates: check nested MIN(MAX(ID)) and so on
//        if(on != null) {
//            on.updateAggregate();
//        }
        HashMap group = select.getCurrentGroup();
        AggregateData data = (AggregateData) group.get(this);
        if(data == null) {
            data = new AggregateData(type);
            group.put(this, data);
        }
        Value v = on == null ? null : on.getValue(session);
        if(type == GROUP_CONCAT) {
            if(v != ValueNull.INSTANCE) {
                v = v.convertTo(Value.STRING);
                if(orderList != null) {
                    Value[] array = new Value[1 + orderList.size()];
                    array[0] = v;
                    for(int i=0; i<orderList.size(); i++) {
                        SelectOrderBy o = (SelectOrderBy) orderList.get(i);
                        array[i+1] = o.expression.getValue(session);
                    }
                    v = ValueArray.get(array);
                }
            }
        }
        data.add(database, distinct, v);
    }

    public Value getValue(Session session) throws SQLException {
        if(select.isQuickQuery()) {
            switch(type) {
            case COUNT_ALL:
                Table table = select.getTopTableFilter().getTable();
                return ValueInt.get(table.getRowCount());
            case MIN:
            case MAX:
                boolean first = type == MIN;
                Index index = getColumnIndex(first);
                Value v = index.findFirstOrLast(session, first);
                return v;
            default:
                throw Message.getInternalError("type="+type);
            }
        }
        HashMap group = select.getCurrentGroup();
        if(group == null) {
            throw Message.getSQLException(Message.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getSQL());
        }
        AggregateData data = (AggregateData) group.get(this);
        if(data == null) {
            data = new AggregateData(type);
        }
        Value v = data.getValue(database, distinct);
        if(type == GROUP_CONCAT) {
            ObjectArray list = data.getList();
            if(list == null || list.size()==0) {
                return ValueNull.INSTANCE;
            }
            if(orderList != null) {
                try {
                    // TODO refactor: don't use built in comparator
                    list.sort(new Comparator() {
                        public int compare(Object o1, Object o2) {
                            try {
                                Value[] a1 = ((ValueArray)o1).getList();
                                Value[] a2 = ((ValueArray)o2).getList();
                                return sort.compare(a1, a2);
                            } catch(SQLException e) {
                                throw Message.getInternalError("sort", e);
                            }
                        }
                    });
                } catch(Error e) {
                    throw Message.convert(e);
                }
            }
            StringBuffer buff = new StringBuffer();
            String sep = separator == null ? "," : separator.getValue(session).getString();
            for(int i=0; i<list.size(); i++) {
                Value val = (Value)list.get(i);
                String s;
                if(val.getType() == Value.ARRAY) {
                    s = ((ValueArray)val).getList()[0].getString();
                } else {
                    s = val.convertTo(Value.STRING).getString();
                }
                if(s == null) {
                    continue;
                }
                if(i > 0 && sep != null) {
                    buff.append(sep);
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
        if(on != null) {
            on.mapColumns(resolver, level);
        }
        if(orderList != null) {
            for(int i=0; i<orderList.size(); i++) {
                SelectOrderBy o = (SelectOrderBy) orderList.get(i);
                o.expression.mapColumns(resolver, level);
            }
        }
        if(separator != null) {
            separator.mapColumns(resolver, level);
        }
    }

    public Expression optimize(Session session) throws SQLException {
        if(on != null) {
            on = on.optimize(session);
            dataType = on.getType();
            scale = on.getScale();
            precision = on.getPrecision();
        }
        if(orderList != null) {
            for(int i=0; i<orderList.size(); i++) {
                SelectOrderBy o = (SelectOrderBy) orderList.get(i);
                o.expression = o.expression.optimize(session);
            }
            sort = initOrder(session);            
        }
        if(separator != null) {
            separator = separator.optimize(session);
        }        
        switch(type) {
        case GROUP_CONCAT:
            dataType = Value.STRING;
            scale = 0;
            precision = 0;
            break;
        case COUNT_ALL:
        case COUNT:
        case SELECTIVITY:
            dataType = Value.INT;
            scale = 0;
            precision = 0;
            break;            
        case SUM:
        case AVG:
            if(!DataType.supportsAdd(dataType)) {
                throw Message.getSQLException(Message.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
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
            scale = 0;
            break;
        case EVERY:
        case SOME:
            dataType = Value.BOOLEAN;
            precision = ValueBoolean.PRECISION;
            scale = 0;
            break;
        default:
            throw Message.getInternalError("type="+type);
        }
        return this;
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if(on != null) {
            on.setEvaluatable(tableFilter, b);
        }
        if(orderList != null) {
            for(int i=0; i<orderList.size(); i++) {
                SelectOrderBy o = (SelectOrderBy) orderList.get(i);
                o.expression.setEvaluatable(tableFilter, b);
            }
        }        
        if(separator != null) {
            separator.setEvaluatable(tableFilter, b);
        }        
    }

    public int getScale() {
        return scale;
    }

    public long getPrecision() {
        return precision;
    }
    
    public String getSQL() {
        String text;
        switch(type) {
        case GROUP_CONCAT: {
            StringBuffer buff = new StringBuffer();
            buff.append("GROUP_CONCAT(");
            buff.append(on.getSQL());
            if(orderList != null) {
                buff.append(" ORDER BY ");
                if(orderList != null) {
                    for(int i=0; i<orderList.size(); i++) {
                        SelectOrderBy o = (SelectOrderBy) orderList.get(i);                        
                        if(i > 0) {
                            buff.append(", ");
                        }
                        buff.append(o.expression.getSQL());
                        if(o.descending) {
                            buff.append(" DESC");
                        }
                    }
                } 
            }
            if(separator != null) {
                buff.append(" SEPARATOR ");
                buff.append(separator.getSQL());
            }
            buff.append(")");
            return buff.toString();
        }
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
            text="AVG";
            break;
        case STDDEV_POP:
            text="STDDEV_POP";
            break;
        case STDDEV_SAMP:
            text="STDDEV_SAMP";
            break;
        case VAR_POP:
            text="VAR_POP";
            break;
        case VAR_SAMP:
            text="VAR_SAMP";
            break;
        case EVERY:
            text="EVERY";
            break;
        case SOME:
            text="SOME";
            break;
        default:
            throw Message.getInternalError("type="+type);
        }
        if(distinct) {
            return text + "(DISTINCT " + on.getSQL()+")";
        } else{
            return text + StringUtils.enclose(on.getSQL());
        }
    }
    
    public int getAggregateType() {
        return type;
    }         
    
    private Index getColumnIndex(boolean first) {
        if(on instanceof ExpressionColumn) {
            ExpressionColumn col = (ExpressionColumn)on;
            Column column = col.getColumn();
            Table table = col.getTableFilter().getTable();
            Index index = table.getIndexForColumn(column, first);
            return index;
        }
        return null;
    }
    
    public boolean isEverything(ExpressionVisitor visitor) {
        if(visitor.type == ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL) {
            switch(type) {
            case COUNT_ALL:
                return visitor.table.canGetRowCount();
            case MIN:
            case MAX:
                if(!Constants.OPTIMIZE_MIN_MAX) {
                    return false;
                }
                boolean first = type == MIN;
                Index index = getColumnIndex(first);
                return index != null;
            default:
                return false;
            }
        }        
        return (on == null || on.isEverything(visitor)) && (separator == null || separator.isEverything(visitor));
    }
    
    public int getCost() {
        return (on == null) ? 1 : on.getCost() + 1;
    }

}
