/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;
import java.util.HashSet;

import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.ValueExpression;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueInt;

public class SelectUnion extends Query {
    public static final int UNION=0, UNION_ALL=1, EXCEPT=2, INTERSECT=3;
    private int unionType;
    private Query left, right;
    private ObjectArray expressions;
    private ObjectArray orderList;
    private SortOrder sort;    
    private boolean distinct;
    private boolean checkPrepared, checkInit;
    private boolean isForUpdate;
    
    public SelectUnion(Session session, Query query) {
        super(session);
        this.left = query;
    }
    
    public void setUnionType(int type) {
        this.unionType = type;
    }
    
    public void setRight(Query select) throws JdbcSQLException {
        right = select;
    }
    
    
    
    public void setSQL(String sql) {
        this.sql = sql;
    }    
    
    public void setOrder(ObjectArray order) {
        orderList = order;
    }
    
    private Value[] convert(Value[] values) throws SQLException {
        for(int i=0; i<values.length; i++) {
            Expression e = (Expression) expressions.get(i);
            values[i] = values[i].convertTo(e.getType());
        }
        return values;
    }

    public LocalResult queryWithoutCache(int maxrows) throws SQLException {
        if(maxrows != 0) {
            if(limit != null) {
                maxrows = Math.min(limit.getValue(session).getInt(), maxrows);
            }
            limit = ValueExpression.get(ValueInt.get(maxrows));
        }
        ObjectArray expressions = left.getExpressions();
        int columnCount = left.getColumnCount();
        LocalResult result = new LocalResult(session, expressions, columnCount);
        result.setSortOrder(sort);
        if(distinct) {
            left.setDistinct(true);
            right.setDistinct(true);
            result.setDistinct();
        }
        switch(unionType) {
        case UNION:
            left.setDistinct(true);
            right.setDistinct(true);
            result.setDistinct();
            break;
        case UNION_ALL:
            break;
        case EXCEPT:
            result.setDistinct();
            // fall through
        case INTERSECT: {
            left.setDistinct(true);
            right.setDistinct(true);
            break;
        }
        default:
            throw Message.getInternalError("type="+unionType);
        }
        LocalResult l = left.query(0);
        LocalResult r = right.query(0);
        l.reset();
        r.reset();
        switch(unionType) {
        case UNION_ALL:
        case UNION: {
            while(l.next()) {
                result.addRow(convert(l.currentRow()));
            }
            while(r.next()) {
                result.addRow(convert(r.currentRow()));
            }
            break;
        }
        case EXCEPT: {
            while(l.next()) {
                result.addRow(convert(l.currentRow()));
            }
            while(r.next()) {
                result.removeDistinct(convert(r.currentRow()));
            }
            break;
        }
        case INTERSECT: {
            LocalResult temp = new LocalResult(session, expressions, columnCount);
            temp.setDistinct();
            while(l.next()) {
                temp.addRow(convert(l.currentRow()));
            }            
            while(r.next()) {
                Value[] values = convert(r.currentRow());
                if(temp.containsDistinct(values)) {
                    result.addRow(values);
                }
            }
            break;
        }
        default:
            throw Message.getInternalError("type="+unionType);
        }
        if(offset != null) {
            result.setOffset(offset.getValue(session).getInt());
        }
        if(limit != null) {
            result.setLimit(limit.getValue(session).getInt());
        }
        result.done();
        return result;
    }
    
    public void init() throws SQLException {
        if(Constants.CHECK && checkInit) {
            throw Message.getInternalError();
        }
        checkInit = true;
        left.init();
        right.init();
        int len = left.getColumnCount();
        if(len != right.getColumnCount()) {
            throw Message.getSQLException(Message.COLUMN_COUNT_DOES_NOT_MATCH);
        }
    }

    public void prepare() throws SQLException {
        if(Constants.CHECK && (checkPrepared || !checkInit)) {
            throw Message.getInternalError("already prepared");
        }
        checkPrepared = true;        
        left.prepare();
        right.prepare();
        ObjectArray le = left.getExpressions();
        ObjectArray re = right.getExpressions();
        expressions = new ObjectArray();
        int len = left.getColumnCount();
        for(int i=0; i<len; i++) {
            Expression l = (Expression)le.get(i);
            Expression r = (Expression)re.get(i);
            int type = Value.getHigherOrder(l.getType(), r.getType());
            long prec = Math.max(l.getPrecision(), r.getPrecision());
            int scale = Math.max(l.getScale(), r.getScale());
            Column col = new Column(l.getAlias(), type, prec, scale);
            Expression e = new ExpressionColumn(session.getDatabase(), null, col);
            expressions.add(e);
        }
        if(orderList != null) {
            sort = initOrder(expressions, orderList, getColumnCount(), true);
            orderList = null;
        }
    }

    public double getCost() {
        return left.getCost() + right.getCost();
    }
    
    public HashSet getTables() {
        HashSet set = left.getTables();
        set.addAll(right.getTables());
        return set;
    }
    
    public void setDistinct(boolean b) {
        distinct = b;
    }

    public ObjectArray getExpressions() {
        return expressions;
    }

    public void setForUpdate(boolean forUpdate) {
        left.setForUpdate(forUpdate);
        right.setForUpdate(forUpdate);
        isForUpdate = forUpdate;
    }

    public int getColumnCount() {
        return left.getColumnCount();
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        left.mapColumns(resolver, level);
        right.mapColumns(resolver, level);        
    }
    
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        right.setEvaluatable(tableFilter, b);
    }

    public void addGlobalCondition(Expression expr, int columnId, int comparisonType) throws SQLException {
        switch(unionType) {
        case UNION_ALL:
        case UNION:
        case INTERSECT: {
            left.addGlobalCondition(expr, columnId, comparisonType);
            right.addGlobalCondition(expr, columnId, comparisonType);
            break;
        }
        case EXCEPT: {
            left.addGlobalCondition(expr, columnId, comparisonType);
            break;
        }
        default:
            throw Message.getInternalError("type="+unionType);
        }
    }
    
    public String getPlan() {
        StringBuffer buff = new StringBuffer();
        buff.append('(');
        buff.append(left.getPlan());
        buff.append(") ");
        switch(unionType) {
        case UNION_ALL:
            buff.append("UNION ALL ");
            break;
        case UNION:
            buff.append("UNION ");
            break;
        case INTERSECT:
            buff.append("INTERSECT ");
            break;
        case EXCEPT:
            buff.append("EXCEPT ");
            break;
        default:
            throw Message.getInternalError("type="+unionType);
        }
        buff.append('(');
        buff.append(right.getPlan());
        buff.append(')');        
        Expression[] exprList = new Expression[expressions.size()];
        expressions.toArray(exprList);
        if(sort != null) {
            buff.append(" ORDER BY ");
            buff.append(sort.getSQL(exprList, exprList.length));
        }
        // TODO refactoring: limit and order by could be in Query (now in SelectUnion and in Select) 
        if(limit != null) {
            buff.append(" LIMIT ");
            buff.append(StringUtils.unEnclose(limit.getSQL()));
            if(offset != null) {
                buff.append(" OFFSET ");
                buff.append(StringUtils.unEnclose(offset.getSQL()));
            }
        }
        if(isForUpdate) {
            buff.append(" FOR UPDATE");
        }
        return buff.toString();
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && right.isEverything(visitor);
    }

}
