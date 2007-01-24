/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;
import java.util.HashSet;

import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.expression.Alias;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.SortOrder;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

public abstract class Query extends Prepared {
    
    protected Expression limit, offset;
    protected int sampleSize;
    
    private int lastLimit;
    private long lastEvaluated;
    private LocalResult lastResult;
    private Value[] lastParameters;
    
    abstract LocalResult queryWithoutCache(int limit) throws SQLException;
    
    public Query(Session session) {
        super(session);
    }
    
    public boolean isQuery() {
        return true;
    }

    public boolean isTransactional() {
        return true;
    }
    
    public final boolean sameResultAsLast(Session session, Value[] params, Value[] lastParams, long lastEvaluated) throws SQLException {
        for(int i=0; i<params.length; i++) {
            if(!session.getDatabase().areEqual(lastParams[i], params[i])) {
                return false;
            }
        }
        if(!isEverything(ExpressionVisitor.DETERMINISTIC) || !isEverything(ExpressionVisitor.INDEPENDENT)) {
            return false;
        }
        if(getMaxDataModificationId() > lastEvaluated) {
            return false;
        }
        return true;
    }
    
    public final Value[] getParameterValues() throws SQLException {
        ObjectArray list = getParameters();
        if(list == null) {
            list = new ObjectArray();
        }
        Value[] params = new Value[list.size()];
        for(int i=0; i<list.size(); i++) {
            Value v = ((Parameter) list.get(i)).getParamValue();
            params[i] = v;
        }
        return params;
    }
    
    public final LocalResult query(int limit) throws SQLException {
        if(!session.getDatabase().getOptimizeReuseResults()) {
            return queryWithoutCache(limit);
        }
        Value[] params = getParameterValues();
        long now = session.getDatabase().getModificationDataId();
        if(lastResult != null && limit == lastLimit) {
            if(sameResultAsLast(session, params, lastParameters, lastEvaluated)) {
                lastResult = lastResult.createShallowCopy(session);
                lastResult.reset();
                return lastResult;
            }
        }
        lastParameters = params;
        lastResult = queryWithoutCache(limit);
        this.lastEvaluated = now;
        lastLimit = limit;
        return lastResult;
    }

    protected SortOrder initOrder(ObjectArray expressions, ObjectArray orderList, int visible, boolean mustBeInResult) throws SQLException {
        int[] index = new int[orderList.size()];
        int[] sortType = new int[orderList.size()];
        int originalLength = expressions.size();
        for(int i=0; i<orderList.size(); i++) {
            SelectOrderBy o = (SelectOrderBy) orderList.get(i);
            int idx;
            if(o.expression != null) {
                Expression e = o.expression;
                // special case: SELECT 1 AS A FROM DUAL ORDER BY A
                // (oracle supports it, but only in order by, not in group by and not in having): 
                // SELECT 1 AS A FROM DUAL ORDER BY -A
                boolean isAlias = false;
                idx = expressions.size();
                if(e instanceof ExpressionColumn) {
                    ExpressionColumn exprCol = (ExpressionColumn)e;
                    String alias = exprCol.getOriginalAliasName();
                    String col = exprCol.getOriginalColumnName();
                    for(int j=0; j<visible; j++) {
                        boolean found = false;
                        Expression ec = (Expression) expressions.get(j);
                        if(ec instanceof ExpressionColumn) {
                            ExpressionColumn c = (ExpressionColumn) ec;
                            found = col.equals(c.getColumnName());
                            if(alias != null && found) {
                                found = alias.equals(c.getOriginalAliasName());
                            }
                        } else if(!(ec instanceof Alias)) {
                            continue;
                        } else if(col.equals(ec.getAlias())) {
                            found = true;
                        } else {
                            Expression ec2 = ec.getNonAliasExpression();
                            if(ec2 instanceof ExpressionColumn) {
                                ExpressionColumn c2 = (ExpressionColumn) ec2;
                                found = col.equals(c2.getColumnName());
                            }
                        }
                        if(found) {
                            idx = j;
                            isAlias = true;
                            break;
                        }
                    }
                }
                if(!isAlias) {
                    if(mustBeInResult) {
                        throw Message.getSQLException(Message.ORDER_BY_NOT_IN_RESULT, e.getSQL());
                    }            
                    expressions.add(e);
                }
            } else {
                idx = o.column;
                if(idx >= originalLength) {
                    throw Message.getSQLException(Message.ORDER_BY_NOT_IN_RESULT, "index " + idx);
                }
            }
            index[i] = idx;
            int type = o.descending ? SortOrder.DESCENDING : SortOrder.ASCENDING;
            if(o.nullsFirst) {
                type += SortOrder.NULLS_FIRST;
            } else if(o.nullsLast) {
                type += SortOrder.NULLS_LAST;
            }
            sortType[i] = type;
        }
        return new SortOrder(session.getDatabase(), index, sortType);
    }
    
    public void setOffset(Expression offset) {
        this.offset = offset;
    }
    
    public void setLimit(Expression limit) {
        this.limit = limit;
    }

    public abstract void init() throws SQLException;
    public abstract ObjectArray getExpressions();
    public abstract double getCost();
    public abstract HashSet getTables();
    public abstract void setOrder(ObjectArray order);
    public abstract void setForUpdate(boolean forUpdate);
    public abstract int getColumnCount();
    public abstract void mapColumns(ColumnResolver resolver, int level) throws SQLException;
    public abstract void setEvaluatable(TableFilter tableFilter, boolean b);
    public abstract void addGlobalCondition(Expression expr, int columnId, int comparisonType) throws SQLException;
    public abstract void setDistinct(boolean b);

    public void setSampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
    }

    public final long getMaxDataModificationId() {
        ExpressionVisitor visitor = ExpressionVisitor.get(ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID);
        isEverything(visitor);
        return visitor.getMaxDataModificationId();
    }

    public abstract boolean isEverything(ExpressionVisitor visitor);
    
    public final boolean isEverything(int expressionVisitorType) {
        ExpressionVisitor visitor = ExpressionVisitor.get(expressionVisitorType);
        return isEverything(visitor);
    }

}
