/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.command.dml.Query;
import org.h2.command.dml.SelectUnion;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.expression.Comparison;
import org.h2.expression.Parameter;
import org.h2.expression.ValueExpression;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.TableView;
import org.h2.util.IntArray;
import org.h2.util.SmallLRUCache;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

public class ViewIndex extends Index {

    private String querySQL;
    private ObjectArray originalParameters;
    private Parameter[] params;
    
    private SmallLRUCache costCache = new SmallLRUCache(Constants.VIEW_COST_CACHE_SIZE);
    
    private Value[] lastParameters;
    private long lastEvaluated;
    private LocalResult lastResult;
    private boolean recursive;
    private int recurseLevel;
    private LocalResult recursiveResult;
    
    public ViewIndex(TableView view, String querySQL, ObjectArray originalParameters, boolean recursive) {
        super(view, 0, null, null, IndexType.createNonUnique(false));
        this.querySQL = querySQL;
        this.originalParameters = originalParameters;
        this.recursive = recursive;
        columns = new Column[0];
        params = new Parameter[0];
    }
    
    public String getPlanSQL() {
        return querySQL;
    }

    public void close(Session session) throws SQLException {
    }

    public void add(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void remove(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException();
    }

    private int getComparisonType(int mask) {
        if((mask & IndexCondition.EQUALITY) == IndexCondition.EQUALITY) {
            return Comparison.EQUAL;
        } else if ((mask & IndexCondition.RANGE) == IndexCondition.RANGE) {
            // TODO view: how to do range queries?
            return Comparison.BIGGER_EQUAL;
        } else if ((mask & IndexCondition.START) == IndexCondition.START) {
            return Comparison.BIGGER_EQUAL;
        } else if ((mask & IndexCondition.END) == IndexCondition.END) {
            return Comparison.SMALLER_EQUAL;
        }
        throw Message.getInternalError("unsupported mask "+mask);
    }
    
    private static class CostElement {
        long evaluatedAt;
        double cost;
    }
    
    public double getCost(Session session, int[] masks) throws SQLException {
        if(recursive) {
            return 10;
        }
        IntArray masksArray = new IntArray(masks == null ? new int[0] : masks);
        CostElement cachedCost = (CostElement) costCache.get(masksArray);
        if(cachedCost != null) {
            long time = System.currentTimeMillis();
            if(time < cachedCost.evaluatedAt + Constants.VIEW_COST_CACHE_MAX_AGE) {
                return cachedCost.cost;
            }
        }
        Query query = (Query)session.prepare(querySQL, true);
        if(masks == null) {
            columns = new Column[0];
            params = new Parameter[0];
        } else {
            IntArray paramIndex = new IntArray();
            for(int i=0; i<masks.length; i++) {
                int mask = masks[i];
                if(mask == 0) {
                    continue;
                }
                paramIndex.add(i);
            }
            int len = paramIndex.size();
            columns = new Column[len];
            params = new Parameter[len];
            for(int i=0; i<len; i++) {
                int idx = paramIndex.get(i);
                Column col = table.getColumn(idx);
                columns[i] = col;
                Parameter param = new Parameter(0);
                params[i] = param;
                int mask = masks[idx];
                int comparisonType = getComparisonType(mask);
                query.addGlobalCondition(param, idx, comparisonType);
            }
            if(recursive) {
                return 10;
            }
            String sql = query.getSQL();
            query = (Query)session.prepare(sql);
        }
        double cost = query.getCost();
        cachedCost = new CostElement();
        cachedCost.evaluatedAt = System.currentTimeMillis();
        cachedCost.cost = cost;
        costCache.put(masksArray, cachedCost);
        return cost;
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        Query query = (Query)session.prepare(querySQL, true);
        if(recursive) {
            SelectUnion union = (SelectUnion) query;
            Query left = union.getLeftQuery();
            Query right = union.getRightQuery();
            LocalResult completeResult;
            if(recurseLevel==0) {
                LocalResult result = left.query(0);
                completeResult = result;
                recurseLevel = 1;
                result = left.query(0);
                while(true) {
                    recursiveResult = result;
                    recurseLevel++;
                    result = right.query(0);
                    if(result.getRowCount() == 0) {
                        break;
                    }
                    result = right.query(0);
                    while(result.next()) {
                        completeResult.addRow(result.currentRow());
                    }
                }
                completeResult.done();
                recurseLevel=0;
                return new ViewCursor(table, completeResult);
            } else {
                return new ViewCursor(table, recursiveResult);
            }
        }
        ObjectArray paramList = query.getParameters();
        for(int i=0; first != null && i<first.getColumnCount(); i++) {
            Value v = first.getValue(i);
            if(v != null) {
                query.addGlobalCondition(ValueExpression.get(v), i, Comparison.BIGGER_EQUAL);
            }
        }
        for(int i=0; last != null && i<last.getColumnCount(); i++) {
            Value v = last.getValue(i);
            if(v != null) {
                query.addGlobalCondition(ValueExpression.get(v), i, Comparison.SMALLER_EQUAL);
            }
        }
        for(int i=0; originalParameters != null && i<originalParameters.size(); i++) {
            Parameter orig = (Parameter) originalParameters.get(i);
            Parameter param = (Parameter) paramList.get(i);
            Value value = orig.getValue(session);
            param.setValue(value);
        }
        boolean canReuse = first == null && last == null;
        long now = session.getDatabase().getModificationDataId();
        Value[] params = query.getParameterValues();
        if(session.getDatabase().getOptimizeReuseResults()) {
            if(lastResult != null && canReuse) {
                if(query.sameResultAsLast(session, params, lastParameters, lastEvaluated)) {
                    lastResult = lastResult.createShallowCopy(session);
                    if(lastResult != null) {
                        lastResult.reset();
                        return new ViewCursor(table, lastResult);
                    }
                }
            }
        }
        query.setSession(session);
        LocalResult result = query.query(0);
        if(canReuse) {
            lastResult = result;
            lastParameters = params;
            lastEvaluated = now;
        }
        return new ViewCursor(table, result);
    }
    
    public int getCost(int[] masks) throws SQLException {
        if(masks != null) {
            throw Message.getUnsupportedException();
        }
        return Integer.MAX_VALUE;
    }

    public void remove(Session session) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void truncate(Session session) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException();
    }

    public boolean needRebuild() {
        return false;
    }
    
    public boolean canGetFirstOrLast(boolean first) {
        return false;
    }

    public Value findFirstOrLast(Session session, boolean first) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void setRecursive(boolean value) {
        this.recursive = value;
    }     

}
