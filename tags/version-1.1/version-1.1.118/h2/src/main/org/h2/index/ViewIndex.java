/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import org.h2.command.dml.Query;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.expression.Comparison;
import org.h2.expression.Parameter;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.TableView;
import org.h2.util.IntArray;
import org.h2.util.MemoryUtils;
import org.h2.util.ObjectArray;
import org.h2.util.SmallLRUCache;
import org.h2.value.Value;

/**
 * This object represents a virtual index for a query.
 * Actually it only represents a prepared SELECT statement.
 */
public class ViewIndex extends BaseIndex {

    private final TableView view;
    private final String querySQL;
    private final ObjectArray<Parameter> originalParameters;
    private final SmallLRUCache<IntArray, CostElement> costCache = SmallLRUCache.newInstance(Constants.VIEW_INDEX_CACHE_SIZE);
    private boolean recursive;
    private int[] masks;
    private String planSQL;
    private Query query;
    private Session session;

    public ViewIndex(TableView view, String querySQL, ObjectArray<Parameter> originalParameters, boolean recursive) {
        initBaseIndex(view, 0, null, null, IndexType.createNonUnique(false));
        this.view = view;
        this.querySQL = querySQL;
        this.originalParameters = originalParameters;
        this.recursive = recursive;
        columns = new Column[0];
    }

    public ViewIndex(TableView view, ViewIndex index, Session session, int[] masks) throws SQLException {
        initBaseIndex(view, 0, null, null, IndexType.createNonUnique(false));
        this.view = view;
        this.querySQL = index.querySQL;
        this.originalParameters = index.originalParameters;
        this.recursive = index.recursive;
        this.masks = masks;
        this.session = session;
        columns = new Column[0];
        query = getQuery(session, masks);
        planSQL =  query.getPlanSQL();
    }

    public Session getSession() {
        return session;
    }

    public String getPlanSQL() {
        return planSQL;
    }

    public void close(Session session) {
        // nothing to do
    }

    public void add(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException("VIEW");
    }

    public void remove(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException("VIEW");
    }

    /**
     * A calculated cost value.
     */
    static class CostElement {

        /**
         * The time in milliseconds when this cost was calculated.
         */
        long evaluatedAt;

        /**
         * The cost.
         */
        double cost;
    }

    public double getCost(Session session, int[] masks) throws SQLException {
        IntArray masksArray = new IntArray(masks == null ? MemoryUtils.EMPTY_INTS : masks);
        CostElement cachedCost = costCache.get(masksArray);
        if (cachedCost != null) {
            long time = System.currentTimeMillis();
            if (time < cachedCost.evaluatedAt + Constants.VIEW_COST_CACHE_MAX_AGE) {
                return cachedCost.cost;
            }
        }
        Query query = (Query) session.prepare(querySQL, true);
        if (masks != null) {
            IntArray paramIndex = new IntArray();
            for (int i = 0; i < masks.length; i++) {
                int mask = masks[i];
                if (mask == 0) {
                    continue;
                }
                paramIndex.add(i);
            }
            int len = paramIndex.size();
            for (int i = 0; i < len; i++) {
                int idx = paramIndex.get(i);
                int mask = masks[idx];
                int nextParamIndex = query.getParameters().size() + view.getParameterOffset();
                if ((mask & IndexCondition.EQUALITY) != 0) {
                    Parameter param = new Parameter(nextParamIndex);
                    query.addGlobalCondition(param, idx, Comparison.EQUAL);
                } else {
                    if ((mask & IndexCondition.START) != 0) {
                        Parameter param = new Parameter(nextParamIndex);
                        query.addGlobalCondition(param, idx, Comparison.BIGGER_EQUAL);
                    }
                    if ((mask & IndexCondition.END) != 0) {
                        Parameter param = new Parameter(nextParamIndex);
                        query.addGlobalCondition(param, idx, Comparison.SMALLER_EQUAL);
                    }
                }
            }
            if (recursive) {
                return 10;
            }
            String sql = query.getPlanSQL();
            query = (Query) session.prepare(sql, true);
        }
        double cost = query.getCost();
        cachedCost = new CostElement();
        cachedCost.evaluatedAt = System.currentTimeMillis();
        cachedCost.cost = cost;
        costCache.put(masksArray, cachedCost);
        return cost;
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        ObjectArray<Parameter> paramList = query.getParameters();
        for (int i = 0; originalParameters != null && i < originalParameters.size(); i++) {
            Parameter orig = originalParameters.get(i);
            int idx = orig.getIndex();
            // the parameter may have been optimized away
            if (idx < paramList.size()) {
                Parameter param = paramList.get(idx);
                Value value = orig.getValue(session);
                param.setValue(value);
            }
        }
        int len;
        if (first != null) {
            len = first.getColumnCount();
        } else if (last != null) {
            len = last.getColumnCount();
        } else {
            len = 0;
        }
        int idx = originalParameters == null ? 0 : originalParameters.size();
        idx += view.getParameterOffset();
        for (int i = 0; i < len; i++) {
            if (first != null) {
                Value v = first.getValue(i);
                if (v != null) {
                    Parameter param = paramList.get(idx++);
                    param.setValue(v);
                }
            }
            // for equality, only one parameter is used (first == last)
            if (last != null && masks[i] != IndexCondition.EQUALITY) {
                Value v = last.getValue(i);
                if (v != null) {
                    Parameter param = paramList.get(idx++);
                    param.setValue(v);
                }
            }
        }
        LocalResult result = query.query(0);
        return new ViewCursor(table, result);
    }

    private Query getQuery(Session session, int[] masks) throws SQLException {
        Query query = (Query) session.prepare(querySQL, true);
        if (masks == null) {
            return query;
        }
        int firstIndexParam = originalParameters == null ? 0 : originalParameters.size();
        firstIndexParam += view.getParameterOffset();
        IntArray paramIndex = new IntArray();
        for (int i = 0; i < masks.length; i++) {
            int mask = masks[i];
            if (mask == 0) {
                continue;
            }
            paramIndex.add(i);
            if ((mask & IndexCondition.RANGE) == IndexCondition.RANGE) {
                // two parameters for range queries: >= x AND <= y
                paramIndex.add(i);
            }
        }
        int len = paramIndex.size();
        columns = new Column[len];
        for (int i = 0; i < len;) {
            int idx = paramIndex.get(i);
            Column col = table.getColumn(idx);
            columns[i] = col;
            int mask = masks[idx];
            if ((mask & IndexCondition.EQUALITY) == IndexCondition.EQUALITY) {
                Parameter param = new Parameter(firstIndexParam + i);
                query.addGlobalCondition(param, idx, Comparison.EQUAL);
                i++;
            } else {
                if ((mask & IndexCondition.START) == IndexCondition.START) {
                    Parameter param = new Parameter(firstIndexParam + i);
                    query.addGlobalCondition(param, idx, Comparison.BIGGER_EQUAL);
                    i++;
                }
                if ((mask & IndexCondition.END) == IndexCondition.END) {
                    Parameter param = new Parameter(firstIndexParam + i);
                    query.addGlobalCondition(param, idx, Comparison.SMALLER_EQUAL);
                    i++;
                }
            }
        }
        String sql = query.getPlanSQL();
        query = (Query) session.prepare(sql, true);
        return query;
    }

    public void remove(Session session) throws SQLException {
        throw Message.getUnsupportedException("VIEW");
    }

    public void truncate(Session session) throws SQLException {
        throw Message.getUnsupportedException("VIEW");
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException("VIEW");
    }

    public boolean needRebuild() {
        return false;
    }

    public boolean canGetFirstOrLast() {
        return false;
    }

    public Cursor findFirstOrLast(Session session, boolean first) throws SQLException {
        throw Message.getUnsupportedException("VIEW");
    }

    public void setRecursive(boolean value) {
        this.recursive = value;
    }

    public long getRowCount(Session session) {
        return 0;
    }

    public long getRowCountApproximation() {
        return 0;
    }

}
