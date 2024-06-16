/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.ArrayList;

import org.h2.command.query.AllColumnsForPlan;
import org.h2.command.query.Query;
import org.h2.engine.Constants;
import org.h2.engine.SessionLocal;
import org.h2.expression.Parameter;
import org.h2.expression.condition.Comparison;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.QueryExpressionTable;
import org.h2.table.TableFilter;
import org.h2.util.IntArray;
import org.h2.value.Value;

/**
 * A regular query expression index.
 */
public final class RegularQueryExpressionIndex extends QueryExpressionIndex implements SpatialIndex {

    private final int[] indexMasks;

    /**
     * The time in nanoseconds when this index (and its cost) was calculated.
     */
    private final long evaluatedAt;

    /**
     * Creates a new instance of a regular query expression index.
     *
     * @param table
     *            the query expression table
     * @param querySQL
     *            the query SQL
     * @param originalParameters
     *            the original parameters
     * @param session
     *            the session
     * @param masks
     *            the masks
     */
    public RegularQueryExpressionIndex(QueryExpressionTable table, String querySQL,
            ArrayList<Parameter> originalParameters, SessionLocal session, int[] masks) {
        super(table, querySQL, originalParameters);
        indexMasks = masks;
        Query q = session.prepareQueryExpression(querySQL, table.getQueryScope());
        if (masks != null && q.allowGlobalConditions()) {
            q = addConditions(table, querySQL, originalParameters, session, masks, q);
        }
        q.preparePlan();
        query = q;
        evaluatedAt = table.getTopQuery() == null ? System.nanoTime() : 0L;
    }

    private Query addConditions(QueryExpressionTable table, String querySQL, ArrayList<Parameter> originalParameters,
            SessionLocal session, int[] masks, Query q) {
        int firstIndexParam = table.getParameterOffset(originalParameters);
        // the column index of each parameter
        // (for example: paramColumnIndex {0, 0} mean
        // param[0] is column 0, and param[1] is also column 0)
        IntArray paramColumnIndex = new IntArray();
        int indexColumnCount = 0;
        for (int i = 0; i < masks.length; i++) {
            int mask = masks[i];
            if (mask == 0) {
                continue;
            }
            indexColumnCount++;
            // the number of parameters depends on the mask;
            // for range queries it is 2: >= x AND <= y
            // but bitMask could also be 7 (=, and <=, and >=)
            int bitCount = Integer.bitCount(mask);
            for (int j = 0; j < bitCount; j++) {
                paramColumnIndex.add(i);
            }
        }
        int len = paramColumnIndex.size();
        ArrayList<Column> columnList = new ArrayList<>(len);
        for (int i = 0; i < len;) {
            int idx = paramColumnIndex.get(i);
            columnList.add(table.getColumn(idx));
            int mask = masks[idx];
            if ((mask & IndexCondition.EQUALITY) != 0) {
                Parameter param = new Parameter(firstIndexParam + i);
                q.addGlobalCondition(param, idx, Comparison.EQUAL_NULL_SAFE);
                i++;
            }
            if ((mask & IndexCondition.START) != 0) {
                Parameter param = new Parameter(firstIndexParam + i);
                q.addGlobalCondition(param, idx, Comparison.BIGGER_EQUAL);
                i++;
            }
            if ((mask & IndexCondition.END) != 0) {
                Parameter param = new Parameter(firstIndexParam + i);
                q.addGlobalCondition(param, idx, Comparison.SMALLER_EQUAL);
                i++;
            }
            if ((mask & IndexCondition.SPATIAL_INTERSECTS) != 0) {
                Parameter param = new Parameter(firstIndexParam + i);
                q.addGlobalCondition(param, idx, Comparison.SPATIAL_INTERSECTS);
                i++;
            }
        }
        columns = columnList.toArray(new Column[0]);

        // reconstruct the index columns from the masks
        this.indexColumns = new IndexColumn[indexColumnCount];
        this.columnIds = new int[indexColumnCount];
        for (int type = 0, indexColumnId = 0; type < 2; type++) {
            for (int i = 0; i < masks.length; i++) {
                int mask = masks[i];
                if (mask == 0) {
                    continue;
                }
                if (type == 0) {
                    if ((mask & IndexCondition.EQUALITY) == 0) {
                        // the first columns need to be equality conditions
                        continue;
                    }
                } else {
                    if ((mask & IndexCondition.EQUALITY) != 0) {
                        // after that only range conditions
                        continue;
                    }
                }
                Column column = table.getColumn(i);
                indexColumns[indexColumnId] = new IndexColumn(column);
                columnIds[indexColumnId] = column.getColumnId();
                indexColumnId++;
            }
        }
        String sql = q.getPlanSQL(DEFAULT_SQL_FLAGS);
        if (!sql.equals(querySQL)) {
            q = session.prepareQueryExpression(sql, table.getQueryScope());
        }
        return q;
    }

    @Override
    public boolean isExpired() {
        return table.getTopQuery() == null
                && System.nanoTime() - evaluatedAt > Constants.VIEW_COST_CACHE_MAX_AGE * 1_000_000L;
    }

    @Override
    public double getCost(SessionLocal session, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        return query.getCost();
    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last, boolean reverse) {
        return find(session, first, last, null);
    }

    @Override
    public Cursor findByGeometry(SessionLocal session, SearchRow first, SearchRow last, boolean reverse,
            SearchRow intersection) {
        assert !reverse;
        return find(session, first, last, intersection);
    }

    private Cursor find(SessionLocal session, SearchRow first, SearchRow last, SearchRow intersection) {
        ArrayList<Parameter> paramList = query.getParameters();
        if (originalParameters != null) {
            for (Parameter orig : originalParameters) {
                if (orig != null) {
                    int idx = orig.getIndex();
                    Value value = orig.getValue(session);
                    setParameter(paramList, idx, value);
                }
            }
        }
        int len;
        if (first != null) {
            len = first.getColumnCount();
        } else if (last != null) {
            len = last.getColumnCount();
        } else if (intersection != null) {
            len = intersection.getColumnCount();
        } else {
            len = 0;
        }
        int idx = table.getParameterOffset(originalParameters);
        for (int i = 0; i < len; i++) {
            int mask = indexMasks[i];
            if ((mask & IndexCondition.EQUALITY) != 0) {
                setParameter(paramList, idx++, first.getValue(i));
            }
            if ((mask & IndexCondition.START) != 0) {
                setParameter(paramList, idx++, first.getValue(i));
            }
            if ((mask & IndexCondition.END) != 0) {
                setParameter(paramList, idx++, last.getValue(i));
            }
            if ((mask & IndexCondition.SPATIAL_INTERSECTS) != 0) {
                setParameter(paramList, idx++, intersection.getValue(i));
            }
        }
        return new QueryExpressionCursor(this, query.query(0), first, last);
    }

    private static void setParameter(ArrayList<Parameter> paramList, int x, Value v) {
        if (x >= paramList.size()) {
            // the parameter may be optimized away as in
            // select * from (select null as x) where x=1;
            return;
        }
        Parameter param = paramList.get(x);
        param.setValue(v);
    }

}
