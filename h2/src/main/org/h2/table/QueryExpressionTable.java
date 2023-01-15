/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.h2.command.query.AllColumnsForPlan;
import org.h2.command.query.Query;
import org.h2.engine.DbObject;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.QueryExpressionIndex;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.util.StringUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * A derived table or view.
 */
public abstract class QueryExpressionTable extends Table {

    /**
     * The key of the index cache for views.
     */
    static final class CacheKey {

        private final int[] masks;

        private final QueryExpressionTable queryExpressionTable;

        CacheKey(int[] masks, QueryExpressionTable queryExpressionTable) {
            this.masks = masks;
            this.queryExpressionTable = queryExpressionTable;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(masks);
            result = prime * result + queryExpressionTable.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            CacheKey other = (CacheKey) obj;
            if (queryExpressionTable != other.queryExpressionTable) {
                return false;
            }
            return Arrays.equals(masks, other.masks);
        }
    }

    private static final long ROW_COUNT_APPROXIMATION = 100;

    /**
     * Creates a list of column templates from a query (usually from WITH query,
     * but could be any query)
     *
     * @param cols
     *            - an optional list of column names (can be specified by WITH
     *            clause overriding usual select names)
     * @param theQuery
     *            - the query object we want the column list for
     * @param querySQLOutput
     *            - array of length 1 to receive extra 'output' field in
     *            addition to return value - containing the SQL query of the
     *            Query object
     * @return a list of column object returned by withQuery
     */
    public static List<Column> createQueryColumnTemplateList(String[] cols, Query theQuery, String[] querySQLOutput) {
        ArrayList<Column> columnTemplateList = new ArrayList<>();
        theQuery.prepare();
        // String array of length 1 is to receive extra 'output' field in
        // addition to
        // return value
        querySQLOutput[0] = StringUtils.cache(theQuery.getPlanSQL(ADD_PLAN_INFORMATION));
        SessionLocal session = theQuery.getSession();
        ArrayList<Expression> withExpressions = theQuery.getExpressions();
        for (int i = 0; i < withExpressions.size(); ++i) {
            Expression columnExp = withExpressions.get(i);
            // use the passed in column name if supplied, otherwise use alias
            // (if found) otherwise use column name derived from column
            // expression
            String columnName = cols != null && cols.length > i ? cols[i] : columnExp.getColumnNameForView(session, i);
            columnTemplateList.add(new Column(columnName, columnExp.getType()));
        }
        return columnTemplateList;
    }

    static int getMaxParameterIndex(ArrayList<Parameter> parameters) {
        int result = -1;
        for (Parameter p : parameters) {
            if (p != null) {
                result = Math.max(result, p.getIndex());
            }
        }
        return result;
    }

    Query viewQuery;

    QueryExpressionIndex index;

    ArrayList<Table> tables;

    private long lastModificationCheck;

    private long maxDataModificationId;

    QueryExpressionTable(Schema schema, int id, String name) {
        super(schema, id, name, false, true);
    }

    Column[] initColumns(SessionLocal session, Column[] columnTemplates, Query query, boolean isDerivedTable) {
        ArrayList<Expression> expressions = query.getExpressions();
        final int count = query.getColumnCount();
        ArrayList<Column> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Expression expr = expressions.get(i);
            String name = null;
            TypeInfo type = TypeInfo.TYPE_UNKNOWN;
            if (columnTemplates != null && columnTemplates.length > i) {
                name = columnTemplates[i].getName();
                type = columnTemplates[i].getType();
            }
            if (name == null) {
                name = isDerivedTable ? expr.getAlias(session, i) : expr.getColumnNameForView(session, i);
            }
            if (type.getValueType() == Value.UNKNOWN) {
                type = expr.getType();
            }
            list.add(new Column(name, type, this, i));
        }
        return list.toArray(new Column[0]);
    }

    public final Query getQuery() {
        return viewQuery;
    }

    public abstract Query getTopQuery();

    @Override
    public final void close(SessionLocal session) {
        // nothing to do
    }

    @Override
    public final Index addIndex(SessionLocal session, String indexName, int indexId, IndexColumn[] cols,
            int uniqueColumnCount, IndexType indexType, boolean create, String indexComment) {
        throw DbException.getUnsupportedException(getClass().getSimpleName() + ".addIndex");
    }

    @Override
    public final boolean isView() {
        return true;
    }

    @Override
    public final PlanItem getBestPlanItem(SessionLocal session, int[] masks, TableFilter[] filters, int filter,
            SortOrder sortOrder, AllColumnsForPlan allColumnsSet) {
        final CacheKey cacheKey = new CacheKey(masks, this);
        Map<Object, QueryExpressionIndex> indexCache = session.getViewIndexCache(getTableType() == null);
        QueryExpressionIndex i = indexCache.get(cacheKey);
        if (i == null || i.isExpired()) {
            i = new QueryExpressionIndex(this, index, session, masks, filters, filter, sortOrder);
            indexCache.put(cacheKey, i);
        }
        PlanItem item = new PlanItem();
        item.cost = i.getCost(session, masks, filters, filter, sortOrder, allColumnsSet);
        item.setIndex(i);
        return item;
    }

    @Override
    public boolean isQueryComparable() {
        for (Table t : tables) {
            if (!t.isQueryComparable()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public final boolean isInsertable() {
        return false;
    }

    @Override
    public final void removeRow(SessionLocal session, Row row) {
        throw DbException.getUnsupportedException(getClass().getSimpleName() + ".removeRow");
    }

    @Override
    public final void addRow(SessionLocal session, Row row) {
        throw DbException.getUnsupportedException(getClass().getSimpleName() + ".addRow");
    }

    @Override
    public final void checkSupportAlter() {
        throw DbException.getUnsupportedException(getClass().getSimpleName() + ".checkSupportAlter");
    }

    @Override
    public final long truncate(SessionLocal session) {
        throw DbException.getUnsupportedException(getClass().getSimpleName() + ".truncate");
    }

    @Override
    public final long getRowCount(SessionLocal session) {
        throw DbException.getInternalError(toString());
    }

    @Override
    public final boolean canGetRowCount(SessionLocal session) {
        // TODO could get the row count, but not that easy
        return false;
    }

    @Override
    public final long getRowCountApproximation(SessionLocal session) {
        return ROW_COUNT_APPROXIMATION;
    }

    /**
     * Get the index of the first parameter.
     *
     * @param additionalParameters
     *            additional parameters
     * @return the index of the first parameter
     */
    public final int getParameterOffset(ArrayList<Parameter> additionalParameters) {
        Query topQuery = getTopQuery();
        int result = topQuery == null ? -1 : getMaxParameterIndex(topQuery.getParameters());
        if (additionalParameters != null) {
            result = Math.max(result, getMaxParameterIndex(additionalParameters));
        }
        return result + 1;
    }

    @Override
    public final boolean canReference() {
        return false;
    }

    @Override
    public final ArrayList<Index> getIndexes() {
        return null;
    }

    @Override
    public long getMaxDataModificationId() {
        // if nothing was modified in the database since the last check, and the
        // last is known, then we don't need to check again
        // this speeds up nested views
        long dbMod = database.getModificationDataId();
        if (dbMod > lastModificationCheck && maxDataModificationId <= dbMod) {
            maxDataModificationId = viewQuery.getMaxDataModificationId();
            lastModificationCheck = dbMod;
        }
        return maxDataModificationId;
    }

    @Override
    public final Index getScanIndex(SessionLocal session) {
        return getBestPlanItem(session, null, null, -1, null, null).getIndex();
    }

    @Override
    public Index getScanIndex(SessionLocal session, int[] masks, TableFilter[] filters, int filter, //
            SortOrder sortOrder, AllColumnsForPlan allColumnsSet) {
        return getBestPlanItem(session, masks, filters, filter, sortOrder, allColumnsSet).getIndex();
    }

    @Override
    public boolean isDeterministic() {
        return viewQuery.isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR);
    }

    @Override
    public final void addDependencies(HashSet<DbObject> dependencies) {
        super.addDependencies(dependencies);
        if (tables != null) {
            for (Table t : tables) {
                if (TableType.VIEW != t.getTableType()) {
                    t.addDependencies(dependencies);
                }
            }
        }
    }

}
