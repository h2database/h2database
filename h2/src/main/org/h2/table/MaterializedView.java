/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import java.util.HashSet;

import org.h2.command.query.AllColumnsForPlan;
import org.h2.command.query.Query;
import org.h2.engine.DbObject;
import org.h2.engine.SessionLocal;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.util.StringUtils;

/**
 * A materialized view.
 */
public class MaterializedView extends Table {

    private Table table;
    private String querySQL;
    private Query query;

    public MaterializedView(Schema schema, int id, String name, Table table, Query query, String querySQL) {
        super(schema, id, name, false, true);
        this.table = table;
        this.query = query;
        this.querySQL = querySQL;
    }

    public void replace(Table table, Query query, String querySQL) {
        this.table = table;
        this.query = query;
        this.querySQL = querySQL;
    }

    public Table getUnderlyingTable() {
        return table;
    }

    public Query getSelect() {
        return query;
    }

    @Override
    public final void close(SessionLocal session) {
        table.close(session);
    }

    @Override
    public final Index addIndex(SessionLocal session, String indexName, int indexId, IndexColumn[] cols,
            int uniqueColumnCount, IndexType indexType, boolean create, String indexComment) {
        return table.addIndex(session, indexName, indexId, cols, uniqueColumnCount, indexType, create, indexComment);
    }

    @Override
    public final boolean isView() {
        return true;
    }

    @Override
    public final PlanItem getBestPlanItem(SessionLocal session, int[] masks, TableFilter[] filters, int filter,
            SortOrder sortOrder, AllColumnsForPlan allColumnsSet) {
        return table.getBestPlanItem(session, masks, filters, filter, sortOrder, allColumnsSet);
    }

    @Override
    public boolean isQueryComparable() {
        return table.isQueryComparable();
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
        return table.getRowCount(session);
    }

    @Override
    public final boolean canGetRowCount(SessionLocal session) {
        return table.canGetRowCount(session);
    }

    @Override
    public final long getRowCountApproximation(SessionLocal session) {
        return table.getRowCountApproximation(session);
    }

    @Override
    public final boolean canReference() {
        return false;
    }

    @Override
    public final ArrayList<Index> getIndexes() {
        return table.getIndexes();
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
        return table.isDeterministic();
    }

    @Override
    public final void addDependencies(HashSet<DbObject> dependencies) {
        table.addDependencies(dependencies);
    }

    @Override
    public String getDropSQL() {
        return getSQL(new StringBuilder("DROP MATERIALIZED VIEW IF EXISTS "), DEFAULT_SQL_FLAGS).toString();
    }

    @Override
    public String getCreateSQLForCopy(Table table, String quotedName) {
        return getCreateSQL(false, true, quotedName);
    }

    @Override
    public String getCreateSQL() {
        return getCreateSQL(false, true);
    }

    /**
     * Generate "CREATE" SQL statement for the materialized view.
     *
     * @param orReplace
     *            if true, then include the OR REPLACE clause
     * @param force
     *            if true, then include the FORCE clause
     * @return the SQL statement
     */
    public String getCreateSQL(boolean orReplace, boolean force) {
        return getCreateSQL(orReplace, force, getSQL(DEFAULT_SQL_FLAGS));
    }

    private String getCreateSQL(boolean orReplace, boolean force, String quotedName) {
        StringBuilder builder = new StringBuilder("CREATE ");
        if (orReplace) {
            builder.append("OR REPLACE ");
        }
        if (force) {
            builder.append("FORCE ");
        }
        builder.append("MATERIALIZED VIEW ");
        builder.append(quotedName);
        if (comment != null) {
            builder.append(" COMMENT ");
            StringUtils.quoteStringSQL(builder, comment);
        }
        return builder.append(" AS\n").append(querySQL).toString();
    }

    @Override
    public boolean canDrop() {
        return true;
    }

    @Override
    public TableType getTableType() {
        return TableType.MATERIALIZED_VIEW;
    }

    @Override
    public void removeChildrenAndResources(SessionLocal session) {
        table.removeChildrenAndResources(session);
        database.removeMeta(session, getId());
        querySQL = null;
        invalidate();
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        if (isTemporary() && querySQL != null) {
            builder.append("(\n");
            return StringUtils.indent(builder, querySQL, 4, true).append(')');
        }
        return super.getSQL(builder, sqlFlags);
    }

    public String getQuerySQL() {
        return querySQL;
    }

    @Override
    public long getMaxDataModificationId() {
        return table.getMaxDataModificationId();
    }

}
