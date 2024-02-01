/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import java.util.List;

import org.h2.api.ErrorCode;
import org.h2.command.Prepared;
import org.h2.command.QueryScope;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.command.query.Query;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.index.Index;
import org.h2.index.QueryExpressionIndex;
import org.h2.index.RegularQueryExpressionIndex;
import org.h2.message.DbException;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.util.StringUtils;
import org.h2.util.Utils;

/**
 * A view is a virtual table that is defined by a query.
 * @author Thomas Mueller
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public final class TableView extends QueryExpressionTable {

    private String querySQL;
    private Column[] columnTemplates;
    private DbException createException;

    public TableView(Schema schema, int id, String name, String querySQL, Column[] columnTemplates,
            SessionLocal session) {
        super(schema, id, name);
        init(querySQL, columnTemplates, session);
    }

    @Override
    protected QueryExpressionIndex createIndex(SessionLocal session, int[] masks) {
        return new RegularQueryExpressionIndex(this, querySQL, null, session, masks);
    }

    /**
     * Try to replace the SQL statement of the view and re-compile this and all
     * dependent views.
     *
     * @param querySQL the SQL statement
     * @param newColumnTemplates the columns
     * @param session the session
     * @param force if errors should be ignored
     */
    public void replace(String querySQL,  Column[] newColumnTemplates, SessionLocal session, boolean force) {
        String oldQuerySQL = this.querySQL;
        Column[] oldColumnTemplates = this.columnTemplates;
        init(querySQL, newColumnTemplates, session);
        DbException e = recompile(session, force, true);
        if (e != null) {
            init(oldQuerySQL, oldColumnTemplates, session);
            recompile(session, true, false);
            throw e;
        }
    }

    private synchronized void init(String querySQL, Column[] columnTemplates, SessionLocal session) {
        this.querySQL = querySQL;
        this.columnTemplates = columnTemplates;
        initColumnsAndTables(session);
    }

    private static Query compileViewQuery(SessionLocal session, String sql) {
        Prepared p;
        session.setParsingCreateView(true);
        try {
            p = session.prepare(sql, false, false, null);
        } finally {
            session.setParsingCreateView(false);
        }
        if (!(p instanceof Query)) {
            throw DbException.getSyntaxError(sql, 0);
        }
        return (Query) p;
    }

    /**
     * Re-compile the view query and all views that depend on this object.
     *
     * @param session the session
     * @param force if exceptions should be ignored
     * @param clearIndexCache if we need to clear view index cache
     * @return the exception if re-compiling this or any dependent view failed
     *         (only when force is disabled)
     */
    public synchronized DbException recompile(SessionLocal session, boolean force, boolean clearIndexCache) {
        try {
            compileViewQuery(session, querySQL);
        } catch (DbException e) {
            if (!force) {
                return e;
            }
        }
        ArrayList<TableView> dependentViews = new ArrayList<>(getDependentViews());
        initColumnsAndTables(session);
        for (TableView v : dependentViews) {
            DbException e = v.recompile(session, force, false);
            if (e != null && !force) {
                return e;
            }
        }
        if (clearIndexCache) {
            clearIndexCaches(database);
        }
        return force ? null : createException;
    }

    private void initColumnsAndTables(SessionLocal session) {
        Column[] cols;
        removeCurrentViewFromOtherTables();
        try {
            Query compiledQuery = compileViewQuery(session, querySQL);
            this.querySQL = compiledQuery.getPlanSQL(DEFAULT_SQL_FLAGS);
            tables = new ArrayList<>(compiledQuery.getTables());
            cols = initColumns(session, columnTemplates, compiledQuery, false);
            createException = null;
            viewQuery = compiledQuery;
        } catch (DbException e) {
            if (e.getErrorCode() == ErrorCode.COLUMN_ALIAS_IS_NOT_SPECIFIED_1) {
                throw e;
            }
            e.addSQL(getCreateSQL());
            createException = e;
            // If it can't be compiled, then it's a 'zero column table'
            // this avoids problems when creating the view when opening the
            // database.
            tables = Utils.newSmallArrayList();
            cols = new Column[0];
        }
        setColumns(cols);
        if (getId() != 0) {
            addDependentViewToTables();
        }
    }

    /**
     * Check if this view is currently invalid.
     *
     * @return true if it is
     */
    public boolean isInvalid() {
        return createException != null;
    }

    @Override
    public Query getTopQuery() {
        return null;
    }

    @Override
    public String getDropSQL() {
        return getSQL(new StringBuilder("DROP VIEW IF EXISTS "), DEFAULT_SQL_FLAGS).append(" CASCADE").toString();
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
     * Generate "CREATE" SQL statement for the view.
     *
     * @param orReplace if true, then include the OR REPLACE clause
     * @param force if true, then include the FORCE clause
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
        builder.append("VIEW ");
        builder.append(quotedName);
        if (comment != null) {
            builder.append(" COMMENT ");
            StringUtils.quoteStringSQL(builder, comment);
        }
        if (columns != null && columns.length > 0) {
            builder.append('(');
            Column.writeColumns(builder, columns, DEFAULT_SQL_FLAGS);
            builder.append(')');
        } else if (columnTemplates != null) {
            builder.append('(');
            Column.writeColumns(builder, columnTemplates, DEFAULT_SQL_FLAGS);
            builder.append(')');
        }
        return builder.append(" AS\n").append(querySQL).toString();
    }

    @Override
    public boolean canDrop() {
        return true;
    }

    @Override
    public TableType getTableType() {
        return TableType.VIEW;
    }

    @Override
    public void removeChildrenAndResources(SessionLocal session) {
        removeCurrentViewFromOtherTables();
        super.removeChildrenAndResources(session);
        querySQL = null;
        clearIndexCaches(database);
        invalidate();
    }

    /**
     * Clear the cached indexes for all sessions.
     *
     * @param database the database
     */
    public static void clearIndexCaches(Database database) {
        for (SessionLocal s : database.getSessions(true)) {
            s.clearViewIndexCache();
        }
    }

    public String getQuerySQL() {
        return querySQL;
    }

    @Override
    public QueryScope getQueryScope() {
        return null;
    }

    @Override
    public Index getScanIndex(SessionLocal session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        if (createException != null) {
            String msg = createException.getMessage();
            throw DbException.get(ErrorCode.VIEW_IS_INVALID_2, createException, getTraceSQL(), msg);
        }
        return super.getScanIndex(session, masks, filters, filter, sortOrder, allColumnsSet);
    }

    @Override
    public long getMaxDataModificationId() {
        if (createException != null || viewQuery == null) {
            return Long.MAX_VALUE;
        }
        return super.getMaxDataModificationId();
    }

    private void removeCurrentViewFromOtherTables() {
        if (tables != null) {
            for (Table t : tables) {
                t.removeDependentView(this);
            }
            tables.clear();
        }
    }

    private void addDependentViewToTables() {
        for (Table t : tables) {
            t.addDependentView(this);
        }
    }

    @Override
    public boolean isDeterministic() {
        if (viewQuery == null) {
            return false;
        }
        return super.isDeterministic();
    }

    public List<Table> getTables() {
        return tables;
    }

}
