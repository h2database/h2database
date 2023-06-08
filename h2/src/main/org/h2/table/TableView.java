/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.h2.api.ErrorCode;
import org.h2.command.Prepared;
import org.h2.command.ddl.CreateTableData;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.command.query.Query;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.expression.Parameter;
import org.h2.index.Index;
import org.h2.index.QueryExpressionIndex;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
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
    private boolean allowRecursive;
    private DbException createException;
    private ResultInterface recursiveResult;
    private boolean isRecursiveQueryDetected;
    private boolean isTableExpression;

    public TableView(Schema schema, int id, String name, String querySQL,
            ArrayList<Parameter> params, Column[] columnTemplates, SessionLocal session,
            boolean allowRecursive, boolean literalsChecked, boolean isTableExpression, boolean isTemporary) {
        super(schema, id, name);
        setTemporary(isTemporary);
        init(querySQL, params, columnTemplates, session, allowRecursive, literalsChecked, isTableExpression);
    }

    /**
     * Try to replace the SQL statement of the view and re-compile this and all
     * dependent views.
     *
     * @param querySQL the SQL statement
     * @param newColumnTemplates the columns
     * @param session the session
     * @param recursive whether this is a recursive view
     * @param force if errors should be ignored
     * @param literalsChecked if literals have been checked
     */
    public void replace(String querySQL,  Column[] newColumnTemplates, SessionLocal session,
            boolean recursive, boolean force, boolean literalsChecked) {
        String oldQuerySQL = this.querySQL;
        Column[] oldColumnTemplates = this.columnTemplates;
        boolean oldRecursive = this.allowRecursive;
        init(querySQL, null, newColumnTemplates, session, recursive, literalsChecked, isTableExpression);
        DbException e = recompile(session, force, true);
        if (e != null) {
            init(oldQuerySQL, null, oldColumnTemplates, session, oldRecursive,
                    literalsChecked, isTableExpression);
            recompile(session, true, false);
            throw e;
        }
    }

    private synchronized void init(String querySQL, ArrayList<Parameter> params,
            Column[] columnTemplates, SessionLocal session, boolean allowRecursive, boolean literalsChecked,
            boolean isTableExpression) {
        this.querySQL = querySQL;
        this.columnTemplates = columnTemplates;
        this.allowRecursive = allowRecursive;
        this.isRecursiveQueryDetected = false;
        this.isTableExpression = isTableExpression;
        index = new QueryExpressionIndex(this, querySQL, params, allowRecursive);
        initColumnsAndTables(session, literalsChecked);
    }

    private Query compileViewQuery(SessionLocal session, String sql, boolean literalsChecked) {
        Prepared p;
        session.setParsingCreateView(true);
        try {
            p = session.prepare(sql, false, literalsChecked);
        } finally {
            session.setParsingCreateView(false);
        }
        if (!(p instanceof Query)) {
            throw DbException.getSyntaxError(sql, 0);
        }
        Query q = (Query) p;
        // only potentially recursive cte queries need to be non-lazy
        if (isTableExpression && allowRecursive) {
            q.setNeverLazy(true);
        }
        return q;
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
    public synchronized DbException recompile(SessionLocal session, boolean force,
            boolean clearIndexCache) {
        try {
            compileViewQuery(session, querySQL, false);
        } catch (DbException e) {
            if (!force) {
                return e;
            }
        }
        ArrayList<TableView> dependentViews = new ArrayList<>(getDependentViews());
        initColumnsAndTables(session, false);
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

    private void initColumnsAndTables(SessionLocal session, boolean literalsChecked) {
        Column[] cols;
        removeCurrentViewFromOtherTables();
        setTableExpression(isTableExpression);
        try {
            Query compiledQuery = compileViewQuery(session, querySQL, literalsChecked);
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
            // If it can not be compiled - it could also be a recursive common
            // table expression query.
            if (isRecursiveQueryExceptionDetected(createException)) {
                this.isRecursiveQueryDetected = true;
            }
            tables = Utils.newSmallArrayList();
            cols = new Column[0];
            if (allowRecursive && columnTemplates != null) {
                cols = new Column[columnTemplates.length];
                for (int i = 0; i < columnTemplates.length; i++) {
                    cols[i] = columnTemplates[i].getClone();
                }
                index.setRecursive(true);
                createException = null;
            }
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
        if (isTableExpression) {
            builder.append("TABLE_EXPRESSION ");
        }
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
        index = null;
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

    public boolean isRecursive() {
        return allowRecursive;
    }

    @Override
    public boolean isDeterministic() {
        if (allowRecursive || viewQuery == null) {
            return false;
        }
        return super.isDeterministic();
    }

    public void setRecursiveResult(ResultInterface value) {
        if (recursiveResult != null) {
            recursiveResult.close();
        }
        this.recursiveResult = value;
    }

    public ResultInterface getRecursiveResult() {
        return recursiveResult;
    }

    /**
     * Was query recursion detected during compiling.
     *
     * @return true if yes
     */
    public boolean isRecursiveQueryDetected() {
        return isRecursiveQueryDetected;
    }

    /**
     * Does exception indicate query recursion?
     */
    private boolean isRecursiveQueryExceptionDetected(DbException exception) {
        if (exception == null) {
            return false;
        }
        int errorCode = exception.getErrorCode();
        if (errorCode != ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1 &&
                errorCode != ErrorCode.TABLE_OR_VIEW_NOT_FOUND_DATABASE_EMPTY_1 &&
                errorCode != ErrorCode.TABLE_OR_VIEW_NOT_FOUND_WITH_CANDIDATES_2
        ) {
            return false;
        }
        return exception.getMessage().contains("\"" + this.getName() + "\"");
    }

    public List<Table> getTables() {
        return tables;
    }

    /**
     * Create a view.
     *
     * @param schema the schema
     * @param id the view id
     * @param name the view name
     * @param querySQL the query
     * @param parameters the parameters
     * @param columnTemplates the columns
     * @param session the session
     * @param literalsChecked whether literals in the query are checked
     * @param isTableExpression if this is a table expression
     * @param isTemporary whether the view is persisted
     * @param db the database
     * @return the view
     */
    public static TableView createTableViewMaybeRecursive(Schema schema, int id, String name, String querySQL,
            ArrayList<Parameter> parameters, Column[] columnTemplates, SessionLocal session,
            boolean literalsChecked, boolean isTableExpression, boolean isTemporary, Database db) {


        Table recursiveTable = createShadowTableForRecursiveTableExpression(isTemporary, session, name,
                schema, Arrays.asList(columnTemplates), db);

        List<Column> columnTemplateList;
        String[] querySQLOutput = new String[1];
        ArrayList<String> columnNames = new ArrayList<>();
        for (Column columnTemplate: columnTemplates) {
            columnNames.add(columnTemplate.getName());
        }

        try {
            Prepared withQuery = session.prepare(querySQL, false, false);
            if (!isTemporary) {
                withQuery.setSession(session);
            }
            columnTemplateList = createQueryColumnTemplateList(columnNames.toArray(new String[1]),
                    (Query) withQuery, querySQLOutput);

        } finally {
            destroyShadowTableForRecursiveExpression(isTemporary, session, recursiveTable);
        }

        // build with recursion turned on
        TableView view = new TableView(schema, id, name, querySQL,
                parameters, columnTemplateList.toArray(columnTemplates), session,
                true/* try recursive */, literalsChecked, isTableExpression, isTemporary);

        // is recursion really detected ? if not - recreate it without recursion flag
        // and no recursive index
        if (!view.isRecursiveQueryDetected()) {
            if (!isTemporary) {
                db.addSchemaObject(session, view);
                view.lock(session, Table.EXCLUSIVE_LOCK);
                session.getDatabase().removeSchemaObject(session, view);

                // during database startup - this method does not normally get called - and it
                // needs to be to correctly un-register the table which the table expression
                // uses...
                view.removeChildrenAndResources(session);
            } else {
                session.removeLocalTempTable(view);
            }
            view = new TableView(schema, id, name, querySQL, parameters,
                    columnTemplates, session,
                    false/* detected not recursive */, literalsChecked, isTableExpression, isTemporary);
        }

        return view;
    }

    /**
     * Create a table for a recursive query.
     *
     * @param isTemporary whether the table is persisted
     * @param targetSession the session
     * @param cteViewName the name
     * @param schema the schema
     * @param columns the columns
     * @param db the database
     * @return the table
     */
    public static Table createShadowTableForRecursiveTableExpression(boolean isTemporary, SessionLocal targetSession,
            String cteViewName, Schema schema, List<Column> columns, Database db) {

        // create table data object
        CreateTableData recursiveTableData = new CreateTableData();
        recursiveTableData.id = db.allocateObjectId();
        recursiveTableData.columns = new ArrayList<>(columns);
        recursiveTableData.tableName = cteViewName;
        recursiveTableData.temporary = isTemporary;
        recursiveTableData.persistData = true;
        recursiveTableData.persistIndexes = !isTemporary;
        recursiveTableData.session = targetSession;

        // this gets a meta table lock that is not released
        Table recursiveTable = schema.createTable(recursiveTableData);

        if (!isTemporary) {
            // this unlock is to prevent lock leak from schema.createTable()
            db.unlockMeta(targetSession);
            synchronized (targetSession) {
                db.addSchemaObject(targetSession, recursiveTable);
            }
        } else {
            targetSession.addLocalTempTable(recursiveTable);
        }
        return recursiveTable;
    }

    /**
     * Remove a table for a recursive query.
     *
     * @param isTemporary whether the table is persisted
     * @param targetSession the session
     * @param recursiveTable the table
     */
    public static void destroyShadowTableForRecursiveExpression(boolean isTemporary, SessionLocal targetSession,
            Table recursiveTable) {
        if (recursiveTable != null) {
            if (!isTemporary) {
                recursiveTable.lock(targetSession, Table.EXCLUSIVE_LOCK);
                targetSession.getDatabase().removeSchemaObject(targetSession, recursiveTable);

            } else {
                targetSession.removeLocalTempTable(recursiveTable);
            }

            // both removeSchemaObject and removeLocalTempTable hold meta locks - release them here
            targetSession.getDatabase().unlockMeta(targetSession);
        }
    }
}
