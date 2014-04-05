/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import java.util.HashSet;

import org.h2.api.ErrorCode;
import org.h2.command.Prepared;
import org.h2.command.dml.Query;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.expression.Alias;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.ViewIndex;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.util.IntArray;
import org.h2.util.New;
import org.h2.util.SmallLRUCache;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.util.SynchronizedVerifier;
import org.h2.util.Utils;
import org.h2.value.Value;

/**
 * A view is a virtual table that is defined by a query.
 * @author Thomas Mueller
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class TableView extends Table {

    private static final long ROW_COUNT_APPROXIMATION = 100;

    private String querySQL;
    private ArrayList<Table> tables;
    private String[] columnNames;
    private Query viewQuery;
    private ViewIndex index;
    private boolean recursive;
    private DbException createException;
    private final SmallLRUCache<IntArray, ViewIndex> indexCache =
            SmallLRUCache.newInstance(Constants.VIEW_INDEX_CACHE_SIZE);
    private long lastModificationCheck;
    private long maxDataModificationId;
    private User owner;
    private Query topQuery;
    private LocalResult recursiveResult;
    private boolean tableExpression;

    public TableView(Schema schema, int id, String name, String querySQL,
            ArrayList<Parameter> params, String[] columnNames, Session session,
            boolean recursive) {
        super(schema, id, name, false, true);
        init(querySQL, params, columnNames, session, recursive);
    }

    /**
     * Try to replace the SQL statement of the view and re-compile this and all
     * dependent views.
     *
     * @param querySQL the SQL statement
     * @param columnNames the column names
     * @param session the session
     * @param recursive whether this is a recursive view
     * @param force if errors should be ignored
     */
    public void replace(String querySQL, String[] columnNames, Session session,
            boolean recursive, boolean force) {
        String oldQuerySQL = this.querySQL;
        String[] oldColumnNames = this.columnNames;
        boolean oldRecursive = this.recursive;
        init(querySQL, null, columnNames, session, recursive);
        DbException e = recompile(session, force);
        if (e != null) {
            init(oldQuerySQL, null, oldColumnNames, session, oldRecursive);
            recompile(session, true);
            throw e;
        }
    }

    private synchronized void init(String querySQL, ArrayList<Parameter> params,
            String[] columnNames, Session session, boolean recursive) {
        this.querySQL = querySQL;
        this.columnNames = columnNames;
        this.recursive = recursive;
        index = new ViewIndex(this, querySQL, params, recursive);
        SynchronizedVerifier.check(indexCache);
        indexCache.clear();
        initColumnsAndTables(session);
    }

    private static Query compileViewQuery(Session session, String sql) {
        Prepared p = session.prepare(sql);
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
     * @return the exception if re-compiling this or any dependent view failed
     *         (only when force is disabled)
     */
    public synchronized DbException recompile(Session session, boolean force) {
        try {
            compileViewQuery(session, querySQL);
        } catch (DbException e) {
            if (!force) {
                return e;
            }
        }
        ArrayList<TableView> views = getViews();
        if (views != null) {
            views = New.arrayList(views);
        }
        SynchronizedVerifier.check(indexCache);
        indexCache.clear();
        initColumnsAndTables(session);
        if (views != null) {
            for (TableView v : views) {
                DbException e = v.recompile(session, force);
                if (e != null && !force) {
                    return e;
                }
            }
        }
        return force ? null : createException;
    }

    private void initColumnsAndTables(Session session) {
        Column[] cols;
        removeViewFromTables();
        try {
            Query query = compileViewQuery(session, querySQL);
            this.querySQL = query.getPlanSQL();
            tables = New.arrayList(query.getTables());
            ArrayList<Expression> expressions = query.getExpressions();
            ArrayList<Column> list = New.arrayList();
            for (int i = 0, count = query.getColumnCount(); i < count; i++) {
                Expression expr = expressions.get(i);
                String name = null;
                if (columnNames != null && columnNames.length > i) {
                    name = columnNames[i];
                }
                if (name == null) {
                    name = expr.getAlias();
                }
                int type = expr.getType();
                long precision = expr.getPrecision();
                int scale = expr.getScale();
                int displaySize = expr.getDisplaySize();
                Column col = new Column(name, type, precision, scale, displaySize);
                col.setTable(this, i);
                // Fetch check constraint from view column source
                ExpressionColumn fromColumn = null;
                if (expr instanceof ExpressionColumn) {
                    fromColumn = (ExpressionColumn) expr;
                } else if (expr instanceof Alias) {
                    Expression aliasExpr = expr.getNonAliasExpression();
                    if (aliasExpr instanceof ExpressionColumn) {
                        fromColumn = (ExpressionColumn) aliasExpr;
                    }
                }
                if (fromColumn != null) {
                    Expression checkExpression = fromColumn.getColumn()
                            .getCheckConstraint(session, name);
                    if (checkExpression != null) {
                        col.addCheckConstraint(session, checkExpression);
                    }
                }
                list.add(col);
            }
            cols = new Column[list.size()];
            list.toArray(cols);
            createException = null;
            viewQuery = query;
        } catch (DbException e) {
            e.addSQL(getCreateSQL());
            createException = e;
            // if it can't be compiled, then it's a 'zero column table'
            // this avoids problems when creating the view when opening the
            // database
            tables = New.arrayList();
            cols = new Column[0];
            if (recursive && columnNames != null) {
                cols = new Column[columnNames.length];
                for (int i = 0; i < columnNames.length; i++) {
                    cols[i] = new Column(columnNames[i], Value.STRING);
                }
                index.setRecursive(true);
                createException = null;
            }
        }
        setColumns(cols);
        if (getId() != 0) {
            addViewToTables();
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
    public synchronized PlanItem getBestPlanItem(Session session, int[] masks,
            TableFilter filter, SortOrder sortOrder) {
        PlanItem item = new PlanItem();
        item.cost = index.getCost(session, masks, filter, sortOrder);
        IntArray masksArray = new IntArray(masks == null ?
                Utils.EMPTY_INT_ARRAY : masks);
        SynchronizedVerifier.check(indexCache);
        ViewIndex i2 = indexCache.get(masksArray);
        if (i2 == null || i2.getSession() != session) {
            i2 = new ViewIndex(this, index, session, masks);
            indexCache.put(masksArray, i2);
        }
        item.setIndex(i2);
        return item;
    }

    @Override
    public String getDropSQL() {
        return "DROP VIEW IF EXISTS " + getSQL() + " CASCADE";
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
        return getCreateSQL(orReplace, force, getSQL());
    }

    private String getCreateSQL(boolean orReplace, boolean force,
            String quotedName) {
        StatementBuilder buff = new StatementBuilder("CREATE ");
        if (orReplace) {
            buff.append("OR REPLACE ");
        }
        if (force) {
            buff.append("FORCE ");
        }
        buff.append("VIEW ");
        buff.append(quotedName);
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        if (columns != null && columns.length > 0) {
            buff.append('(');
            for (Column c : columns) {
                buff.appendExceptFirst(", ");
                buff.append(c.getSQL());
            }
            buff.append(')');
        } else if (columnNames != null) {
            buff.append('(');
            for (String n : columnNames) {
                buff.appendExceptFirst(", ");
                buff.append(n);
            }
            buff.append(')');
        }
        return buff.append(" AS\n").append(querySQL).toString();
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public void lock(Session session, boolean exclusive, boolean force) {
        // exclusive lock means: the view will be dropped
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    @Override
    public void unlock(Session s) {
        // nothing to do
    }

    @Override
    public boolean isLockedExclusively() {
        return false;
    }

    @Override
    public Index addIndex(Session session, String indexName, int indexId,
            IndexColumn[] cols, IndexType indexType, boolean create,
            String indexComment) {
        throw DbException.getUnsupportedException("VIEW");
    }

    @Override
    public void removeRow(Session session, Row row) {
        throw DbException.getUnsupportedException("VIEW");
    }

    @Override
    public void addRow(Session session, Row row) {
        throw DbException.getUnsupportedException("VIEW");
    }

    @Override
    public void checkSupportAlter() {
        throw DbException.getUnsupportedException("VIEW");
    }

    @Override
    public void truncate(Session session) {
        throw DbException.getUnsupportedException("VIEW");
    }

    @Override
    public long getRowCount(Session session) {
        throw DbException.throwInternalError();
    }

    @Override
    public boolean canGetRowCount() {
        // TODO view: could get the row count, but not that easy
        return false;
    }

    @Override
    public boolean canDrop() {
        return true;
    }

    @Override
    public String getTableType() {
        return Table.VIEW;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        removeViewFromTables();
        super.removeChildrenAndResources(session);
        database.removeMeta(session, getId());
        querySQL = null;
        index = null;
        invalidate();
    }

    @Override
    public String getSQL() {
        if (isTemporary()) {
            return "(\n" + StringUtils.indent(querySQL) + ")";
        }
        return super.getSQL();
    }

    public String getQuery() {
        return querySQL;
    }

    @Override
    public Index getScanIndex(Session session) {
        if (createException != null) {
            String msg = createException.getMessage();
            throw DbException.get(ErrorCode.VIEW_IS_INVALID_2,
                    createException, getSQL(), msg);
        }
        PlanItem item = getBestPlanItem(session, null, null, null);
        return item.getIndex();
    }

    @Override
    public boolean canReference() {
        return false;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return null;
    }

    @Override
    public long getMaxDataModificationId() {
        if (createException != null) {
            return Long.MAX_VALUE;
        }
        if (viewQuery == null) {
            return Long.MAX_VALUE;
        }
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
    public Index getUniqueIndex() {
        return null;
    }

    private void removeViewFromTables() {
        if (tables != null) {
            for (Table t : tables) {
                t.removeView(this);
            }
            tables.clear();
        }
    }

    private void addViewToTables() {
        for (Table t : tables) {
            t.addView(this);
        }
    }

    private void setOwner(User owner) {
        this.owner = owner;
    }

    public User getOwner() {
        return owner;
    }

    /**
     * Create a temporary view out of the given query.
     *
     * @param session the session
     * @param owner the owner of the query
     * @param name the view name
     * @param query the query
     * @param topQuery the top level query
     * @return the view table
     */
    public static TableView createTempView(Session session, User owner,
            String name, Query query, Query topQuery) {
        Schema mainSchema = session.getDatabase().getSchema(Constants.SCHEMA_MAIN);
        String querySQL = query.getPlanSQL();
        TableView v = new TableView(mainSchema, 0, name,
                querySQL, query.getParameters(), null, session,
                false);
        if (v.createException != null) {
            throw v.createException;
        }
        v.setTopQuery(topQuery);
        v.setOwner(owner);
        v.setTemporary(true);
        return v;
    }

    private void setTopQuery(Query topQuery) {
        this.topQuery = topQuery;
    }

    @Override
    public long getRowCountApproximation() {
        return ROW_COUNT_APPROXIMATION;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    public int getParameterOffset() {
        return topQuery == null ? 0 : topQuery.getParameters().size();
    }

    @Override
    public boolean isDeterministic() {
        if (recursive || viewQuery == null) {
            return false;
        }
        return viewQuery.isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR);
    }

    public void setRecursiveResult(LocalResult value) {
        if (recursiveResult != null) {
            recursiveResult.close();
        }
        this.recursiveResult = value;
    }

    public ResultInterface getRecursiveResult() {
        return recursiveResult;
    }

    public void setTableExpression(boolean tableExpression) {
        this.tableExpression = tableExpression;
    }

    public boolean isTableExpression() {
        return tableExpression;
    }

    @Override
    public void addDependencies(HashSet<DbObject> dependencies) {
        super.addDependencies(dependencies);
        if (tables != null) {
            for (Table t : tables) {
                if (!Table.VIEW.equals(t.getTableType())) {
                    t.addDependencies(dependencies);
                }
            }
        }
    }

}
