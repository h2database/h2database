/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.SQLException;
import org.h2.command.Prepared;
import org.h2.command.dml.Query;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.ViewIndex;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.util.IntArray;
import org.h2.util.MemoryUtils;
import org.h2.util.ObjectArray;
import org.h2.util.SmallLRUCache;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.value.Value;

/**
 * A view is a virtual table that is defined by a query.
 */
public class TableView extends Table {

    private static final long ROW_COUNT_APPROXIMATION = 100;

    private String querySQL;
    private ObjectArray<Table> tables;
    private final String[] columnNames;
    private Query viewQuery;
    private ViewIndex index;
    private boolean recursive;
    private SQLException createException;
    private SmallLRUCache<IntArray, ViewIndex> indexCache = SmallLRUCache.newInstance(Constants.VIEW_INDEX_CACHE_SIZE);
    private long lastModificationCheck;
    private long maxDataModificationId;
    private User owner;
    private Query topQuery;

    public TableView(Schema schema, int id, String name, String querySQL, ObjectArray<Parameter> params, String[] columnNames,
            Session session, boolean recursive) throws SQLException {
        super(schema, id, name, false, true);
        this.querySQL = querySQL;
        this.columnNames = columnNames;
        this.recursive = recursive;
        index = new ViewIndex(this, querySQL, params, recursive);
        initColumnsAndTables(session);
    }

    /**
     * Re-compile the query, updating the SQL statement.
     *
     * @param session the session
     * @return the query
     */
    public Query recompileQuery(Session session) throws SQLException {
        Prepared p = session.prepare(querySQL);
        if (!(p instanceof Query)) {
            throw Message.getSyntaxError(querySQL, 0);
        }
        Query query = (Query) p;
        querySQL = query.getPlanSQL();
        return query;
    }

    private void initColumnsAndTables(Session session) throws SQLException {
        Column[] cols;
        removeViewFromTables();
        try {
            Query query = recompileQuery(session);
            tables = ObjectArray.newInstance(query.getTables());
            ObjectArray<Expression> expressions = query.getExpressions();
            ObjectArray<Column> list = ObjectArray.newInstance();
            for (int i = 0; i < query.getColumnCount(); i++) {
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
                list.add(col);
            }
            cols = new Column[list.size()];
            list.toArray(cols);
            createException = null;
            viewQuery = query;
        } catch (SQLException e) {
            createException = e;
            // if it can't be compiled, then it's a 'zero column table'
            // this avoids problems when creating the view when opening the
            // database
            tables = ObjectArray.newInstance();
            cols = new Column[0];
            if (recursive && columnNames != null) {
                cols = new Column[columnNames.length];
                for (int i = 0; i < columnNames.length; i++) {
                    cols[i] = new Column(columnNames[i], Value.STRING);
                }
                index.setRecursive(true);
                recursive = true;
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

    public PlanItem getBestPlanItem(Session session, int[] masks) throws SQLException {
        PlanItem item = new PlanItem();
        item.cost = index.getCost(session, masks);
        IntArray masksArray = new IntArray(masks == null ? MemoryUtils.EMPTY_INTS : masks);
        ViewIndex i2 = indexCache.get(masksArray);
        if (i2 == null || i2.getSession() != session) {
            i2 = new ViewIndex(this, index, session, masks);
            indexCache.put(masksArray, i2);
        }
        item.setIndex(i2);
        return item;
    }

    public String getDropSQL() {
        return "DROP VIEW IF EXISTS " + getSQL();
    }

    public String getCreateSQL() {
        StatementBuilder buff = new StatementBuilder("CREATE FORCE VIEW ");
        buff.append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        if (columns.length > 0) {
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

    public void checkRename() {
        // ok
    }

    public void lock(Session session, boolean exclusive, boolean force) {
        // exclusive lock means: the view will be dropped
    }

    public void close(Session session) {
        // nothing to do
    }

    public void unlock(Session s) {
        // nothing to do
    }

    public boolean isLockedExclusively() {
        return false;
    }

    public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            int headPos, String comment) throws SQLException {
        throw Message.getUnsupportedException("VIEW");
    }

    public void removeRow(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException("VIEW");
    }

    public void addRow(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException("VIEW");
    }

    public void checkSupportAlter() throws SQLException {
        throw Message.getUnsupportedException("VIEW");
    }

    public void truncate(Session session) throws SQLException {
        throw Message.getUnsupportedException("VIEW");
    }

    public long getRowCount(Session session) {
        throw Message.throwInternalError();
    }

    public boolean canGetRowCount() {
        // TODO view: could get the row count, but not that easy
        return false;
    }

    public boolean canDrop() {
        return true;
    }

    public String getTableType() {
        return Table.VIEW;
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        removeViewFromTables();
        super.removeChildrenAndResources(session);
        database.removeMeta(session, getId());
        querySQL = null;
        index = null;
        invalidate();
    }

    public String getSQL() {
        if (isTemporary()) {
            return "(" + querySQL + ")";
        }
        return super.getSQL();
    }

    public Index getScanIndex(Session session) throws SQLException {
        if (createException != null) {
            String msg = createException.getMessage();
            throw Message.getSQLException(ErrorCode.VIEW_IS_INVALID_2, createException, getSQL(), msg);
        }
        PlanItem item = getBestPlanItem(session, null);
        return item.getIndex();
    }

    public ObjectArray<Index> getIndexes() {
        return null;
    }

    /**
     * Re-compile the view query.
     *
     * @param session the session
     */
    public void recompile(Session session) throws SQLException {
        for (Table t : tables) {
            t.removeView(this);
        }
        tables.clear();
        initColumnsAndTables(session);
    }

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
    public static TableView createTempView(Session session, User owner, String name, Query query, Query topQuery) throws SQLException {
        Schema mainSchema = session.getDatabase().getSchema(Constants.SCHEMA_MAIN);
        String querySQL = query.getPlanSQL();
        TableView v = new TableView(mainSchema, 0, name, querySQL, query.getParameters(), null, session,
                false);
        v.setTopQuery(topQuery);
        if (v.createException != null) {
            throw v.createException;
        }
        v.setOwner(owner);
        v.setTemporary(true);
        return v;
    }

    private void setTopQuery(Query topQuery) {
        this.topQuery = topQuery;
    }

    public long getRowCountApproximation() {
        return ROW_COUNT_APPROXIMATION;
    }

    public int getParameterOffset() {
        return topQuery == null ? 0 : topQuery.getParameters().size();
    }

    public boolean isDeterministic() {
        return viewQuery.isEverything(ExpressionVisitor.DETERMINISTIC);
    }

}
