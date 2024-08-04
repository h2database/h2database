/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.query;

import static org.h2.expression.Expression.WITHOUT_PARENTHESES;
import static org.h2.util.HasSQL.DEFAULT_SQL_FLAGS;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.command.QueryScope;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.SessionLocal;
import org.h2.expression.Alias;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.result.SortOrder;
import org.h2.table.CTE;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.DerivedTable;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.ExtTypeInfoRow;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueInteger;
import org.h2.value.ValueNull;

/**
 * Represents a SELECT statement (simple, or union).
 */
public abstract class Query extends Prepared {

    /**
     * Evaluated values of OFFSET and FETCH clauses.
     */
    static final class OffsetFetch {

        /**
         * OFFSET value.
         */
        final long offset;

        /**
         * FETCH value.
         */
        final long fetch;

        /**
         * Whether FETCH value is a PERCENT value.
         */
        final boolean fetchPercent;

        OffsetFetch(long offset, long fetch, boolean fetchPercent) {
            this.offset = offset;
            this.fetch = fetch;
            this.fetchPercent = fetchPercent;
        }

    }

    /**
     * The column list, including invisible expressions such as order by expressions.
     */
    ArrayList<Expression> expressions;

    /**
     * Array of expressions.
     *
     * @see #expressions
     */
    Expression[] expressionArray;

    /**
     * Describes elements of the ORDER BY clause of a query.
     */
    ArrayList<QueryOrderBy> orderList;

    /**
     *  A sort order represents an ORDER BY clause in a query.
     */
    SortOrder sort;

    /**
     * The fetch expression as specified in the FETCH, LIMIT, or TOP clause.
     */
    Expression fetchExpr;

    /**
     * Whether limit expression specifies percentage of rows.
     */
    boolean fetchPercent;

    /**
     * Whether tied rows should be included in result too.
     */
    boolean withTies;

    /**
     * The offset expression as specified in the OFFSET clause.
     */
    Expression offsetExpr;

    /**
     * Whether the result must only contain distinct rows.
     */
    boolean distinct;

    /**
     * Whether the result needs to support random access.
     */
    boolean randomAccessResult;

    /**
     * The visible columns (the ones required in the result).
     */
    int visibleColumnCount;

    /**
     * Number of columns including visible columns and additional virtual
     * columns for ORDER BY and DISTINCT ON clauses. This number does not
     * include virtual columns for HAVING and QUALIFY.
     */
    int resultColumnCount;

    private boolean noCache;
    private long lastLimit;
    private long lastEvaluated;
    private ResultInterface lastResult;
    private Boolean lastExists;
    private Value[] lastParameters;
    private boolean cacheableChecked;
    private boolean neverLazy;

    boolean checkInit;

    boolean isPrepared;

    /**
     * The outer scope of this query.
     */
    private QueryScope outerQueryScope;

    /**
     * The WITH clause of this query.
     */
    private LinkedHashMap<String, Table> withClause;

    Query(SessionLocal session) {
        super(session);
    }

    public void setNeverLazy(boolean b) {
        this.neverLazy = b;
    }

    public boolean isNeverLazy() {
        return neverLazy;
    }

    /**
     * Check if this is a UNION query.
     *
     * @return {@code true} if this is a UNION query
     */
    public abstract boolean isUnion();

    @Override
    public ResultInterface queryMeta() {
        LocalResult result = new LocalResult(session, expressionArray, visibleColumnCount, resultColumnCount);
        result.done();
        return result;
    }

    /**
     * Execute the query without checking the cache. If a target is specified,
     * the results are written to it, and the method returns null. If no target
     * is specified, a new LocalResult is created and returned.
     *
     * @param limit the limit as specified in the JDBC method call
     * @param target the target to write results to
     * @return the result
     */
    protected abstract ResultInterface queryWithoutCache(long limit, ResultTarget target);

    private ResultInterface queryWithoutCacheLazyCheck(long limit, ResultTarget target) {
        boolean disableLazy = neverLazy && session.isLazyQueryExecution();
        if (disableLazy) {
            session.setLazyQueryExecution(false);
        }
        try {
            return queryWithoutCache(limit, target);
        } finally {
            if (disableLazy) {
                session.setLazyQueryExecution(true);
            }
        }
    }

    /**
     * Initialize the query.
     */
    public abstract void init();

    @Override
    public final void prepare() {
        if (!checkInit) {
            throw DbException.getInternalError("not initialized");
        }
        if (isPrepared) {
            return;
        }
        prepareExpressions();
        preparePlan();
    }

    public abstract void prepareExpressions();

    public abstract void preparePlan();

    /**
     * The the list of select expressions.
     * This may include invisible expressions such as order by expressions.
     *
     * @return the list of expressions
     */
    public ArrayList<Expression> getExpressions() {
        return expressions;
    }

    /**
     * Calculate the cost to execute this query.
     *
     * @return the cost
     */
    public abstract double getCost();

    /**
     * Calculate the cost when used as a subquery.
     * This method returns a value between 10 and 1000000,
     * to ensure adding other values can't result in an integer overflow.
     *
     * @return the estimated cost as an integer
     */
    public int getCostAsExpression() {
        // ensure the cost is not larger than 1 million,
        // so that adding other values can't overflow
        return (int) Math.min(1_000_000d, 10d + 10d * getCost());
    }

    /**
     * Get all tables that are involved in this query.
     *
     * @return the set of tables
     */
    public abstract HashSet<Table> getTables();

    /**
     * Set the order by list.
     *
     * @param order the order by list
     */
    public void setOrder(ArrayList<QueryOrderBy> order) {
        orderList = order;
    }

    /**
     * Whether the query has an order.
     *
     * @return true if it has
     */
    public boolean hasOrder() {
        return orderList != null || sort != null;
    }

    /**
     * Returns FOR UPDATE clause, if any.
     * @return FOR UPDATE clause or {@code null}
     */
    public ForUpdate getForUpdate() {
        return null;
    }

    /**
     * Set the FOR UPDATE clause.
     *
     * @param forUpdate the new FOR UPDATE clause
     */
    public abstract void setForUpdate(ForUpdate forUpdate);

    /**
     * Get the column count of this query.
     *
     * @return the column count
     */
    public int getColumnCount() {
        return visibleColumnCount;
    }

    /**
     * Returns data type of rows.
     *
     * @return data type of rows
     */
    public TypeInfo getRowDataType() {
        if (visibleColumnCount == 1) {
            return expressionArray[0].getType();
        }
        return TypeInfo.getTypeInfo(Value.ROW, -1L, -1, new ExtTypeInfoRow(expressionArray, visibleColumnCount));
    }

    /**
     * Map the columns to the given column resolver.
     *
     * @param resolver
     *            the resolver
     * @param level
     *            the subquery level (0 is the top level query, 1 is the first
     *            subquery level)
     * @param outer
     *            whether this method was called from the outer query
     */
    public abstract void mapColumns(ColumnResolver resolver, int level, boolean outer);

    /**
     * Change the evaluatable flag. This is used when building the execution
     * plan.
     *
     * @param tableFilter the table filter
     * @param b the new value
     */
    public abstract void setEvaluatable(TableFilter tableFilter, boolean b);

    /**
     * Add a condition to the query. This is used for views.
     *
     * @param param the parameter
     * @param columnId the column index (0 meaning the first column)
     * @param comparisonType the comparison type
     */
    public abstract void addGlobalCondition(Parameter param, int columnId,
            int comparisonType);

    /**
     * Check whether adding condition to the query is allowed. This is not
     * allowed for views that have an order by and a limit, as it would affect
     * the returned results.
     *
     * @return true if adding global conditions is allowed
     */
    public abstract boolean allowGlobalConditions();

    /**
     * Check if this expression and all sub-expressions can fulfill a criteria.
     * If any part returns false, the result is false.
     *
     * @param visitor the visitor
     * @return if the criteria can be fulfilled
     */
    public abstract boolean isEverything(ExpressionVisitor visitor);

    @Override
    public boolean isReadOnly() {
        return isEverything(ExpressionVisitor.READONLY_VISITOR);
    }

    /**
     * Update all aggregate function values.
     *
     * @param s the session
     * @param stage select stage
     */
    public abstract void updateAggregate(SessionLocal s, int stage);

    /**
     * Call the before triggers on all tables.
     */
    public abstract void fireBeforeSelectTriggers();

    /**
     * Set the distinct flag only if it is possible, may be used as a possible
     * optimization only.
     */
    public void setDistinctIfPossible() {
        if (!isAnyDistinct() && offsetExpr == null && fetchExpr == null) {
            distinct = true;
        }
    }

    /**
     * @return whether this query is a plain {@code DISTINCT} query
     */
    public boolean isStandardDistinct() {
        return distinct;
    }

    /**
     * @return whether this query is a {@code DISTINCT} or
     *         {@code DISTINCT ON (...)} query
     */
    public boolean isAnyDistinct() {
        return distinct;
    }

    /**
     * Returns whether results support random access.
     *
     * @return whether results support random access
     */
    public boolean isRandomAccessResult() {
        return randomAccessResult;
    }

    /**
     * Whether results need to support random access.
     *
     * @param b the new value
     */
    public void setRandomAccessResult(boolean b) {
        randomAccessResult = b;
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    /**
     * Disable caching of result sets.
     */
    public void disableCache() {
        this.noCache = true;
    }

    private boolean getNoCache() {
        if (!cacheableChecked) {
            if (getMaxDataModificationId() == Long.MAX_VALUE || !isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR)
                    || !isEverything(ExpressionVisitor.INDEPENDENT_VISITOR)) {
                noCache = true;
            }
            cacheableChecked = true;
        }
        return noCache;
    }

    private static boolean sameParameters(Value[] params, Value[] lastParams) {
        for (int i = 0; i < params.length; i++) {
            Value a = lastParams[i], b = params[i];
            // Derived tables can have gaps in parameters
            if (a != null && !a.equals(b)) {
                return false;
            }
        }
        return true;
    }

    private  Value[] getParameterValues() {
        ArrayList<Parameter> list = getParameters();
        if (list == null) {
            return Value.EMPTY_VALUES;
        }
        int size = list.size();
        Value[] params = new Value[size];
        for (int i = 0; i < size; i++) {
            Parameter parameter = list.get(i);
            // Derived tables can have gaps in parameters
            params[i] = parameter != null ? parameter.getParamValue() : null;
        }
        return params;
    }

    @Override
    public final ResultInterface query(long maxrows) {
        return query(maxrows, null);
    }

    /**
     * Execute the query, writing the result to the target result.
     *
     * @param limit the maximum number of rows to return
     * @param target the target result (null will return the result)
     * @return the result set (if the target is not set).
     */
    public final ResultInterface query(long limit, ResultTarget target) {
        if (isUnion()) {
            // union doesn't always know the parameter list of the left and
            // right queries
            return queryWithoutCacheLazyCheck(limit, target);
        }
        fireBeforeSelectTriggers();
        if (getNoCache() || !getDatabase().getOptimizeReuseResults() ||
                (session.isLazyQueryExecution() && !neverLazy)) {
            return queryWithoutCacheLazyCheck(limit, target);
        }
        Value[] params = getParameterValues();
        long now = session.getStatementModificationDataId(), maxDataModificationId = getMaxDataModificationId();
        if (lastResult != null && !lastResult.isClosed() && limit == lastLimit //
                && maxDataModificationId <= lastEvaluated && sameParameters(params, lastParameters)) {
            lastResult = lastResult.createShallowCopy(session);
            if (lastResult != null) {
                lastResult.reset();
                return lastResult;
            }
        }
        closeLastResult();
        ResultInterface r = queryWithoutCacheLazyCheck(limit, target);
        if (maxDataModificationId <= now) {
            lastParameters = params;
            lastResult = r;
            lastEvaluated = now;
            lastLimit = limit;
        } else {
            lastParameters = null;
            lastResult = null;
            lastLimit = lastEvaluated = 0L;
        }
        lastExists = null;
        return r;
    }

    private void closeLastResult() {
        if (lastResult != null) {
            lastResult.close();
        }
    }

    /**
     * Execute the EXISTS predicate over the query.
     *
     * @return EXISTS predicate result
     */
    public final boolean exists() {
        if (isUnion()) {
            // union doesn't always know the parameter list of the left and
            // right queries
            return executeExists();
        }
        fireBeforeSelectTriggers();
        if (getNoCache() || !getDatabase().getOptimizeReuseResults()) {
            return executeExists();
        }
        Value[] params = getParameterValues();
        long now = session.getStatementModificationDataId(), maxDataModificationId = getMaxDataModificationId();
        if (lastExists != null && maxDataModificationId <= lastEvaluated && sameParameters(params, lastParameters)) {
            return lastExists;
        }
        boolean exists = executeExists();
        if (maxDataModificationId <= now) {
            lastParameters = params;
            lastExists = exists;
            lastEvaluated = now;
        } else {
            lastParameters = null;
            lastExists = null;
            lastEvaluated = 0L;
        }
        lastResult = null;
        return exists;
    }

    private boolean executeExists() {
        ResultInterface r = queryWithoutCacheLazyCheck(1L, null);
        boolean exists = r.hasNext();
        r.close();
        return exists;
    }

    /**
     * Initialize the order by list. This call may extend the expressions list.
     *
     * @param expressionSQL the select list SQL snippets
     * @param mustBeInResult all order by expressions must be in the select list
     * @param filters the table filters
     * @return {@code true} if ORDER BY clause is preserved, {@code false}
     *         otherwise
     */
    boolean initOrder(ArrayList<String> expressionSQL, boolean mustBeInResult, ArrayList<TableFilter> filters) {
        for (Iterator<QueryOrderBy> i = orderList.iterator(); i.hasNext();) {
            QueryOrderBy o = i.next();
            Expression e = o.expression;
            if (e == null) {
                continue;
            }
            if (e.isConstant()) {
                i.remove();
                continue;
            }
            int idx = initExpression(expressionSQL, e, mustBeInResult, filters);
            o.columnIndexExpr = ValueExpression.get(ValueInteger.get(idx + 1));
            o.expression = expressions.get(idx).getNonAliasExpression();
        }
        if (orderList.isEmpty()) {
            orderList = null;
            return false;
        }
        return true;
    }

    /**
     * Initialize the 'ORDER BY' or 'DISTINCT' expressions.
     *
     * @param expressionSQL the select list SQL snippets
     * @param e the expression.
     * @param mustBeInResult all order by expressions must be in the select list
     * @param filters the table filters.
     * @return index on the expression in the {@link #expressions} list.
     */
    int initExpression(ArrayList<String> expressionSQL, Expression e, boolean mustBeInResult,
            ArrayList<TableFilter> filters) {
        Database db = getDatabase();
        // special case: SELECT 1 AS A FROM DUAL ORDER BY A
        // (oracle supports it, but only in order by, not in group by and
        // not in having):
        // SELECT 1 AS A FROM DUAL ORDER BY -A
        if (e instanceof ExpressionColumn) {
            // order by expression
            ExpressionColumn exprCol = (ExpressionColumn) e;
            String tableAlias = exprCol.getOriginalTableAliasName();
            String col = exprCol.getOriginalColumnName();
            for (int j = 0, visible = getColumnCount(); j < visible; j++) {
                Expression ec = expressions.get(j);
                if (ec instanceof ExpressionColumn) {
                    // select expression
                    ExpressionColumn c = (ExpressionColumn) ec;
                    if (!db.equalsIdentifiers(col, c.getColumnName(session, j))) {
                        continue;
                    }
                    if (tableAlias == null) {
                        return j;
                    }
                    String ca = c.getOriginalTableAliasName();
                    if (ca != null) {
                        if (db.equalsIdentifiers(ca, tableAlias)) {
                            return j;
                        }
                    } else if (filters != null) {
                        // select id from test order by test.id
                        for (TableFilter f : filters) {
                            if (db.equalsIdentifiers(f.getTableAlias(), tableAlias)) {
                                return j;
                            }
                        }
                    }
                } else if (ec instanceof Alias) {
                    if (tableAlias == null && db.equalsIdentifiers(col, ec.getAlias(session, j))) {
                        return j;
                    }
                    Expression ec2 = ec.getNonAliasExpression();
                    if (ec2 instanceof ExpressionColumn) {
                        ExpressionColumn c2 = (ExpressionColumn) ec2;
                        String ta = exprCol.getSQL(DEFAULT_SQL_FLAGS, WITHOUT_PARENTHESES);
                        String tb = c2.getSQL(DEFAULT_SQL_FLAGS, WITHOUT_PARENTHESES);
                        String s2 = c2.getColumnName(session, j);
                        if (db.equalsIdentifiers(col, s2) && db.equalsIdentifiers(ta, tb)) {
                            return j;
                        }
                    }
                }
            }
        } else if (expressionSQL != null) {
            String s = e.getSQL(DEFAULT_SQL_FLAGS, WITHOUT_PARENTHESES);
            for (int j = 0, size = expressionSQL.size(); j < size; j++) {
                if (db.equalsIdentifiers(expressionSQL.get(j), s)) {
                    return j;
                }
            }
        }
        if (expressionSQL == null
                || mustBeInResult && !db.getMode().allowUnrelatedOrderByExpressionsInDistinctQueries
                        && !checkOrderOther(session, e, expressionSQL)) {
            throw DbException.get(ErrorCode.ORDER_BY_NOT_IN_RESULT, e.getTraceSQL());
        }
        int idx = expressions.size();
        expressions.add(e);
        expressionSQL.add(e.getSQL(DEFAULT_SQL_FLAGS, WITHOUT_PARENTHESES));
        return idx;
    }

    /**
     * An additional check for expression in ORDER BY list for DISTINCT selects
     * that was not matched with selected expressions in regular way. This
     * method allows expressions based only on selected expressions in different
     * complicated ways with functions, comparisons, or operators.
     *
     * @param session session
     * @param expr expression to check
     * @param expressionSQL SQL of allowed expressions
     * @return whether the specified expression should be allowed in ORDER BY
     *         list of DISTINCT select
     */
    private static boolean checkOrderOther(SessionLocal session, Expression expr, ArrayList<String> expressionSQL) {
        if (expr == null || expr.isConstant()) {
            // ValueExpression, null expression in CASE, or other
            return true;
        }
        String exprSQL = expr.getSQL(DEFAULT_SQL_FLAGS, WITHOUT_PARENTHESES);
        for (String sql: expressionSQL) {
            if (session.getDatabase().equalsIdentifiers(exprSQL, sql)) {
                return true;
            }
        }
        int count = expr.getSubexpressionCount();
        if (!expr.isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR)) {
            return false;
        } else if (count <= 0) {
            // Expression is an ExpressionColumn, Parameter, SequenceValue or
            // has other unsupported type without subexpressions
            return false;
        }
        for (int i = 0; i < count; i++) {
            if (!checkOrderOther(session, expr.getSubexpression(i), expressionSQL)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Create a {@link SortOrder} object given the list of {@link QueryOrderBy}
     * objects.
     *
     * @param orderList a list of {@link QueryOrderBy} elements
     * @param expressionCount the number of columns in the query
     */
    void prepareOrder(ArrayList<QueryOrderBy> orderList, int expressionCount) {
        int size = orderList.size();
        int[] index = new int[size];
        int[] sortType = new int[size];
        for (int i = 0; i < size; i++) {
            QueryOrderBy o = orderList.get(i);
            int idx;
            boolean reverse = false;
            Value v = o.columnIndexExpr.getValue(null);
            if (v == ValueNull.INSTANCE) {
                // parameter not yet set - order by first column
                idx = 0;
            } else {
                idx = v.getInt();
                if (idx < 0) {
                    reverse = true;
                    idx = -idx;
                }
                idx -= 1;
                if (idx < 0 || idx >= expressionCount) {
                    throw DbException.get(ErrorCode.ORDER_BY_NOT_IN_RESULT, Integer.toString(idx + 1));
                }
            }
            index[i] = idx;
            int type = o.sortType;
            if (reverse) {
                // TODO NULLS FIRST / LAST should be inverted too?
                type ^= SortOrder.DESCENDING;
            }
            sortType[i] = type;
        }
        sort = new SortOrder(session, index, sortType, orderList);
        this.orderList = null;
    }

    /**
     * Removes constant expressions from the sort order.
     *
     * Some constants are detected only after optimization of expressions, this
     * method removes them from the sort order only. They are currently
     * preserved in the list of expressions.
     */
    void cleanupOrder() {
        int sourceIndexes[] = sort.getQueryColumnIndexes();
        int count = sourceIndexes.length;
        int constants = 0;
        for (int i = 0; i < count; i++) {
            if (expressions.get(sourceIndexes[i]).isConstant()) {
                constants++;
            }
        }
        if (constants == 0) {
            return;
        }
        if (constants == count) {
            sort = null;
            return;
        }
        int size = count - constants;
        int[] indexes = new int[size];
        int[] sortTypes = new int[size];
        int[] sourceSortTypes = sort.getSortTypes();
        ArrayList<QueryOrderBy> orderList = sort.getOrderList();
        for (int i = 0, j = 0; j < size; i++) {
            if (!expressions.get(sourceIndexes[i]).isConstant()) {
                indexes[j] = sourceIndexes[i];
                sortTypes[j] = sourceSortTypes[i];
                j++;
            } else {
                orderList.remove(j);
            }
        }
        sort = new SortOrder(session, indexes, sortTypes, orderList);
    }

    @Override
    public int getType() {
        return CommandInterface.SELECT;
    }

    public void setOffset(Expression offset) {
        this.offsetExpr = offset;
    }

    public Expression getOffset() {
        return offsetExpr;
    }

    public void setFetch(Expression fetch) {
        this.fetchExpr = fetch;
    }

    public Expression getFetch() {
        return fetchExpr;
    }

    public void setFetchPercent(boolean fetchPercent) {
        this.fetchPercent = fetchPercent;
    }

    public boolean isFetchPercent() {
        return fetchPercent;
    }

    public void setWithTies(boolean withTies) {
        this.withTies = withTies;
    }

    public boolean isWithTies() {
        return withTies;
    }

    /**
     * Add a parameter to the parameter list.
     *
     * @param param the parameter to add
     */
    void addParameter(Parameter param) {
        if (parameters == null) {
            parameters = Utils.newSmallArrayList();
        }
        parameters.add(param);
    }

    public final long getMaxDataModificationId() {
        ExpressionVisitor visitor = ExpressionVisitor.getMaxModificationIdVisitor();
        isEverything(visitor);
        return Math.max(visitor.getMaxDataModificationId(), session.getSnapshotDataModificationId());
    }

    /**
     * Returns the scope of the outer query.
     *
     * @return the scope of the outer query
     */
    public QueryScope getOuterQueryScope() {
        return outerQueryScope;
    }

    /**
     * Sets the scope of the outer query.
     *
     * @param outerQueryScope
     *            the scope of the outer query
     */
    public void setOuterQueryScope(QueryScope outerQueryScope) {
        this.outerQueryScope = outerQueryScope;
    }

    /**
     * Sets the WITH clause of this query.
     *
     * @param withClause
     *            the WITH clause of this query
     */
    public void setWithClause(LinkedHashMap<String, Table> withClause) {
        this.withClause = withClause;
    }

    protected void writeWithList(StringBuilder builder, int sqlFlags) {
        if (withClause != null) {
            boolean recursive = false;
            for (Table t : withClause.values()) {
                if (((CTE) t).isRecursive()) {
                    recursive = true;
                    break;
                }
            }
            builder.append("WITH ");
            if (recursive) {
                builder.append(" RECURSIVE ");
            }
            boolean f = false;
            for (Table table : withClause.values()) {
                if (!f) {
                    f = true;
                } else {
                    builder.append(",\n");
                }
                table.getSQL(builder, sqlFlags).append('(');
                Column.writeColumns(builder, table.getColumns(), sqlFlags).append(") AS (\n");
                StringUtils.indent(builder, ((CTE) table).getQuerySQL(), 4, true).append(')');
            }
            builder.append('\n');
        }
    }

    /**
     * Appends ORDER BY, OFFSET, and FETCH clauses to the plan.
     *
     * @param builder query plan string builder.
     * @param sqlFlags formatting flags
     * @param expressions the array of expressions
     */
    void appendEndOfQueryToSQL(StringBuilder builder, int sqlFlags, Expression[] expressions) {
        if (sort != null) {
            sort.getSQL(builder.append("\nORDER BY "), expressions, visibleColumnCount, sqlFlags);
        } else if (orderList != null) {
            builder.append("\nORDER BY ");
            for (int i = 0, l = orderList.size(); i < l; i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                orderList.get(i).getSQL(builder, sqlFlags);
            }
        }
        if (offsetExpr != null) {
            String count = offsetExpr.getSQL(sqlFlags, WITHOUT_PARENTHESES);
            builder.append("\nOFFSET ").append(count).append("1".equals(count) ? " ROW" : " ROWS");
        }
        if (fetchExpr != null) {
            builder.append("\nFETCH ").append(offsetExpr != null ? "NEXT" : "FIRST");
            String count = fetchExpr.getSQL(sqlFlags, WITHOUT_PARENTHESES);
            boolean withCount = fetchPercent || !"1".equals(count);
            if (withCount) {
                builder.append(' ').append(count);
                if (fetchPercent) {
                    builder.append(" PERCENT");
                }
            }
            builder.append(!withCount ? " ROW" : " ROWS")
                    .append(withTies ? " WITH TIES" : " ONLY");
        }
    }

    /**
     * Evaluates OFFSET and FETCH expressions.
     *
     * @param maxRows
     *            additional limit
     * @return the evaluated values
     */
    OffsetFetch getOffsetFetch(long maxRows) {
        long offset;
        if (offsetExpr != null) {
            Value v = offsetExpr.getValue(session);
            if (v == ValueNull.INSTANCE || (offset = v.getLong()) < 0) {
                throw DbException.getInvalidValueException("result OFFSET", v);
            }
        } else {
            offset = 0;
        }
        long fetch = maxRows == 0 ? -1 : maxRows;
        if (fetchExpr != null) {
            Value v = fetchExpr.getValue(session);
            long l;
            if (v == ValueNull.INSTANCE || (l = v.getLong()) < 0) {
                throw DbException.getInvalidValueException("result FETCH", v);
            }
            fetch = fetch < 0 ? l : Math.min(l, fetch);
        }
        boolean fetchPercent = this.fetchPercent;
        if (fetchPercent) {
            if (fetch > 100) {
                throw DbException.getInvalidValueException("result FETCH PERCENT", fetch);
            }
            // 0 PERCENT means 0
            if (fetch == 0) {
                fetchPercent = false;
            }
        }
        return new OffsetFetch(offset, fetch, fetchPercent);
    }

    /**
     * Applies limits, if any, to a result and makes it ready for value
     * retrieval.
     *
     * @param result
     *            the result
     * @param offset
     *            OFFSET value
     * @param fetch
     *            FETCH value
     * @param fetchPercent
     *            whether FETCH value is a PERCENT value
     * @param target
     *            target result or null
     * @return the result or null
     */
    LocalResult finishResult(LocalResult result, long offset, long fetch, boolean fetchPercent, ResultTarget target) {
        if (offset != 0) {
            result.setOffset(offset);
        }
        if (fetch >= 0) {
            result.setLimit(fetch);
            result.setFetchPercent(fetchPercent);
            if (withTies) {
                result.setWithTies(sort);
            }
        }
        result.done();
        if (randomAccessResult && !distinct) {
            result = convertToDistinct(result);
        }
        if (target != null) {
            while (result.next()) {
                target.addRow(result.currentRow());
            }
            result.close();
            return null;
        }
        return result;
    }

    /**
     * Convert a result into a distinct result, using the current columns.
     *
     * @param result the source
     * @return the distinct result
     */
    LocalResult convertToDistinct(ResultInterface result) {
        LocalResult distinctResult = new LocalResult(session, expressionArray, visibleColumnCount, resultColumnCount);
        distinctResult.setDistinct();
        result.reset();
        while (result.next()) {
            distinctResult.addRow(result.currentRow());
        }
        result.close();
        distinctResult.done();
        return distinctResult;
    }

    /**
     * Converts this query to a table or a view.
     *
     * @param alias alias name for the view
     * @param columnTemplates column templates, or {@code null}
     * @param parameters the parameters
     * @param forCreateView if true, a system session will be used for the view
     * @param topQuery the top level query
     * @return the table or the view
     */
    public Table toTable(String alias, Column[] columnTemplates, ArrayList<Parameter> parameters,
            boolean forCreateView, Query topQuery) {
        setParameterList(new ArrayList<>(parameters));
        if (!checkInit) {
            init();
        }
        return new DerivedTable(forCreateView ? getDatabase().getSystemSession() : session, alias,
                columnTemplates, this, topQuery);
    }

    @Override
    public void collectDependencies(HashSet<DbObject> dependencies) {
        ExpressionVisitor visitor = ExpressionVisitor.getDependenciesVisitor(dependencies);
        isEverything(visitor);
    }

    /**
     * Check if this query will always return the same value and has no side
     * effects.
     *
     * @return if this query will always return the same value and has no side
     *         effects.
     */
    public boolean isConstantQuery() {
        return !hasOrder() && (offsetExpr == null || offsetExpr.isConstant())
                && (fetchExpr == null || fetchExpr.isConstant());
    }

    /**
     * If this query is determined as a single-row query, returns a replacement
     * expression.
     *
     * @return the expression, or {@code null}
     */
    public Expression getIfSingleRow() {
        return null;
    }

    @Override
    public boolean isRetryable() {
        ForUpdate forUpdate = getForUpdate();
        return forUpdate == null || forUpdate.getType() == ForUpdate.Type.SKIP_LOCKED;
    }

}
