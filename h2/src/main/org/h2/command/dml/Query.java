/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;
import java.util.HashSet;

import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Alias;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.ValueExpression;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.SortOrder;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueNull;

/**
 * Represents a SELECT statement (simple, or union).
 */
public abstract class Query extends Prepared {

    /**
     * The limit expression as specified in the LIMIT or TOP clause.
     */
    protected Expression limitExpr;

    /**
     * The offset expression as specified in the LIMIT .. OFFSET clause.
     */
    protected Expression offsetExpr;

    /**
     * The sample size
     */
    protected int sampleSize;

    private int lastLimit;
    private long lastEvaluated;
    private LocalResult lastResult;
    private Value[] lastParameters;

    public Query(Session session) {
        super(session);
    }

    /**
     * Execute the query without checking the cache.
     *
     * @param limit the limit as specified in the JDBC method call
     * @return the result
     */
    protected abstract LocalResult queryWithoutCache(int limit) throws SQLException;

    /**
     * Initialize the query.
     */
    public abstract void init() throws SQLException;

    /**
     * The the list of select expressions.
     * This may include invisible expressions such as order by expressions.
     *
     * @return the list of expressions
     */
    public abstract ObjectArray<Expression> getExpressions();

    /**
     * Calculate the cost to execute this query.
     *
     * @return the cost
     */
    public abstract double getCost();

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
    public abstract void setOrder(ObjectArray<SelectOrderBy> order);

    /**
     * Set the 'for update' flag.
     *
     * @param forUpdate the new setting
     */
    public abstract void setForUpdate(boolean forUpdate);

    /**
     * Get the column count of this query.
     *
     * @return the column count
     */
    public abstract int getColumnCount();

    /**
     * Map the columns to the given column resolver.
     *
     * @param resolver
     *            the resolver
     * @param level
     *            the subquery level (0 is the top level query, 1 is the first
     *            subquery level)
     */
    public abstract void mapColumns(ColumnResolver resolver, int level) throws SQLException;

    /**
     * Change the evaluatable flag. This is used when building the execution plan.
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
    public abstract void addGlobalCondition(Parameter param, int columnId, int comparisonType) throws SQLException;

    /**
     * Set the distinct flag.
     *
     * @param b the new value
     */
    public abstract void setDistinct(boolean b);

    /**
     * Get the alias (or column name) of the first column.
     * This is used to convert IN(SELECT ...) queries to inner joins.
     *
     * @param s the session
     * @return the alias or column name
     */
    public abstract String getFirstColumnAlias(Session s);

    /**
     * Check if this expression and all sub-expressions can fulfill a criteria.
     * If any part returns false, the result is false.
     *
     * @param visitor the visitor
     * @return if the criteria can be fulfilled
     */
    public abstract boolean isEverything(ExpressionVisitor visitor);

    /**
     * Update all aggregate function values.
     *
     * @param s the session
     */
    public abstract void updateAggregate(Session s) throws SQLException;

    public boolean isQuery() {
        return true;
    }

    public boolean isTransactional() {
        return true;
    }

    private boolean sameResultAsLast(Session s, Value[] params, Value[] lastParams, long lastEval)
            throws SQLException {
        Database db = s.getDatabase();
        for (int i = 0; i < params.length; i++) {
            if (!db.areEqual(lastParams[i], params[i])) {
                return false;
            }
        }
        if (!isEverything(ExpressionVisitor.DETERMINISTIC) || !isEverything(ExpressionVisitor.INDEPENDENT)) {
            return false;
        }
        if (db.getModificationDataId() > lastEval && getMaxDataModificationId() > lastEval) {
            return false;
        }
        return true;
    }

    public final Value[] getParameterValues() {
        ObjectArray<Parameter> list = getParameters();
        if (list == null) {
            list = ObjectArray.newInstance();
        }
        Value[] params = new Value[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Value v = list.get(i).getParamValue();
            params[i] = v;
        }
        return params;
    }

    public LocalResult query(int limit) throws SQLException {
        if (!session.getDatabase().getOptimizeReuseResults()) {
            return queryWithoutCache(limit);
        }
        Value[] params = getParameterValues();
        long now = session.getDatabase().getModificationDataId();
        if (isEverything(ExpressionVisitor.DETERMINISTIC)) {
            if (lastResult != null && !lastResult.isClosed() && limit == lastLimit) {
                if (sameResultAsLast(session, params, lastParameters, lastEvaluated)) {
                    lastResult = lastResult.createShallowCopy(session);
                    if (lastResult != null) {
                        lastResult.reset();
                        return lastResult;
                    }
                }
            }
        }
        lastParameters = params;
        closeLastResult();
        lastResult = queryWithoutCache(limit);
        this.lastEvaluated = now;
        lastLimit = limit;
        return lastResult;
    }

    private void closeLastResult() {
        if (lastResult != null) {
            lastResult.close();
        }
    }

    /**
     * Init the order by list.
     *
     * @param expressions the select list expressions
     * @param expressionSQL the select list SQL snippets
     * @param orderList the order by list
     * @param visible the number of visible columns in the select list
     * @param mustBeInResult all order by expressions must be in the select list
     */
    void initOrder(ObjectArray<Expression> expressions, ObjectArray<String> expressionSQL, ObjectArray<SelectOrderBy> orderList, int visible,
            boolean mustBeInResult) throws SQLException {
        for (SelectOrderBy o : orderList) {
            Expression e = o.expression;
            if (e == null) {
                continue;
            }
            // special case: SELECT 1 AS A FROM DUAL ORDER BY A
            // (oracle supports it, but only in order by, not in group by and
            // not in having):
            // SELECT 1 AS A FROM DUAL ORDER BY -A
            boolean isAlias = false;
            int idx = expressions.size();
            if (e instanceof ExpressionColumn) {
                ExpressionColumn exprCol = (ExpressionColumn) e;
                String tableAlias = exprCol.getOriginalTableAliasName();
                String col = exprCol.getOriginalColumnName();
                for (int j = 0; j < visible; j++) {
                    boolean found = false;
                    Expression ec = expressions.get(j);
                    if (ec instanceof ExpressionColumn) {
                        ExpressionColumn c = (ExpressionColumn) ec;
                        found = col.equals(c.getColumnName());
                        if (tableAlias != null && found) {
                            String ca = c.getOriginalTableAliasName();
                            if (ca != null) {
                                found = tableAlias.equals(ca);
                            }
                        }
                    } else if (!(ec instanceof Alias)) {
                        continue;
                    } else if (tableAlias == null && col.equals(ec.getAlias())) {
                        found = true;
                    } else {
                        Expression ec2 = ec.getNonAliasExpression();
                        if (ec2 instanceof ExpressionColumn) {
                            ExpressionColumn c2 = (ExpressionColumn) ec2;
                            String ta = exprCol.getSQL();
                            // exprCol.getTableAlias();
                            String tb = c2.getSQL();
                            // getTableAlias();
                            found = col.equals(c2.getColumnName());
                            if (!StringUtils.equals(ta, tb)) {
                                found = false;
                            }
                        }
                    }
                    if (found) {
                        idx = j;
                        isAlias = true;
                        break;
                    }
                }
            } else {
                String s = e.getSQL();
                for (int j = 0; expressionSQL != null && j < expressionSQL.size(); j++) {
                    String s2 = expressionSQL.get(j);
                    if (s2.equals(s)) {
                        idx = j;
                        isAlias = true;
                        break;
                    }
                }
            }
            if (!isAlias) {
                if (mustBeInResult) {
                    throw Message.getSQLException(ErrorCode.ORDER_BY_NOT_IN_RESULT, e.getSQL());
                }
                expressions.add(e);
                String sql = e.getSQL();
                expressionSQL.add(sql);
            }
            o.columnIndexExpr = ValueExpression.get(ValueInt.get(idx + 1));
        }
    }

    /**
     * Create a {@link SortOrder} object given the list of {@link SelectOrderBy}
     * objects. The expression list is extended if necessary.
     *
     * @param orderList a list of {@link SelectOrderBy} elements
     * @param expressionCount the number of columns in the query
     * @return the {@link SortOrder} object
     */
    public SortOrder prepareOrder(ObjectArray<SelectOrderBy> orderList, int expressionCount) throws SQLException {
        int[] index = new int[orderList.size()];
        int[] sortType = new int[orderList.size()];
        for (int i = 0; i < orderList.size(); i++) {
            SelectOrderBy o = orderList.get(i);
            int idx;
            boolean reverse = false;
            Expression expr = o.columnIndexExpr;
            Value v = expr.getValue(null);
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
                    throw Message.getSQLException(ErrorCode.ORDER_BY_NOT_IN_RESULT, "" + (idx + 1));
                }
            }
            index[i] = idx;
            boolean desc = o.descending;
            if (reverse) {
                desc = !desc;
            }
            int type = desc ? SortOrder.DESCENDING : SortOrder.ASCENDING;
            if (o.nullsFirst) {
                type += SortOrder.NULLS_FIRST;
            } else if (o.nullsLast) {
                type += SortOrder.NULLS_LAST;
            }
            sortType[i] = type;
        }
        return new SortOrder(session.getDatabase(), index, sortType);
    }

    public void setOffset(Expression offset) {
        this.offsetExpr = offset;
    }

    public void setLimit(Expression limit) {
        this.limitExpr = limit;
    }

    /**
     * Add a parameter to the parameter list.
     *
     * @param param the parameter to add
     */
    void addParameter(Parameter param) {
        if (parameters == null) {
            parameters = ObjectArray.newInstance();
        }
        parameters.add(param);
    }

    public void setSampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
    }

    public final long getMaxDataModificationId() {
        ExpressionVisitor visitor = ExpressionVisitor.get(ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID);
        isEverything(visitor);
        return visitor.getMaxDataModificationId();
    }

    /**
     * Visit all expressions and subqueries in this query using the visitor pattern.
     *
     * @param expressionVisitorType the visitor type
     * @return true if no component returned false
     */
    public final boolean isEverything(int expressionVisitorType) {
        ExpressionVisitor visitor = ExpressionVisitor.get(expressionVisitorType);
        return isEverything(visitor);
    }

}
