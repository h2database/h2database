/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;
import java.util.HashSet;

import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.table.TableValueConstructorTable;
import org.h2.value.Value;

/**
 * Table value constructor.
 */
public class TableValueConstructor extends Query {

    private final ArrayList<ArrayList<Expression>> rows;

    /**
     * The table.
     */
    final TableValueConstructorTable table;

    private final TableValueColumnResolver columnResolver;

    private boolean isPrepared, checkInit;

    private double cost;

    /**
     * Creates new instance of table value constructor.
     *
     * @param session
     *            the session
     * @param columns
     *            the columns
     * @param rows
     *            the rows
     */
    public TableValueConstructor(Session session, Column[] columns, ArrayList<ArrayList<Expression>> rows) {
        super(session);
        this.rows = rows;
        Database database = session.getDatabase();
        int columnCount = columns.length;
        ArrayList<Expression> expressions = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            expressions.add(new ExpressionColumn(database, null, null, columns[i].getName(), false));
        }
        this.expressions = expressions;
        table = new TableValueConstructorTable(session.getDatabase().getMainSchema(), session, columns, rows);
        columnResolver = new TableValueColumnResolver();
    }

    /**
     * Appends visible columns of all rows to the specified result.
     *
     * @param session
     *            the session
     * @param result
     *            the result
     * @param columns
     *            the columns
     * @param rows
     *            the rows with data
     */
    public static void getVisibleResult(Session session, ResultTarget result, Column[] columns,
            ArrayList<ArrayList<Expression>> rows) {
        int count = columns.length;
        for (ArrayList<Expression> row : rows) {
            Value[] values = new Value[count];
            for (int i = 0; i < count; i++) {
                values[i] = row.get(i).getValue(session).convertTo(columns[i].getType(), session, false, null);
            }
            result.addRow(values);
        }
    }

    /**
     * Appends the SQL of the values to the specified string builder..
     *
     * @param builder
     *            string builder
     * @param alwaysQuote
     *            quote all identifiers
     * @param rows
     *            the values
     */
    public static void getValuesSQL(StringBuilder builder, boolean alwaysQuote, //
            ArrayList<ArrayList<Expression>> rows) {
        builder.append("VALUES ");
        int rowCount = rows.size();
        for (int i = 0; i < rowCount; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append('(');
            Expression.writeExpressions(builder, rows.get(i), alwaysQuote);
            builder.append(')');
        }
    }

    @Override
    public boolean isUnion() {
        return false;
    }

    @Override
    public void prepareJoinBatch() {
    }

    @Override
    protected ResultInterface queryWithoutCache(int limit, ResultTarget target) {
        OffsetFetch offsetFetch = getOffsetFetch(limit);
        long offset = offsetFetch.offset;
        int fetch = offsetFetch.fetch;
        boolean fetchPercent = offsetFetch.fetchPercent;
        int visibleColumnCount = this.visibleColumnCount, resultColumnCount = this.resultColumnCount;
        LocalResult result = session.getDatabase().getResultFactory().create(session, expressionArray,
                visibleColumnCount, resultColumnCount);
        if (sort != null) {
            result.setSortOrder(sort);
        }
        if (distinct) {
            result.setDistinct();
        }
        Column[] columns = table.getColumns();
        if (visibleColumnCount == resultColumnCount) {
            getVisibleResult(session, result, columns, rows);
        } else {
            for (ArrayList<Expression> row : rows) {
                Value[] values = new Value[resultColumnCount];
                for (int i = 0; i < visibleColumnCount; i++) {
                    values[i] = row.get(i).getValue(session).convertTo(columns[i].getType(), session, false, null);
                }
                columnResolver.currentRow = values;
                for (int i = visibleColumnCount; i < resultColumnCount; i++) {
                    values[i] = expressionArray[i].getValue(session);
                }
                result.addRow(values);
            }
            columnResolver.currentRow = null;
        }
        return finishResult(result, offset, fetch, fetchPercent, target);
    }

    @Override
    public void init() {
        if (checkInit) {
            DbException.throwInternalError();
        }
        checkInit = true;
        if (withTies && !hasOrder()) {
            throw DbException.get(ErrorCode.WITH_TIES_WITHOUT_ORDER_BY);
        }
    }

    @Override
    public void prepare() {
        if (isPrepared) {
            // sometimes a subquery is prepared twice (CREATE TABLE AS SELECT)
            return;
        }
        if (!checkInit) {
            DbException.throwInternalError("not initialized");
        }
        isPrepared = true;
        visibleColumnCount = expressions.size();
        if (orderList != null) {
            ArrayList<String> expressionsSQL = new ArrayList<>();
            for (Expression e : expressions) {
                expressionsSQL.add(e.getSQL(true));
            }
            initOrder(session, expressions, expressionsSQL, orderList, getColumnCount(), false, null);
            sort = prepareOrder(orderList, expressions.size());
            orderList = null;
        }
        resultColumnCount = expressions.size();
        for (int i = 0; i < resultColumnCount; i++) {
            expressions.get(i).mapColumns(columnResolver, 0, Expression.MAP_INITIAL);
        }
        for (int i = visibleColumnCount; i < resultColumnCount; i++) {
            expressions.set(i, expressions.get(i).optimize(session));
        }
        expressionArray = expressions.toArray(new Expression[0]);
        double cost = 0;
        int columnCount = visibleColumnCount;
        for (ArrayList<Expression> row : rows) {
            for (int i = 0; i < columnCount; i++) {
                cost += row.get(i).getCost();
            }
        }
        this.cost = cost + rows.size();
    }

    @Override
    public double getCost() {
        return cost;
    }

    @Override
    public HashSet<Table> getTables() {
        HashSet<Table> tables = new HashSet<>(1, 1f);
        tables.add(table);
        return tables;
    }

    @Override
    public void setForUpdate(boolean forUpdate) {
        throw DbException.get(ErrorCode.RESULT_SET_READONLY);
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        int columnCount = expressions.size();
        for (ArrayList<Expression> row : rows) {
            for (int i = 0; i < columnCount; i++) {
                row.get(i).mapColumns(resolver, level, Expression.MAP_INITIAL);
            }
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        int columnCount = expressionArray.length;
        for (ArrayList<Expression> row : rows) {
            for (int i = 0; i < columnCount; i++) {
                row.get(i).setEvaluatable(tableFilter, b);
            }
        }
    }

    @Override
    public void addGlobalCondition(Parameter param, int columnId, int comparisonType) {
        // Can't add
    }

    @Override
    public boolean allowGlobalConditions() {
        return false;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        ExpressionVisitor v2 = visitor.incrementQueryLevel(1);
        for (Expression e : expressionArray) {
            if (!e.isEverything(v2)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void updateAggregate(Session s, int stage) {
        int columnCount = expressionArray.length;
        for (ArrayList<Expression> row : rows) {
            for (int i = 0; i < columnCount; i++) {
                row.get(i).updateAggregate(s, stage);
            }
        }
    }

    @Override
    public void fireBeforeSelectTriggers() {
        // Nothing to do
    }

    @Override
    public String getPlanSQL(boolean alwaysQuote) {
        StringBuilder builder = new StringBuilder();
        getValuesSQL(builder, alwaysQuote, rows);
        appendEndOfQueryToSQL(builder, alwaysQuote, expressions.toArray(new Expression[0]));
        return builder.toString();
    }

    @Override
    public Table toTable(String alias, ArrayList<Parameter> parameters, boolean forCreateView, Query topQuery) {
        if (!hasOrder() && offsetExpr == null && limitExpr == null) {
            return table;
        }
        return super.toTable(alias, parameters, forCreateView, topQuery);
    }

    private final class TableValueColumnResolver implements ColumnResolver {

        Value[] currentRow;

        TableValueColumnResolver() {
        }

        @Override
        public String getTableAlias() {
            return null;
        }

        @Override
        public Column[] getColumns() {
            return table.getColumns();
        }

        @Override
        public Column findColumn(String name) {
            return table.findColumn(name);
        }

        @Override
        public String getColumnName(Column column) {
            return column.getName();
        }

        @Override
        public boolean hasDerivedColumnList() {
            return false;
        }

        @Override
        public Column[] getSystemColumns() {
            return null;
        }

        @Override
        public Column getRowIdColumn() {
            return null;
        }

        @Override
        public String getSchemaName() {
            return null;
        }

        @Override
        public Value getValue(Column column) {
            return currentRow[column.getColumnId()];
        }

        @Override
        public TableFilter getTableFilter() {
            return null;
        }

        @Override
        public Select getSelect() {
            return null;
        }

        @Override
        public Expression optimize(ExpressionColumn expressionColumn, Column column) {
            return expressions.get(column.getColumnId());
        }

    }

}
