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

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionList;
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
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * Table value constructor.
 */
public class TableValueConstructor extends Query {

    private final ArrayList<ArrayList<Expression>> rows;

    /**
     * The table.
     */
    TableValueConstructorTable table;

    private TableValueColumnResolver columnResolver;

    private double cost;

    /**
     * Creates new instance of table value constructor.
     *
     * @param session
     *            the session
     * @param rows
     *            the rows
     */
    public TableValueConstructor(SessionLocal session, ArrayList<ArrayList<Expression>> rows) {
        super(session);
        this.rows = rows;
        if ((visibleColumnCount = rows.get(0).size()) > Constants.MAX_COLUMNS) {
            throw DbException.get(ErrorCode.TOO_MANY_COLUMNS_1, "" + Constants.MAX_COLUMNS);
        }
        for (ArrayList<Expression> row : rows) {
            for (Expression column : row) {
                if (!column.isConstant()) {
                    return;
                }
            }
        }
        createTable();
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
    public static void getVisibleResult(SessionLocal session, ResultTarget result, Column[] columns,
            ArrayList<ArrayList<Expression>> rows) {
        int count = columns.length;
        for (ArrayList<Expression> row : rows) {
            Value[] values = new Value[count];
            for (int i = 0; i < count; i++) {
                values[i] = row.get(i).getValue(session).convertTo(columns[i].getType(), session);
            }
            result.addRow(values);
        }
    }

    /**
     * Appends the SQL of the values to the specified string builder..
     *
     * @param builder
     *            string builder
     * @param sqlFlags
     *            formatting flags
     * @param rows
     *            the values
     */
    public static void getValuesSQL(StringBuilder builder, int sqlFlags, ArrayList<ArrayList<Expression>> rows) {
        builder.append("VALUES ");
        int rowCount = rows.size();
        for (int i = 0; i < rowCount; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            Expression.writeExpressions(builder.append('('), rows.get(i), sqlFlags).append(')');
        }
    }

    @Override
    public boolean isUnion() {
        return false;
    }

    @Override
    protected ResultInterface queryWithoutCache(long limit, ResultTarget target) {
        OffsetFetch offsetFetch = getOffsetFetch(limit);
        long offset = offsetFetch.offset;
        long fetch = offsetFetch.fetch;
        boolean fetchPercent = offsetFetch.fetchPercent;
        int visibleColumnCount = this.visibleColumnCount, resultColumnCount = this.resultColumnCount;
        LocalResult result = new LocalResult(session, expressionArray, visibleColumnCount, resultColumnCount);
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
                    values[i] = row.get(i).getValue(session).convertTo(columns[i].getType(), session);
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
            throw DbException.getInternalError();
        }
        checkInit = true;
        if (withTies && !hasOrder()) {
            throw DbException.get(ErrorCode.WITH_TIES_WITHOUT_ORDER_BY);
        }
    }

    @Override
    public void prepareExpressions() {
        if (columnResolver == null) {
            createTable();
        }
        if (orderList != null) {
            ArrayList<String> expressionsSQL = new ArrayList<>();
            for (Expression e : expressions) {
                expressionsSQL.add(e.getSQL(DEFAULT_SQL_FLAGS, WITHOUT_PARENTHESES));
            }
            if (initOrder(expressionsSQL, false, null)) {
                prepareOrder(orderList, expressions.size());
            }
        }
        resultColumnCount = expressions.size();
        for (int i = 0; i < resultColumnCount; i++) {
            expressions.get(i).mapColumns(columnResolver, 0, Expression.MAP_INITIAL);
        }
        for (int i = visibleColumnCount; i < resultColumnCount; i++) {
            expressions.set(i, expressions.get(i).optimize(session));
        }
        if (sort != null) {
            cleanupOrder();
        }
        expressionArray = expressions.toArray(new Expression[0]);
    }

    @Override
    public void preparePlan() {
        double cost = 0;
        int columnCount = visibleColumnCount;
        for (ArrayList<Expression> r : rows) {
            for (int i = 0; i < columnCount; i++) {
                cost += r.get(i).getCost();
            }
        }
        this.cost = cost + rows.size();
        isPrepared = true;
    }

    private void createTable() {
        int rowCount = rows.size();
        ArrayList<Expression> row = rows.get(0);
        int columnCount = row.size();
        TypeInfo[] types = new TypeInfo[columnCount];
        for (int c = 0; c < columnCount; c++) {
            Expression e = row.get(c).optimize(session);
            row.set(c, e);
            TypeInfo type = e.getType();
            if (type.getValueType() == Value.UNKNOWN) {
                type = TypeInfo.TYPE_VARCHAR;
            }
            types[c] = type;
        }
        for (int r = 1; r < rowCount; r++) {
            row = rows.get(r);
            for (int c = 0; c < columnCount; c++) {
                Expression e = row.get(c).optimize(session);
                row.set(c, e);
                types[c] = TypeInfo.getHigherType(types[c], e.getType());
            }
        }
        Column[] columns = new Column[columnCount];
        for (int c = 0; c < columnCount;) {
            TypeInfo type = types[c];
            columns[c] = new Column("C" + ++c, type);
        }
        Database database = session.getDatabase();
        ArrayList<Expression> expressions = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            expressions.add(new ExpressionColumn(database, null, null, columns[i].getName()));
        }
        this.expressions = expressions;
        table = new TableValueConstructorTable(database.getMainSchema(), session, columns, rows);
        columnResolver = new TableValueColumnResolver();
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
    public void setForUpdate(ForUpdate forUpdate) {
        throw DbException.get(ErrorCode.RESULT_SET_READONLY);
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, boolean outer) {
        int columnCount = visibleColumnCount;
        for (ArrayList<Expression> row : rows) {
            for (int i = 0; i < columnCount; i++) {
                row.get(i).mapColumns(resolver, level, Expression.MAP_INITIAL);
            }
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        int columnCount = visibleColumnCount;
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
    public void updateAggregate(SessionLocal s, int stage) {
        int columnCount = visibleColumnCount;
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
    public StringBuilder getPlanSQL(StringBuilder builder, int sqlFlags) {
        writeWithList(builder, sqlFlags);
        getValuesSQL(builder, sqlFlags, rows);
        appendEndOfQueryToSQL(builder, sqlFlags, expressionArray);
        return builder;
    }

    @Override
    public Table toTable(String alias, Column[] columnTemplates, ArrayList<Parameter> parameters,
            boolean forCreateView, Query topQuery) {
        if (!hasOrder() && offsetExpr == null && fetchExpr == null && table != null) {
            return table;
        }
        return super.toTable(alias, columnTemplates, parameters, forCreateView, topQuery);
    }

    @Override
    public boolean isConstantQuery() {
        if (!super.isConstantQuery()) {
            return false;
        }
        for (ArrayList<Expression> row : rows) {
            for (int i = 0; i < visibleColumnCount; i++) {
                if (!row.get(i).isConstant()) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public Expression getIfSingleRow() {
        if (offsetExpr != null || fetchExpr != null || rows.size() != 1) {
            return null;
        }
        ArrayList<Expression> row = rows.get(0);
        if (visibleColumnCount == 1) {
            return row.get(0);
        }
        Expression[] array = new Expression[visibleColumnCount];
        for (int i = 0; i < visibleColumnCount; i++) {
            array[i] = row.get(i);
        }
        return new ExpressionList(array, false);
    }

    private final class TableValueColumnResolver implements ColumnResolver {

        Value[] currentRow;

        TableValueColumnResolver() {
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
        public Value getValue(Column column) {
            return currentRow[column.getColumnId()];
        }

        @Override
        public Expression optimize(ExpressionColumn expressionColumn, Column column) {
            return expressions.get(column.getColumnId());
        }

    }

}
