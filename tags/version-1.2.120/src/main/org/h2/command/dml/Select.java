/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.Mode;
import org.h2.engine.Session;
import org.h2.expression.Alias;
import org.h2.expression.Comparison;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.Wildcard;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.New;
import org.h2.util.ObjectArray;
import org.h2.util.ObjectUtils;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.util.ValueHashMap;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;

/**
 * This class represents a simple SELECT statement.
 *
 * For each select statement,
 * visibleColumnCount &lt;= distinctColumnCount &lt;= expressionCount.
 * The expression list count could include ORDER BY and GROUP BY expressions
 * that are not in the select list.
 *
 * The call sequence is init(), mapColumns() if it's a subquery, prepare().
 *
 * @author Thomas Mueller
 * @author Joel Turkel (Group sorted query)
 */
public class Select extends Query {
    private TableFilter topTableFilter;
    private ObjectArray<TableFilter> filters = ObjectArray.newInstance();
    private ObjectArray<TableFilter> topFilters = ObjectArray.newInstance();
    private ObjectArray<Expression> expressions;
    private Expression having;
    private Expression condition;
    private int visibleColumnCount, distinctColumnCount;
    private ObjectArray<SelectOrderBy> orderList;
    private ObjectArray<Expression> group;
    private int[] groupIndex;
    private boolean[] groupByExpression;
    private boolean distinct;
    private HashMap<Expression, Object> currentGroup;
    private int havingIndex;
    private boolean isGroupQuery, isGroupSortedQuery;
    private boolean isForUpdate;
    private double cost;
    private boolean isQuickAggregateQuery, isDistinctQuery;
    private boolean isPrepared, checkInit;
    private boolean sortUsingIndex;
    private SortOrder sort;
    private int currentGroupRowId;

    public Select(Session session) {
        super(session);
    }

    /**
     * Add a table to the query.
     *
     * @param filter the table to add
     * @param isTop if the table can be the first table in the query plan
     */
    public void addTableFilter(TableFilter filter, boolean isTop) {
        // Oracle doesn't check on duplicate aliases
        // String alias = filter.getAlias();
        // if(filterNames.contains(alias)) {
        //     throw Message.getSQLException(
        //         ErrorCode.DUPLICATE_TABLE_ALIAS, alias);
        // }
        // filterNames.add(alias);
        filters.add(filter);
        if (isTop) {
            topFilters.add(filter);
        }
    }

    public ObjectArray<TableFilter> getTopFilters() {
        return topFilters;
    }

    public void setExpressions(ObjectArray<Expression> expressions) {
        this.expressions = expressions;
    }

    /**
     * Called if this query contains aggregate functions.
     */
    public void setGroupQuery() {
        isGroupQuery = true;
    }

    public void setGroupBy(ObjectArray<Expression> group) {
        this.group = group;
    }

    public HashMap<Expression, Object> getCurrentGroup() {
        return currentGroup;
    }

    public int getCurrentGroupRowId() {
        return currentGroupRowId;
    }

    public void setOrder(ObjectArray<SelectOrderBy> order) {
        orderList = order;
    }

    /**
     * Add a condition to the list of conditions.
     *
     * @param cond the condition to add
     */
    public void addCondition(Expression cond) {
        if (condition == null) {
            condition = cond;
        } else {
            condition = new ConditionAndOr(ConditionAndOr.AND, cond, condition);
        }
    }

    private void queryGroupSorted(int columnCount, LocalResult result) throws SQLException {
        int rowNumber = 0;
        setCurrentRowNumber(0);
        Value[] previousKeyValues = null;
        while (topTableFilter.next()) {
            setCurrentRowNumber(rowNumber + 1);
            if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                rowNumber++;
                Value[] keyValues = new Value[groupIndex.length];
                // update group
                for (int i = 0; i < groupIndex.length; i++) {
                    int idx = groupIndex[i];
                    Expression expr = expressions.get(idx);
                    keyValues[i] = expr.getValue(session);
                }

                if (previousKeyValues == null) {
                    previousKeyValues = keyValues;
                    currentGroup = New.hashMap();
                } else if (!Arrays.equals(previousKeyValues, keyValues)) {
                    addGroupSortedRow(previousKeyValues, columnCount, result);
                    previousKeyValues = keyValues;
                    currentGroup = New.hashMap();
                }
                currentGroupRowId++;

                for (int i = 0; i < columnCount; i++) {
                    if (groupByExpression == null || !groupByExpression[i]) {
                        Expression expr = expressions.get(i);
                        expr.updateAggregate(session);
                    }
                }
            }
        }
        if (previousKeyValues != null) {
            addGroupSortedRow(previousKeyValues, columnCount, result);
        }
    }

    private void addGroupSortedRow(Value[] keyValues, int columnCount, LocalResult result) throws SQLException {
        Value[] row = new Value[columnCount];
        for (int j = 0; groupIndex != null && j < groupIndex.length; j++) {
            row[groupIndex[j]] = keyValues[j];
        }
        for (int j = 0; j < columnCount; j++) {
            if (groupByExpression != null && groupByExpression[j]) {
                continue;
            }
            Expression expr = expressions.get(j);
            row[j] = expr.getValue(session);
        }
        if (isHavingNullOrFalse(row)) {
            return;
        }
        row = keepOnlyDistinct(row, columnCount);
        result.addRow(row);
    }

    private Value[] keepOnlyDistinct(Value[] row, int columnCount) {
        if (columnCount == distinctColumnCount) {
            return row;
        }
        // remove columns so that 'distinct' can filter duplicate rows
        Value[] r2 = new Value[distinctColumnCount];
        ObjectUtils.arrayCopy(row, r2, distinctColumnCount);
        return r2;
    }

    private boolean isHavingNullOrFalse(Value[] row) throws SQLException {
        if (havingIndex >= 0) {
            Value v = row[havingIndex];
            if (v == ValueNull.INSTANCE) {
                return true;
            }
            if (!Boolean.TRUE.equals(v.getBoolean())) {
                return true;
            }
        }
        return false;
    }

    private Index getGroupSortedIndex() {
        if (groupIndex == null || groupByExpression == null) {
            return null;
        }
        ObjectArray<Index> indexes = topTableFilter.getTable().getIndexes();
        for (int i = 0; indexes != null && i < indexes.size(); i++) {
            Index index = indexes.get(i);
            if (index.getIndexType().isScan()) {
                continue;
            }
            if (isGroupSortedIndex(topTableFilter, index)) {
                return index;
            }
        }
        return null;
    }

    private boolean isGroupSortedIndex(TableFilter tableFilter, Index index) {
        // check that all the GROUP BY expressions are part of the index
        Column[] indexColumns = index.getColumns();
        // also check that the first columns in the index are grouped
        boolean[] grouped = new boolean[indexColumns.length];
        outerLoop:
        for (int i = 0; i < expressions.size(); i++) {
            if (!groupByExpression[i]) {
                continue;
            }
            Expression expr = expressions.get(i);
            if (!(expr instanceof ExpressionColumn)) {
                return false;
            }
            ExpressionColumn exprCol = (ExpressionColumn) expr;
            for (int j = 0; j < indexColumns.length; ++j) {
                if (tableFilter == exprCol.getTableFilter()) {
                    if (indexColumns[j].equals(exprCol.getColumn())) {
                        grouped[j] = true;
                        continue outerLoop;
                    }
                }
            }
            // We didn't find a matching index column
            // for one group by expression
            return false;
        }
        // check that the first columns in the index are grouped
        // good: index(a, b, c); group by b, a
        // bad: index(a, b, c); group by a, c
        for (int i = 1; i < grouped.length; i++) {
            if (!grouped[i - 1] && grouped[i]) {
                return false;
            }
        }
        return true;
    }

    private int getGroupByExpressionCount() {
        if (groupByExpression == null) {
            return 0;
        }
        int count = 0;
        for (boolean b : groupByExpression) {
            if (b) {
                ++count;
            }
        }
        return count;
    }

    private void queryGroup(int columnCount, LocalResult result) throws SQLException {
        ValueHashMap<HashMap<Expression, Object>> groups = ValueHashMap.newInstance(session.getDatabase());
        int rowNumber = 0;
        setCurrentRowNumber(0);
        ValueArray defaultGroup = ValueArray.get(new Value[0]);
        while (topTableFilter.next()) {
            setCurrentRowNumber(rowNumber + 1);
            if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                Value key;
                rowNumber++;
                if (groupIndex == null) {
                    key = defaultGroup;
                } else {
                    Value[] keyValues = new Value[groupIndex.length];
                    // update group
                    for (int i = 0; i < groupIndex.length; i++) {
                        int idx = groupIndex[i];
                        Expression expr = expressions.get(idx);
                        keyValues[i] = expr.getValue(session);
                    }
                    key = ValueArray.get(keyValues);
                }
                HashMap<Expression, Object> values = groups.get(key);
                if (values == null) {
                    values = new HashMap<Expression, Object>();
                    groups.put(key, values);
                }
                currentGroup = values;
                currentGroupRowId++;
                int len = columnCount;
                for (int i = 0; i < len; i++) {
                    if (groupByExpression == null || !groupByExpression[i]) {
                        Expression expr = expressions.get(i);
                        expr.updateAggregate(session);
                    }
                }
                if (sampleSize > 0 && rowNumber >= sampleSize) {
                    break;
                }
            }
        }
        if (groupIndex == null && groups.size() == 0) {
            groups.put(defaultGroup, new HashMap<Expression, Object>());
        }
        ObjectArray<Value> keys = groups.keys();
        for (Value v : keys) {
            ValueArray key = (ValueArray) v;
            currentGroup = groups.get(key);
            Value[] keyValues = key.getList();
            Value[] row = new Value[columnCount];
            for (int j = 0; groupIndex != null && j < groupIndex.length; j++) {
                row[groupIndex[j]] = keyValues[j];
            }
            for (int j = 0; j < columnCount; j++) {
                if (groupByExpression != null && groupByExpression[j]) {
                    continue;
                }
                Expression expr = expressions.get(j);
                row[j] = expr.getValue(session);
            }
            if (isHavingNullOrFalse(row)) {
                continue;
            }
            row = keepOnlyDistinct(row, columnCount);
            result.addRow(row);
        }
    }

    /**
     * Get the index that matches the ORDER BY list, if one exists. This is to
     * avoid running a separate ORDER BY if an index can be used. This is
     * specially important for large result sets, if only the first few rows are
     * important (LIMIT is used)
     *
     * @return the index if one is found
     */
    private Index getSortIndex() throws SQLException {
        if (sort == null) {
            return null;
        }
        ObjectArray<Column> sortColumns = ObjectArray.newInstance();
        for (int idx : sort.getIndexes()) {
            if (idx < 0 || idx >= expressions.size()) {
                throw Message.getInvalidValueException("" + (idx + 1), "ORDER BY");
            }
            Expression expr = expressions.get(idx);
            expr = expr.getNonAliasExpression();
            if (expr.isConstant()) {
                continue;
            }
            if (!(expr instanceof ExpressionColumn)) {
                return null;
            }
            ExpressionColumn exprCol = (ExpressionColumn) expr;
            if (exprCol.getTableFilter() != topTableFilter) {
                return null;
            }
            sortColumns.add(exprCol.getColumn());
        }
        Column[] sortCols = new Column[sortColumns.size()];
        sortColumns.toArray(sortCols);
        int[] sortTypes = sort.getSortTypes();
        if (sortCols.length == 0) {
            // sort just on constants - can use scan index
            return topTableFilter.getTable().getScanIndex(session);
        }
        ObjectArray<Index> list = topTableFilter.getTable().getIndexes();
        for (int i = 0; list != null && i < list.size(); i++) {
            Index index = list.get(i);
            if (index.getCreateSQL() == null) {
                // can't use the scan index
                continue;
            }
            if (index.getIndexType().isHash()) {
                continue;
            }
            IndexColumn[] indexCols = index.getIndexColumns();
            if (indexCols.length < sortCols.length) {
                continue;
            }
            boolean ok = true;
            for (int j = 0; j < sortCols.length; j++) {
                // the index and the sort order must start
                // with the exact same columns
                IndexColumn idxCol = indexCols[j];
                Column sortCol = sortCols[j];
                if (idxCol.column != sortCol) {
                    ok = false;
                    break;
                }
                if (idxCol.sortType != sortTypes[j]) {
                    // NULL FIRST for ascending and NULLS LAST
                    // for descending would actually match the default
                    ok = false;
                    break;
                }
            }
            if (ok) {
                return index;
            }
        }
        return null;
    }

    private void queryDistinct(LocalResult result, long limitRows) throws SQLException {
        if (limitRows != 0 && offsetExpr != null) {
            // limitRows must be long, otherwise we get an int overflow
            // if limitRows is at or near Integer.MAX_VALUE
            limitRows += offsetExpr.getValue(session).getInt();
        }
        int rowNumber = 0;
        setCurrentRowNumber(0);
        Index index = topTableFilter.getIndex();
        SearchRow first = null;
        int columnIndex = index.getColumns()[0].getColumnId();
        while (true) {
            setCurrentRowNumber(rowNumber + 1);
            Cursor cursor = index.findNext(session, first, null);
            if (!cursor.next()) {
                break;
            }
            SearchRow found = cursor.getSearchRow();
            Value value = found.getValue(columnIndex);
            if (first == null) {
                first = topTableFilter.getTable().getTemplateSimpleRow(true);
            }
            first.setValue(columnIndex, value);
            Value[] row = new Value[1];
            row[0] = value;
            result.addRow(row);
            rowNumber++;
            if ((sort == null || sortUsingIndex) && limitRows != 0 && result.getRowCount() >= limitRows) {
                break;
            }
            if (sampleSize > 0 && rowNumber >= sampleSize) {
                break;
            }
        }
    }

    private void queryFlat(int columnCount, LocalResult result, long limitRows) throws SQLException {
        if (limitRows != 0 && offsetExpr != null) {
            // limitRows must be long, otherwise we get an int overflow
            // if limitRows is at or near Integer.MAX_VALUE
            limitRows += offsetExpr.getValue(session).getInt();
        }
        int rowNumber = 0;
        setCurrentRowNumber(0);
        while (topTableFilter.next()) {
            setCurrentRowNumber(rowNumber + 1);
            if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                Value[] row = new Value[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    Expression expr = expressions.get(i);
                    row[i] = expr.getValue(session);
                }
                result.addRow(row);
                rowNumber++;
                if ((sort == null || sortUsingIndex) && limitRows != 0 && result.getRowCount() >= limitRows) {
                    break;
                }
                if (sampleSize > 0 && rowNumber >= sampleSize) {
                    break;
                }
            }
        }
    }

    private void queryQuick(int columnCount, LocalResult result) throws SQLException {
        Value[] row = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Expression expr = expressions.get(i);
            row[i] = expr.getValue(session);
        }
        result.addRow(row);
    }

    public LocalResult queryMeta() throws SQLException {
        LocalResult result = new LocalResult(session, expressions, visibleColumnCount);
        result.done();
        return result;
    }

    protected LocalResult queryWithoutCache(int maxRows) throws SQLException {
        int limitRows = maxRows;
        if (limitExpr != null) {
            int l = limitExpr.getValue(session).getInt();
            if (limitRows == 0) {
                limitRows = l;
            } else {
                limitRows = Math.min(l, limitRows);
            }
        }
        int columnCount = expressions.size();
        LocalResult result = new LocalResult(session, expressions, visibleColumnCount);
        if (!sortUsingIndex) {
            result.setSortOrder(sort);
        }
        if (distinct && !isDistinctQuery) {
            result.setDistinct();
        }
        topTableFilter.startQuery(session);
        topTableFilter.reset();
        topTableFilter.lock(session, isForUpdate, isForUpdate);
        if (isQuickAggregateQuery) {
            queryQuick(columnCount, result);
        } else if (isGroupQuery) {
            if (isGroupSortedQuery) {
                queryGroupSorted(columnCount, result);
            } else {
                queryGroup(columnCount, result);
            }
        } else if (isDistinctQuery) {
            queryDistinct(result, limitRows);
        } else {
            queryFlat(columnCount, result, limitRows);
        }
        if (offsetExpr != null) {
            result.setOffset(offsetExpr.getValue(session).getInt());
        }
        if (limitRows != 0) {
            result.setLimit(limitRows);
        }
        result.done();
        return result;
    }

    private void expandColumnList() throws SQLException {
        for (int i = 0; i < expressions.size(); i++) {
            Expression expr = expressions.get(i);
            if (!expr.isWildcard()) {
                continue;
            }
            String schemaName = expr.getSchemaName();
            String tableAlias = expr.getTableAlias();
            if (tableAlias == null) {
                int temp = i;
                expressions.remove(i);
                for (TableFilter filter : filters) {
                    Wildcard c2 = new Wildcard(filter.getTable().getSchema().getName(), filter.getTableAlias());
                    expressions.add(i++, c2);
                }
                i = temp - 1;
            } else {
                TableFilter filter = null;
                for (TableFilter f : filters) {
                    if (tableAlias.equals(f.getTableAlias())) {
                        if (schemaName == null || schemaName.equals(f.getSchemaName())) {
                            filter = f;
                            break;
                        }
                    }
                }
                if (filter == null) {
                    throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableAlias);
                }
                Table t = filter.getTable();
                String alias = filter.getTableAlias();
                expressions.remove(i);
                Column[] columns = t.getColumns();
                for (Column c : columns) {
                    if (filter.isNaturalJoinColumn(c)) {
                        continue;
                    }
                    ExpressionColumn ec = new ExpressionColumn(session.getDatabase(), null, alias, c.getName());
                    expressions.add(i++, ec);
                }
                i--;
            }
        }
    }

    public void init() throws SQLException {
        if (SysProperties.CHECK && checkInit) {
            Message.throwInternalError();
        }
        expandColumnList();
        visibleColumnCount = expressions.size();
        ObjectArray<String> expressionSQL;
        if (orderList != null || group != null) {
            expressionSQL = ObjectArray.newInstance();
            for (int i = 0; i < visibleColumnCount; i++) {
                Expression expr = expressions.get(i);
                expr = expr.getNonAliasExpression();
                String sql = expr.getSQL();
                expressionSQL.add(sql);
            }
        } else {
            expressionSQL = null;
        }
        if (orderList != null) {
            initOrder(expressions, expressionSQL, orderList, visibleColumnCount, distinct);
        }
        distinctColumnCount = expressions.size();
        if (having != null) {
            expressions.add(having);
            havingIndex = expressions.size() - 1;
            having = null;
        } else {
            havingIndex = -1;
        }

        // first the select list (visible columns),
        // then 'ORDER BY' expressions,
        // then 'HAVING' expressions,
        // and 'GROUP BY' expressions at the end
        if (group != null) {
            groupIndex = new int[group.size()];
            for (int i = 0; i < group.size(); i++) {
                Expression expr = group.get(i);
                String sql = expr.getSQL();
                int found = -1;
                for (int j = 0; j < expressionSQL.size(); j++) {
                    String s2 = expressionSQL.get(j);
                    if (s2.equals(sql)) {
                        found = j;
                        break;
                    }
                }
                if (found < 0) {
                    // special case: GROUP BY a column alias
                    for (int j = 0; j < expressionSQL.size(); j++) {
                        Expression e = expressions.get(j);
                        if (sql.equals(e.getAlias())) {
                            found = j;
                            break;
                        }
                    }
                }
                if (found < 0) {
                    int index = expressions.size();
                    groupIndex[i] = index;
                    expressions.add(expr);
                } else {
                    groupIndex[i] = found;
                }
            }
            groupByExpression = new boolean[expressions.size()];
            for (int gi : groupIndex) {
                groupByExpression[gi] = true;
            }
            group = null;
        }
        // map columns in select list and condition
        for (TableFilter f : filters) {
            for (Expression expr : expressions) {
                expr.mapColumns(f, 0);
            }
            if (condition != null) {
                condition.mapColumns(f, 0);
            }
        }
        if (havingIndex >= 0) {
            Expression expr = expressions.get(havingIndex);
            SelectListColumnResolver res = new SelectListColumnResolver(this);
            expr.mapColumns(res, 0);
        }
        checkInit = true;
    }

    public void prepare() throws SQLException {
        if (isPrepared) {
            // sometimes a subquery is prepared twice (CREATE TABLE AS SELECT)
            return;
        }
        if (SysProperties.CHECK && !checkInit) {
            Message.throwInternalError("not initialized");
        }
        if (orderList != null) {
            sort = prepareOrder(orderList, expressions.size());
            orderList = null;
        }
        for (int i = 0; i < expressions.size(); i++) {
            Expression e = expressions.get(i);
            expressions.set(i, e.optimize(session));
        }
        if (condition != null) {
            condition = condition.optimize(session);
            if (SysProperties.optimizeInJoin) {
                condition = condition.optimizeInJoin(session, this);
            }
            for (TableFilter f : filters) {
                condition.createIndexConditions(session, f);
            }
        }
        if (isGroupQuery && groupIndex == null && havingIndex < 0 && filters.size() == 1) {
            if (condition == null) {
                ExpressionVisitor optimizable = ExpressionVisitor.get(ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL);
                optimizable.setTable((filters.get(0)).getTable());
                isQuickAggregateQuery = isEverything(optimizable);
            }
        }
        cost = preparePlan();
        if (SysProperties.OPTIMIZE_DISTINCT && distinct && !isGroupQuery && filters.size() == 1 && expressions.size() == 1 && condition == null) {
            Expression expr = expressions.get(0);
            expr = expr.getNonAliasExpression();
            if (expr instanceof ExpressionColumn) {
                Column column = ((ExpressionColumn) expr).getColumn();
                int selectivity = column.getSelectivity();
                Index columnIndex = topTableFilter.getTable().getIndexForColumn(column, true);
                if (columnIndex != null && selectivity != Constants.SELECTIVITY_DEFAULT && selectivity < 20) {
                    // the first column must be ascending
                    boolean ascending = columnIndex.getIndexColumns()[0].sortType == SortOrder.ASCENDING;
                    Index current = topTableFilter.getIndex();
                    // if another index is faster
                    if (columnIndex.canFindNext() && ascending && (current == null || current.getIndexType().isScan() || columnIndex == current)) {
                        IndexType type = columnIndex.getIndexType();
                        // hash indexes don't work, and unique single column indexes don't work
                        if (!type.isHash() && (!type.isUnique() || columnIndex.getColumns().length > 1)) {
                            topTableFilter.setIndex(columnIndex);
                            isDistinctQuery = true;
                        }
                    }
                }
            }
        }
        if (sort != null && !isQuickAggregateQuery && !isGroupQuery) {
            Index index = getSortIndex();
            Index current = topTableFilter.getIndex();
            if (index != null && (current.getIndexType().isScan() || current == index)) {
                topTableFilter.setIndex(index);
                if (!distinct || isDistinctQuery) {
                    // sort using index would not work correctly for distinct result sets
                    // because it would break too early when limit is used
                    sortUsingIndex = true;
                }
            }
        }
        if (SysProperties.OPTIMIZE_GROUP_SORTED && !isQuickAggregateQuery && isGroupQuery && getGroupByExpressionCount() > 0) {
            Index index = getGroupSortedIndex();
            Index current = topTableFilter.getIndex();
            if (index != null && (current.getIndexType().isScan() || current == index)) {
                topTableFilter.setIndex(index);
                isGroupSortedQuery = true;
            }
        }
        isPrepared = true;
    }

    public double getCost() {
        return cost;
    }

    public HashSet<Table> getTables() {
        HashSet<Table> set = New.hashSet();
        for (TableFilter filter : filters) {
            set.add(filter.getTable());
        }
        return set;
    }

    private double preparePlan() throws SQLException {
        TableFilter[] topArray = new TableFilter[topFilters.size()];
        topFilters.toArray(topArray);
        for (TableFilter t : topArray) {
            t.setFullCondition(condition);
        }

        Optimizer optimizer = new Optimizer(topArray, condition, session);
        optimizer.optimize();
        topTableFilter = optimizer.getTopFilter();
        double planCost = optimizer.getCost();

        TableFilter f = topTableFilter;
        while (f != null) {
            f.setEvaluatable(f, true);
            if (condition != null) {
                condition.setEvaluatable(f, true);
            }
            Expression on = f.getJoinCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE)) {
                    if (f.isJoinOuter()) {
                        // this will check if all columns exist - it may or may not throw an exception
                        on = on.optimize(session);
                        // it is not supported even if the columns exist
                        throw Message.getSQLException(ErrorCode.UNSUPPORTED_OUTER_JOIN_CONDITION_1, on.getSQL());
                    }
                    f.removeJoinCondition();
                    // need to check that all added are bound to a table
                    on = on.optimize(session);
                    addCondition(on);
                }
            }
            on = f.getFilterCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE)) {
                    f.removeFilterCondition();
                    addCondition(on);
                }
            }
            // this is only important for subqueries, so they know
            // the result columns are evaluatable
            for (Expression e : expressions) {
                e.setEvaluatable(f, true);
            }
            f = f.getJoin();
        }
        topTableFilter.prepare();
        return planCost;
    }

    public String getPlanSQL() {
        // can not use the field sqlStatement because the parameter
        // indexes may be incorrect: ? may be in fact ?2 for a subquery
        // but indexes may be set manually as well
        Expression[] exprList = new Expression[expressions.size()];
        expressions.toArray(exprList);
        StatementBuilder buff = new StatementBuilder("SELECT ");
        if (distinct) {
            buff.append("DISTINCT ");
        }
        for (int i = 0; i < visibleColumnCount; i++) {
            buff.appendExceptFirst(", ");
            buff.append(exprList[i].getSQL());
        }
        buff.append("\nFROM ");
        TableFilter filter = topTableFilter;
        if (filter != null) {
            buff.resetCount();
            int i = 0;
            do {
                buff.appendExceptFirst("\n");
                buff.append(filter.getPlanSQL(i++ > 0));
                filter = filter.getJoin();
            } while (filter != null);
        } else {
            buff.resetCount();
            int i = 0;
            for (TableFilter f : filters) {
                buff.appendExceptFirst("\n");
                buff.append(f.getPlanSQL(i++ > 0));
            }
        }
        if (condition != null) {
            buff.append("\nWHERE ").append(StringUtils.unEnclose(condition.getSQL()));
        }
        if (groupIndex != null) {
            buff.append("\nGROUP BY ");
            buff.resetCount();
            for (int gi : groupIndex) {
                Expression g = exprList[gi];
                g = g.getNonAliasExpression();
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(g.getSQL()));
            }
        }
        if (group != null) {
            buff.append("\nGROUP BY ");
            buff.resetCount();
            for (Expression g : group) {
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(g.getSQL()));
            }
        }
        if (having != null) {
            // could be set in addGlobalCondition
            // in this case the query is not run directly, just getPlanSQL is
            // called
            Expression h = having;
            buff.append("\nHAVING ").append(StringUtils.unEnclose(h.getSQL()));
        } else if (havingIndex >= 0) {
            Expression h = exprList[havingIndex];
            buff.append("\nHAVING ").append(StringUtils.unEnclose(h.getSQL()));
        }
        if (sort != null) {
            buff.append("\nORDER BY ").append(sort.getSQL(exprList, visibleColumnCount));
        }
        if (orderList != null) {
            buff.append("\nORDER BY ");
            buff.resetCount();
            for (SelectOrderBy o : orderList) {
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(o.getSQL()));
            }
        }
        if (limitExpr != null) {
            buff.append("\nLIMIT ").append(StringUtils.unEnclose(limitExpr.getSQL()));
            if (offsetExpr != null) {
                buff.append(" OFFSET ").append(StringUtils.unEnclose(offsetExpr.getSQL()));
            }
        }
        if (isForUpdate) {
            buff.append("\nFOR UPDATE");
        }
        if (isQuickAggregateQuery) {
            buff.append("\n/* direct lookup */");
        }
        if (isDistinctQuery) {
            buff.append("\n/* distinct */");
        }
        if (sortUsingIndex) {
            buff.append("\n/* index sorted */");
        }
        if (isGroupQuery) {
            if (isGroupSortedQuery) {
                buff.append("\n/* group sorted */");
            }
        }
        return buff.toString();
    }

    public void setDistinct(boolean b) {
        distinct = b;
    }

    public void setHaving(Expression having) {
        this.having = having;
    }

    public int getColumnCount() {
        return visibleColumnCount;
    }

    public TableFilter getTopTableFilter() {
        return topTableFilter;
    }

    public ObjectArray<Expression> getExpressions() {
        return expressions;
    }

    public void setForUpdate(boolean b) {
        this.isForUpdate = b;
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        for (Expression e : expressions) {
            e.mapColumns(resolver, level);
        }
        if (condition != null) {
            condition.mapColumns(resolver, level);
        }
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (Expression e : expressions) {
            e.setEvaluatable(tableFilter, b);
        }
        if (condition != null) {
            condition.setEvaluatable(tableFilter, b);
        }
    }

    /**
     * Check if this is an aggregate query with direct lookup, for example a
     * query of the type SELECT COUNT(*) FROM TEST or
     * SELECT MAX(ID) FROM TEST.
     *
     * @return true if a direct lookup is possible
     */
    public boolean isQuickAggregateQuery() {
        return isQuickAggregateQuery;
    }

    public void addGlobalCondition(Parameter param, int columnId, int comparisonType) throws SQLException {
        addParameter(param);
        Expression col = expressions.get(columnId);
        col = col.getNonAliasExpression();
        Expression comp = new Comparison(session, comparisonType, col, param);
        comp = comp.optimize(session);
        boolean addToCondition = true;
        if (isGroupQuery) {
            addToCondition = false;
            for (int i = 0; groupIndex != null && i < groupIndex.length; i++) {
                if (groupIndex[i] == columnId) {
                    addToCondition = true;
                    break;
                }
            }
            if (!addToCondition) {
                if (havingIndex >= 0) {
                    having = expressions.get(havingIndex);
                }
                if (having == null) {
                    having = comp;
                } else {
                    having = new ConditionAndOr(ConditionAndOr.AND, having, comp);
                }
            }
        }
        if (addToCondition) {
            if (condition == null) {
                condition = comp;
            } else {
                condition = new ConditionAndOr(ConditionAndOr.AND, condition, comp);
            }
        }
    }

    public void updateAggregate(Session s) throws SQLException {
        for (Expression e : expressions) {
            e.updateAggregate(s);
        }
        if (condition != null) {
            condition.updateAggregate(s);
        }
        if (having != null) {
            having.updateAggregate(s);
        }
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        switch(visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC: {
            for (TableFilter f : filters) {
                if (!f.getTable().isDeterministic()) {
                    return false;
                }
            }
            break;
        }
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID: {
            for (TableFilter f : filters) {
                long m = f.getTable().getMaxDataModificationId();
                visitor.addDataModificationId(m);
            }
            break;
        }
        case ExpressionVisitor.EVALUATABLE: {
            if (!SysProperties.OPTIMIZE_EVALUATABLE_SUBQUERIES) {
                return false;
            }
            break;
        }
        case ExpressionVisitor.GET_DEPENDENCIES: {
            for (TableFilter filter : filters) {
                Table table = filter.getTable();
                visitor.addDependency(table);
                table.addDependencies(visitor.getDependencies());
            }
            break;
        }
        default:
        }
        visitor.incrementQueryLevel(1);
        boolean result = true;
        for (Expression e : expressions) {
            if (!e.isEverything(visitor)) {
                result = false;
                break;
            }
        }
        if (result && condition != null && !condition.isEverything(visitor)) {
            result = false;
        }
        if (result && having != null && !having.isEverything(visitor)) {
            result = false;
        }
        visitor.incrementQueryLevel(-1);
        return result;
    }

    public boolean isReadOnly() {
        return isEverything(ExpressionVisitor.READONLY);
    }

    public String getFirstColumnAlias(Session s) {
        if (SysProperties.CHECK) {
            if (visibleColumnCount > 1) {
                Message.throwInternalError("" + visibleColumnCount);
            }
        }
        Expression expr = expressions.get(0);
        if (expr instanceof Alias) {
            return expr.getAlias();
        }
        Mode mode = s.getDatabase().getMode();
        String name = s.getNextSystemIdentifier(getSQL());
        expr = new Alias(expr,  name, mode.aliasColumnName);
        expressions.set(0, expr);
        return expr.getAlias();
    }

}
