/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
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
import org.h2.util.ObjectArray;
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
    private ObjectArray filters = new ObjectArray();
    private ObjectArray topFilters = new ObjectArray();
    private ObjectArray expressions;
    private Expression having;
    private Expression condition;
    private int visibleColumnCount, distinctColumnCount;
    private ObjectArray orderList;
    private ObjectArray group;
    private int[] groupIndex;
    private boolean[] groupByExpression;
    private boolean distinct;
    private HashMap currentGroup;
    private int havingIndex;
    private boolean isGroupQuery, isGroupSortedQuery;
    private boolean isForUpdate;
    private double cost;
    private boolean isQuickAggregateQuery, isDistinctQuery;
    private boolean isPrepared, checkInit;
    private boolean sortUsingIndex;
    private SortOrder sort;

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
        // TODO compatibility: it seems oracle doesn't check on 
        // duplicate aliases; do other databases check it?
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

    public ObjectArray getTopFilters() {
        return topFilters;
    }

    public void setExpressions(ObjectArray expressions) {
        this.expressions = expressions;
    }

    /**
     * Called if this query contains aggregate functions.
     */
    public void setGroupQuery() {
        isGroupQuery = true;
    }

    public void setGroupBy(ObjectArray group) {
        this.group = group;
    }

    public HashMap getCurrentGroup() {
        return currentGroup;
    }

    public void setOrder(ObjectArray order) {
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
            checkCancelled();
            setCurrentRowNumber(rowNumber + 1);
            if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                rowNumber++;
                Value[] keyValues = new Value[groupIndex.length];
                // update group
                for (int i = 0; i < groupIndex.length; i++) {
                    int idx = groupIndex[i];
                    Expression expr = (Expression) expressions.get(idx);
                    keyValues[i] = expr.getValue(session);
                }

                if (previousKeyValues == null) {
                    previousKeyValues = keyValues;
                    currentGroup = new HashMap();
                } else if (!Arrays.equals(previousKeyValues, keyValues)) {
                    addGroupSortedRow(previousKeyValues, columnCount, result);
                    previousKeyValues = keyValues;
                    currentGroup = new HashMap();
                }

                for (int i = 0; i < columnCount; i++) {
                    if (groupByExpression == null || !groupByExpression[i]) {
                        Expression expr = (Expression) expressions.get(i);
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
            Expression expr = (Expression) expressions.get(j);
            row[j] = expr.getValue(session);
        }
        if (havingIndex > 0) {
            Value v = row[havingIndex];
            if (v == ValueNull.INSTANCE) {
                return;
            }
            if (!Boolean.TRUE.equals(v.getBoolean())) {
                return;
            }
        }
        if (columnCount != distinctColumnCount) {
            // remove columns so that 'distinct' can filter duplicate rows
            Value[] r2 = new Value[distinctColumnCount];
            System.arraycopy(row, 0, r2, 0, distinctColumnCount);
            row = r2;
        }
        result.addRow(row);
    }

    private Index getGroupSortedIndex() {
        if (groupIndex == null || groupByExpression == null) {
            return null;
        }
        ObjectArray indexes = topTableFilter.getTable().getIndexes();
        for (int i = 0; indexes != null && i < indexes.size(); i++) {
            Index index = (Index) indexes.get(i);
            if (index.getIndexType().isScan()) {
                continue;
            }
            if (isGroupSortedIndex(index)) {
                return index;
            }
        }
        return null;
    }

    private boolean isGroupSortedIndex(Index index) {
        Column[] indexColumns = index.getColumns();
        outerLoop: 
        for (int i = 0; i < expressions.size(); i++) {
            if (!groupByExpression[i]) {
                continue;
            }
            Expression expr = (Expression) expressions.get(i);
            if (!(expr instanceof ExpressionColumn)) {
                return false;
            }
            ExpressionColumn exprCol = (ExpressionColumn) expr;
            for (int j = 0; j < indexColumns.length; ++j) {
                if (indexColumns[j].equals(exprCol.getColumn())) {
                    continue outerLoop;
                }
            }
            // We didn't find a matching index column for the group by
            // expression
            return false;
        }
        return true;
    }

    private int getGroupByExpressionCount() {
        if (groupByExpression == null) {
            return 0;
        }
        int count = 0;
        for (int i = 0; i < groupByExpression.length; i++) {
            if (groupByExpression[i]) {
                ++count;
            }
        }
        return count;
    }

    private void queryGroup(int columnCount, LocalResult result) throws SQLException {
        ValueHashMap groups = new ValueHashMap(session.getDatabase());
        int rowNumber = 0;
        setCurrentRowNumber(0);
        ValueArray defaultGroup = ValueArray.get(new Value[0]);
        while (topTableFilter.next()) {
            checkCancelled();
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
                        Expression expr = (Expression) expressions.get(idx);
                        keyValues[i] = expr.getValue(session);
                    }
                    key = ValueArray.get(keyValues);
                }
                HashMap values = (HashMap) groups.get(key);
                if (values == null) {
                    values = new HashMap();
                    groups.put(key, values);
                }
                currentGroup = values;
                int len = columnCount;
                for (int i = 0; i < len; i++) {
                    if (groupByExpression == null || !groupByExpression[i]) {
                        Expression expr = (Expression) expressions.get(i);
                        expr.updateAggregate(session);
                    }
                }
                if (sampleSize > 0 && rowNumber >= sampleSize) {
                    break;
                }
            }
        }
        if (groupIndex == null && groups.size() == 0) {
            groups.put(defaultGroup, new HashMap());
        }
        ObjectArray keys = groups.keys();
        for (int i = 0; i < keys.size(); i++) {
            ValueArray key = (ValueArray) keys.get(i);
            currentGroup = (HashMap) groups.get(key);
            Value[] keyValues = key.getList();
            Value[] row = new Value[columnCount];
            for (int j = 0; groupIndex != null && j < groupIndex.length; j++) {
                row[groupIndex[j]] = keyValues[j];
            }
            for (int j = 0; j < columnCount; j++) {
                if (groupByExpression != null && groupByExpression[j]) {
                    continue;
                }
                Expression expr = (Expression) expressions.get(j);
                row[j] = expr.getValue(session);
            }
            if (havingIndex > 0) {
                Value v = row[havingIndex];
                if (v == ValueNull.INSTANCE) {
                    continue;
                }
                if (!Boolean.TRUE.equals(v.getBoolean())) {
                    continue;
                }
            }
            if (columnCount != distinctColumnCount) {
                // remove columns so that 'distinct' can filter duplicate rows
                Value[] r2 = new Value[distinctColumnCount];
                System.arraycopy(row, 0, r2, 0, distinctColumnCount);
                row = r2;
            }
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
        int[] indexes = sort.getIndexes();
        ObjectArray sortColumns = new ObjectArray();
        for (int i = 0; i < indexes.length; i++) {
            int idx = indexes[i];
            if (idx < 0 || idx >= expressions.size()) {
                throw Message.getInvalidValueException("" + (idx + 1), "ORDER BY");
            }
            Expression expr = (Expression) expressions.get(idx);
            expr = expr.getNonAliasExpression();
            if (expr.isConstant()) {
                continue;
            }
            if (!(expr instanceof ExpressionColumn)) {
                return null;
            }
            Column col = ((ExpressionColumn) expr).getColumn();
            if (col.getTable() != topTableFilter.getTable()) {
                return null;
            }
            sortColumns.add(col);
        }
        Column[] sortCols = new Column[sortColumns.size()];
        sortColumns.toArray(sortCols);
        int[] sortTypes = sort.getSortTypes();
        if (sortCols.length == 0) {
            // sort just on constants - can use scan index
            return topTableFilter.getTable().getScanIndex(session);
        }
        ObjectArray list = topTableFilter.getTable().getIndexes();
        for (int i = 0; list != null && i < list.size(); i++) {
            Index index = (Index) list.get(i);
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
                    // TODO NULL FIRST for ascending and NULLS LAST 
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
        if (limitRows != 0 && offset != null) {
            // limitRows must be long, otherwise we get an int overflow 
            // if limitRows is at or near Integer.MAX_VALUE
            limitRows += offset.getValue(session).getInt();
        }
        int rowNumber = 0;
        setCurrentRowNumber(0);
        Index index = topTableFilter.getIndex();
        SearchRow first = null;
        int columnIndex = index.getColumns()[0].getColumnId();
        while (true) {
            checkCancelled();
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
        if (limitRows != 0 && offset != null) {
            // limitRows must be long, otherwise we get an int overflow 
            // if limitRows is at or near Integer.MAX_VALUE
            limitRows += offset.getValue(session).getInt();
        }
        int rowNumber = 0;
        setCurrentRowNumber(0);
        while (topTableFilter.next()) {
            checkCancelled();
            setCurrentRowNumber(rowNumber + 1);
            if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                Value[] row = new Value[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    Expression expr = (Expression) expressions.get(i);
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
            Expression expr = (Expression) expressions.get(i);
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
        if (limit != null) {
            int l = limit.getValue(session).getInt();
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
        if (offset != null) {
            result.setOffset(offset.getValue(session).getInt());
        }
        if (limitRows != 0) {
            result.setLimit(limitRows);
        }
        result.done();
        return result;
    }

    private void expandColumnList() throws SQLException {
        // TODO this works: select distinct count(*) from system_columns group
        // by table
        for (int i = 0; i < expressions.size(); i++) {
            Expression expr = (Expression) expressions.get(i);
            if (!expr.isWildcard()) {
                continue;
            }
            String schemaName = expr.getSchemaName();
            String tableAlias = expr.getTableAlias();
            if (tableAlias == null) {
                int temp = i;
                expressions.remove(i);
                for (int j = 0; j < filters.size(); j++) {
                    TableFilter filter = (TableFilter) filters.get(j);
                    Wildcard c2 = new Wildcard(filter.getTable().getSchema().getName(), filter.getTableAlias());
                    expressions.add(i++, c2);
                }
                i = temp - 1;
            } else {
                TableFilter filter = null;
                for (int j = 0; j < filters.size(); j++) {
                    TableFilter f = (TableFilter) filters.get(j);
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
                for (int j = 0; j < columns.length; j++) {
                    Column c = columns[j];
                    ExpressionColumn ec = new ExpressionColumn(session.getDatabase(), null, alias, c.getName());
                    expressions.add(i++, ec);
                }
                i--;
            }
        }
    }

    public void init() throws SQLException {
        if (SysProperties.CHECK && checkInit) {
            throw Message.getInternalError();
        }
        expandColumnList();
        visibleColumnCount = expressions.size();
        ObjectArray expressionSQL;
        if (orderList != null || group != null) {
            expressionSQL = new ObjectArray();
            for (int i = 0; i < expressions.size(); i++) {
                Expression expr = (Expression) expressions.get(i);
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

        // first visible columns, then order by, then having,
        // and group by at the end
        if (group != null) {
            groupIndex = new int[group.size()];
            for (int i = 0; i < group.size(); i++) {
                Expression expr = (Expression) group.get(i);
                String sql = expr.getSQL();
                int found = -1;
                for (int j = 0; j < expressionSQL.size(); j++) {
                    String s2 = (String) expressionSQL.get(j);
                    if (s2.equals(sql)) {
                        found = j;
                        break;
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
            for (int i = 0; i < groupIndex.length; i++) {
                groupByExpression[groupIndex[i]] = true;
            }
            group = null;
        }
        // map columns in select list and condition
        for (int i = 0; i < filters.size(); i++) {
            TableFilter f = (TableFilter) filters.get(i);
            for (int j = 0; j < expressions.size(); j++) {
                Expression expr = (Expression) expressions.get(j);
                expr.mapColumns(f, 0);
            }
            if (condition != null) {
                condition.mapColumns(f, 0);
            }
        }
        checkInit = true;
    }

    public void prepare() throws SQLException {
        if (isPrepared) {
            // sometimes a subquery is prepared twice (CREATE TABLE AS SELECT)
            return;
        }
        if (SysProperties.CHECK && !checkInit) {
            throw Message.getInternalError("not initialized");
        }
        if (orderList != null) {
            sort = prepareOrder(orderList, expressions.size());
            orderList = null;
        }
        for (int i = 0; i < expressions.size(); i++) {
            Expression e = (Expression) expressions.get(i);
            expressions.set(i, e.optimize(session));
        }
        if (condition != null) {
            condition = condition.optimize(session);
            if (SysProperties.OPTIMIZE_IN_JOIN) {
                condition = condition.optimizeInJoin(session, this);
            }
            for (int j = 0; j < filters.size(); j++) {
                TableFilter f = (TableFilter) filters.get(j);
                condition.createIndexConditions(session, f);
            }
        }
        if (isGroupQuery && groupIndex == null && havingIndex < 0 && filters.size() == 1) {
            if (condition == null) {
                ExpressionVisitor optimizable = ExpressionVisitor.get(ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL);
                optimizable.setTable(((TableFilter) filters.get(0)).getTable());
                isQuickAggregateQuery = isEverything(optimizable);
            }
        }
        cost = preparePlan();
        if (SysProperties.OPTIMIZE_DISTINCT && distinct && !isGroupQuery && filters.size() == 1 && expressions.size() == 1 && condition == null) {
            Expression expr = (Expression) expressions.get(0);
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

    public HashSet getTables() {
        HashSet set = new HashSet();
        for (int i = 0; i < filters.size(); i++) {
            TableFilter filter = (TableFilter) filters.get(i);
            set.add(filter.getTable());
        }
        return set;
    }

    private double preparePlan() throws SQLException {

        TableFilter[] topArray = new TableFilter[topFilters.size()];
        topFilters.toArray(topArray);
        for (int i = 0; i < topArray.length; i++) {
            topArray[i].setFullCondition(condition);
        }

        Optimizer optimizer = new Optimizer(topArray, condition, session);
        optimizer.optimize();
        topTableFilter = optimizer.getTopFilter();
        double cost = optimizer.getCost();

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
            for (int i = 0; i < expressions.size(); i++) {
                Expression e = (Expression) expressions.get(i);
                e.setEvaluatable(f, true);
            }
            f = f.getJoin();
        }
        topTableFilter.prepare();
        return cost;
    }

    public String getPlanSQL() {
        if (topTableFilter == null) {
            return sqlStatement;
        }
        StringBuffer buff = new StringBuffer();
        Expression[] exprList = new Expression[expressions.size()];
        expressions.toArray(exprList);
        buff.append("SELECT ");
        if (distinct) {
            buff.append("DISTINCT ");
        }
        for (int i = 0; i < visibleColumnCount; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            Expression expr = exprList[i];
            buff.append(expr.getSQL());
        }
        buff.append("\nFROM ");
        TableFilter filter = topTableFilter;
        boolean join = false;
        int id = 0;
        do {
            if (id > 0) {
                buff.append("\n");
            }
            buff.append(filter.getPlanSQL(join));
            id++;
            join = true;
            filter = filter.getJoin();
        } while (filter != null);
        if (condition != null) {
            buff.append("\nWHERE " + StringUtils.unEnclose(condition.getSQL()));
        }
        if (groupIndex != null) {
            buff.append("\nGROUP BY ");
            for (int i = 0; i < groupIndex.length; i++) {
                Expression g = exprList[groupIndex[i]];
                g = g.getNonAliasExpression();
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append(StringUtils.unEnclose(g.getSQL()));
            }
        }
        if (having != null) {
            // could be set in addGlobalCondition
            // in this case the query is not run directly, just getPlanSQL is
            // called
            Expression h = having;
            buff.append("\nHAVING " + StringUtils.unEnclose(h.getSQL()));
        } else if (havingIndex >= 0) {
            Expression h = exprList[havingIndex];
            buff.append("\nHAVING " + StringUtils.unEnclose(h.getSQL()));
        }
        if (sort != null) {
            buff.append("\nORDER BY ");
            buff.append(sort.getSQL(exprList, visibleColumnCount));
        }
        if (limit != null) {
            buff.append("\nLIMIT ");
            buff.append(StringUtils.unEnclose(limit.getSQL()));
            if (offset != null) {
                buff.append(" OFFSET ");
                buff.append(StringUtils.unEnclose(offset.getSQL()));
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

    public ObjectArray getExpressions() {
        return expressions;
    }

    public void setForUpdate(boolean b) {
        this.isForUpdate = b;
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        for (int i = 0; i < expressions.size(); i++) {
            Expression e = (Expression) expressions.get(i);
            e.mapColumns(resolver, level);
        }
        if (condition != null) {
            condition.mapColumns(resolver, level);
        }
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (int i = 0; i < expressions.size(); i++) {
            Expression e = (Expression) expressions.get(i);
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
        Expression col = (Expression) expressions.get(columnId);
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
                    having = (Expression) expressions.get(havingIndex);
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

    public void updateAggregate(Session session) throws SQLException {
        for (int i = 0; i < expressions.size(); i++) {
            Expression e = (Expression) expressions.get(i);
            e.updateAggregate(session);
        }
        if (condition != null) {
            condition.updateAggregate(session);
        }
        if (having != null) {
            having.updateAggregate(session);
        }
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        switch(visitor.getType()) {
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID: {
            for (int i = 0; i < filters.size(); i++) {
                TableFilter f = (TableFilter) filters.get(i);
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
            for (int i = 0; i < filters.size(); i++) {
                TableFilter filter = (TableFilter) filters.get(i);
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
        for (int i = 0; i < expressions.size(); i++) {
            Expression e = (Expression) expressions.get(i);
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

    public String getFirstColumnAlias(Session session) {
        if (SysProperties.CHECK) {
            if (visibleColumnCount > 1) {
                throw Message.getInternalError("" + visibleColumnCount);
            }
        }
        Expression expr = (Expression) expressions.get(0);
        if (expr instanceof Alias) {
            return expr.getAlias();
        }
        expr = new Alias(expr,  session.getNextTempViewName() + "_X");
        expressions.set(0, expr);
        return expr.getAlias();
    }

}
