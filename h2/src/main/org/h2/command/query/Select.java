/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.query;

import static org.h2.expression.Expression.WITHOUT_PARENTHESES;
import static org.h2.util.HasSQL.ADD_PLAN_INFORMATION;
import static org.h2.util.HasSQL.DEFAULT_SQL_FLAGS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Mode.ExpressionNames;
import org.h2.engine.SessionLocal;
import org.h2.expression.Alias;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionList;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.Wildcard;
import org.h2.expression.analysis.DataAnalysisOperation;
import org.h2.expression.analysis.Window;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionLocalAndGlobal;
import org.h2.expression.function.CoalesceFunction;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexSort;
import org.h2.index.QueryExpressionIndex;
import org.h2.message.DbException;
import org.h2.mode.DefaultNullOrdering;
import org.h2.result.LazyResult;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.table.TableType;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueRow;

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

    private enum QuickOffset { NO, YES, PARTIAL }

    /**
     * The main (top) table filter.
     */
    TableFilter topTableFilter;

    private final ArrayList<TableFilter> filters = Utils.newSmallArrayList();
    private final ArrayList<TableFilter> topFilters = Utils.newSmallArrayList();

    /**
     * Parent select for selects in table filters.
     */
    private Select parentSelect;

    /**
     * WHERE condition.
     */
    private Expression condition;

    /**
     * HAVING condition.
     */
    private Expression having;

    /**
     * QUALIFY condition.
     */
    private Expression qualify;

    /**
     * {@code DISTINCT ON(...)} expressions.
     */
    private Expression[] distinctExpressions;

    private int[] distinctIndexes;

    private ArrayList<Expression> group;

    /**
     * The indexes of the group-by columns.
     */
    int[] groupIndex;

    /**
     * Whether a column in the expression list is part of a group-by.
     */
    boolean[] groupByExpression;

    /**
     * Grouped data for aggregates.
     */
    SelectGroups groupData;

    private int havingIndex;

    private int qualifyIndex;

    private int[] groupByCopies;

    /**
     * Whether this SELECT is an explicit table (TABLE tableName). It is used in
     * {@link #getPlanSQL(StringBuilder, int)} to generate SQL similar to original query.
     */
    private boolean isExplicitTable;

    /**
     * This flag is set when SELECT statement contains (non-window) aggregate
     * functions, GROUP BY clause or HAVING clause.
     */
    boolean isGroupQuery;
    private boolean isGroupSortedQuery;
    private boolean isWindowQuery;
    private ForUpdate forUpdate;
    private double cost;
    private boolean isQuickAggregateQuery, isDistinctQuery;
    private int indexSortedColumns;

    private boolean isGroupWindowStage2;

    private HashMap<String, Window> windows;

    public Select(SessionLocal session, Select parentSelect) {
        super(session);
        this.parentSelect = parentSelect;
    }

    @Override
    public boolean isUnion() {
        return false;
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
        // if (filterNames.contains(alias)) {
        //     throw Message.getSQLException(
        //         ErrorCode.DUPLICATE_TABLE_ALIAS, alias);
        // }
        // filterNames.add(alias);
        filters.add(filter);
        if (isTop) {
            topFilters.add(filter);
        }
    }

    public ArrayList<TableFilter> getTopFilters() {
        return topFilters;
    }

    public void setExpressions(ArrayList<Expression> expressions) {
        this.expressions = expressions;
    }

    /**
     * Convert this SELECT to an explicit table (TABLE tableName).
     */
    public void setExplicitTable() {
        setWildcard();
        isExplicitTable = true;
    }

    /**
     * Sets a wildcard expression as in "SELECT * FROM TEST".
     */
    public void setWildcard() {
        expressions = new ArrayList<>(1);
        expressions.add(new Wildcard(null, null));
    }

    /**
     * Set when SELECT statement contains (non-window) aggregate functions,
     * GROUP BY clause or HAVING clause.
     */
    public void setGroupQuery() {
        isGroupQuery = true;
    }

    /**
     * Called if this query contains window functions.
     */
    public void setWindowQuery() {
        isWindowQuery = true;
    }

    public void setGroupBy(ArrayList<Expression> group) {
        this.group = group;
    }

    public ArrayList<Expression> getGroupBy() {
        return group;
    }

    /**
     * Get the group data if there is currently a group-by active.
     *
     * @param window is this a window function
     * @return the grouped data
     */
    public SelectGroups getGroupDataIfCurrent(boolean window) {
        return groupData != null && (window || groupData.isCurrentGroup()) ? groupData : null;
    }

    /**
     * Set the distinct flag.
     */
    public void setDistinct() {
        if (distinctExpressions != null) {
            throw DbException.getUnsupportedException("DISTINCT ON together with DISTINCT");
        }
        distinct = true;
    }

    /**
     * Set the DISTINCT ON expressions.
     *
     * @param distinctExpressions array of expressions
     */
    public void setDistinct(Expression[] distinctExpressions) {
        if (distinct) {
            throw DbException.getUnsupportedException("DISTINCT ON together with DISTINCT");
        }
        this.distinctExpressions = distinctExpressions;
    }

    @Override
    public boolean isAnyDistinct() {
        return distinct || distinctExpressions != null;
    }

    /**
     * Adds a named window definition.
     *
     * @param name name
     * @param window window definition
     * @return true if a new definition was added, false if old definition was replaced
     */
    public boolean addWindow(String name, Window window) {
        if (windows == null) {
            windows = new HashMap<>();
        }
        return windows.put(name, window) == null;
    }

    /**
     * Returns a window with specified name, or null.
     *
     * @param name name of the window
     * @return the window with specified name, or null
     */
    public Window getWindow(String name) {
        return windows != null ? windows.get(name) : null;
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

    public Expression getCondition() {
        return condition;
    }

    private LazyResult queryGroupSorted(int columnCount, ResultTarget result, long offset, boolean quickOffset) {
        LazyResultGroupSorted lazyResult = new LazyResultGroupSorted(expressionArray, columnCount);
        skipOffset(lazyResult, offset, quickOffset);
        if (result == null) {
            return lazyResult;
        }
        while (lazyResult.next()) {
            result.addRow(lazyResult.currentRow());
        }
        return null;
    }

    /**
     * Create a row with the current values, for queries with group-sort.
     *
     * @param keyValues the key values
     * @param columnCount the number of columns
     * @return the row
     */
    Value[] createGroupSortedRow(Value[] keyValues, int columnCount) {
        Value[] row = constructGroupResultRow(keyValues, columnCount);
        if (isHavingNullOrFalse(row)) {
            return null;
        }
        return rowForResult(row, columnCount);
    }

    /**
     * Removes HAVING and QUALIFY columns from the row.
     *
     * @param row
     *            the complete row
     * @param columnCount
     *            the number of columns to keep
     * @return the same or the truncated row
     */
    private Value[] rowForResult(Value[] row, int columnCount) {
        if (columnCount == resultColumnCount) {
            return row;
        }
        return Arrays.copyOf(row, resultColumnCount);
    }

    private boolean isHavingNullOrFalse(Value[] row) {
        return havingIndex >= 0 && !row[havingIndex].isTrue();
    }

    private Index getGroupSortedIndex() {
        if (groupIndex == null || groupByExpression == null) {
            return null;
        }
        ArrayList<Index> indexes = topTableFilter.getTable().getIndexes();
        if (indexes != null) {
            for (Index index : indexes) {
                if (index.getIndexType().isScan()) {
                    continue;
                }
                if (index.getIndexType().isHash()) {
                    // does not allow scanning entries
                    continue;
                }
                if (isGroupSortedIndex(topTableFilter, index)) {
                    return index;
                }
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
        for (int i = 0, size = expressions.size(); i < size; i++) {
            if (!groupByExpression[i]) {
                continue;
            }
            Expression expr = expressions.get(i).getNonAliasExpression();
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

    boolean isConditionMetForUpdate() {
        if (isConditionMet()) {
            int count = filters.size();
            boolean notChanged = true;
            for (int i = 0; i < count; i++) {
                TableFilter tableFilter = filters.get(i);
                if (!tableFilter.isJoinOuter() && !tableFilter.isJoinOuterIndirect()) {
                    Row row = tableFilter.get();
                    Table table = tableFilter.getTable();
                    // Views, function tables, links, etc. do not support locks
                    if (table.isRowLockable()) {
                        Row lockedRow = table.lockRow(session, row, forUpdate.getTimeoutMillis());
                        if (lockedRow == null) {
                            return false;
                        }
                        if (!row.hasSharedData(lockedRow)) {
                            tableFilter.set(lockedRow);
                            notChanged = false;
                        }
                    }
                }
            }
            return notChanged || isConditionMet();
        }
        return false;
    }

    boolean isConditionMet() {
        return condition == null || condition.getBooleanValue(session);
    }

    private void queryWindow(int columnCount, LocalResult result, long offset, boolean quickOffset) {
        initGroupData(columnCount);
        try {
            gatherGroup(columnCount, DataAnalysisOperation.STAGE_WINDOW);
            processGroupResult(columnCount, result, offset, quickOffset, false);
        } finally {
            groupData.reset();
        }
    }

    private void queryGroupWindow(int columnCount, LocalResult result, long offset, boolean quickOffset) {
        initGroupData(columnCount);
        try {
            gatherGroup(columnCount, DataAnalysisOperation.STAGE_GROUP);
            try {
                isGroupWindowStage2 = true;
                while (groupData.next() != null) {
                    if (havingIndex < 0 || expressions.get(havingIndex).getBooleanValue(session)) {
                        updateAgg(columnCount, DataAnalysisOperation.STAGE_WINDOW);
                    } else {
                        groupData.remove();
                    }
                }
                groupData.done();
                processGroupResult(columnCount, result, offset, quickOffset, /* Having was performed earlier */ false);
            } finally {
                isGroupWindowStage2 = false;
            }
        } finally {
            groupData.reset();
        }
    }

    private void queryGroup(int columnCount, LocalResult result, long offset, boolean quickOffset) {
        initGroupData(columnCount);
        try {
            gatherGroup(columnCount, DataAnalysisOperation.STAGE_GROUP);
            processGroupResult(columnCount, result, offset, quickOffset, true);
        } finally {
            groupData.reset();
        }
    }

    private void initGroupData(int columnCount) {
        if (groupData == null) {
            setGroupData(SelectGroups.getInstance(session, expressions, isGroupQuery, groupIndex));
        } else {
            updateAgg(columnCount, DataAnalysisOperation.STAGE_RESET);
        }
        groupData.reset();
    }

    void setGroupData(final SelectGroups groupData) {
        this.groupData = groupData;
        topTableFilter.visit(f -> {
            Select s = f.getSelect();
            if (s != null) {
                s.groupData = groupData;
            }
        });
    }

    private void gatherGroup(int columnCount, int stage) {
        long rowNumber = 0;
        setCurrentRowNumber(0);
        while (topTableFilter.next()) {
            setCurrentRowNumber(rowNumber + 1);
            if (forUpdate != null ? isConditionMetForUpdate() : isConditionMet()) {
                rowNumber++;
                groupData.nextSource();
                updateAgg(columnCount, stage);
            }
        }
        groupData.done();
    }


    /**
     * Update any aggregate expressions with the query stage.
     * @param columnCount number of columns
     * @param stage see STAGE_RESET/STAGE_GROUP/STAGE_WINDOW in DataAnalysisOperation
     */
    void updateAgg(int columnCount, int stage) {
        for (int i = 0; i < columnCount; i++) {
            if ((groupByExpression == null || !groupByExpression[i])
                    && (groupByCopies == null || groupByCopies[i] < 0)) {
                Expression expr = expressions.get(i);
                expr.updateAggregate(session, stage);
            }
        }
    }

    private void processGroupResult(int columnCount, LocalResult result, long offset, boolean quickOffset,
            boolean withHaving) {
        for (ValueRow currentGroupsKey; (currentGroupsKey = groupData.next()) != null;) {
            Value[] row = constructGroupResultRow(currentGroupsKey.getList(), columnCount);
            if (withHaving && isHavingNullOrFalse(row)) {
                continue;
            }
            if (qualifyIndex >= 0 && !row[qualifyIndex].isTrue()) {
                continue;
            }
            if (quickOffset && offset > 0) {
                offset--;
                continue;
            }
            result.addRow(rowForResult(row, columnCount));
        }
    }

    private Value[] constructGroupResultRow(Value[] keyValues, int columnCount) {
        Value[] row = new Value[columnCount];
        if (groupIndex != null) {
            for (int i = 0, l = groupIndex.length; i < l; i++) {
                row[groupIndex[i]] = keyValues[i];
            }
        }
        for (int i = 0; i < columnCount; i++) {
            if (groupByExpression != null && groupByExpression[i]) {
                continue;
            }
            if (groupByCopies != null) {
                int original = groupByCopies[i];
                if (original >= 0) {
                    row[i] = row[original];
                    continue;
                }
            }
            row[i] = expressions.get(i).getValue(session);
        }
        return row;
    }

    /**
     * Returns possible index-sorting operations (better first) if they exist.
     *
     * @return possible index-sorting operations, or {@code null} if unavailable
     */
    private List<IndexSort> getIndexSorts() {
        if (sort == null) {
            return null;
        }
        ArrayList<Column> sortColumns = Utils.newSmallArrayList();
        int[] queryColumnIndexes = sort.getQueryColumnIndexes();
        int queryIndexesLength = queryColumnIndexes.length;
        int[] sortIndex = new int[queryIndexesLength];
        int sortedColumns = 0;
        boolean needMore = false;
        for (int i = 0; i < queryIndexesLength; i++) {
            int idx = queryColumnIndexes[i];
            if (idx < 0 || idx >= expressions.size()) {
                throw DbException.getInvalidValueException("ORDER BY", idx + 1);
            }
            Expression expr = expressions.get(idx);
            expr = expr.getNonAliasExpression();
            if (expr.isConstant()) {
                continue;
            }
            if (!(expr instanceof ExpressionColumn)) {
                needMore = true;
                break;
            }
            ExpressionColumn exprCol = (ExpressionColumn) expr;
            if (exprCol.getTableFilter() != topTableFilter) {
                needMore = true;
                break;
            }
            sortColumns.add(exprCol.getColumn());
            sortIndex[sortedColumns++] = i;
        }
        if (sortedColumns == 0) {
            if (needMore) {
                // Can't sort using index
                return null;
            }
            // sort just on constants - can use scan index
            return List.of(new IndexSort(topTableFilter.getTable().getScanIndex(session), false));
        }
        Column[] sortCols;
        int[] sortTypes = sort.getSortTypesWithNullOrdering();
        if (sortedColumns == 1) {
            Column column = sortColumns.get(0);
            if (column.getColumnId() == -1) {
                // special case: order by _ROWID_
                Index index = topTableFilter.getTable().getScanIndex(session);
                if (index.isRowIdIndex()) {
                    return List.of(new IndexSort(index, needMore ? sortedColumns : IndexSort.FULLY_SORTED,
                            (sortTypes[sortIndex[0]] & SortOrder.DESCENDING) != 0));
                }
            }
            sortCols = new Column[] { column };
        } else {
            sortCols = sortColumns.toArray(new Column[0]);
        }
        ArrayList<Index> list = topTableFilter.getTable().getIndexes();
        if (list == null) {
            return null;
        }
        DefaultNullOrdering defaultNullOrdering = getDatabase().getDefaultNullOrdering();
        ArrayList<IndexSort> indexSorts = Utils.newSmallArrayList();
        loop: for (Index index : list) {
            if (index.getCreateSQL() == null || index.getIndexType().isHash()) {
                // can't use scan or hash indexes
                continue;
            }
            IndexColumn[] indexCols = index.getIndexColumns();
            int count = Math.min(indexCols.length, sortedColumns);
            boolean reverse = false;
            for (int j = 0; j < count; j++) {
                // the index and the sort order must start
                // with the exact same columns
                IndexColumn idxCol = indexCols[j];
                Column sortCol = sortCols[j];
                boolean mismatch = idxCol.column != sortCol;
                if (!mismatch) {
                    if (sortCol.isNullable()) {
                        int o1 = defaultNullOrdering.addExplicitNullOrdering(idxCol.sortType);
                        int o2 = sortTypes[sortIndex[j]];
                        if (j == 0) {
                            if (o1 != o2) {
                                if (o1 == SortOrder.inverse(o2)) {
                                    reverse = true;
                                } else {
                                    mismatch = true;
                                }
                            }
                        } else {
                            if (o1 != (reverse ? SortOrder.inverse(o2) : o2)) {
                                mismatch = true;
                            }
                        }
                    } else {
                        boolean different = (idxCol.sortType & SortOrder.DESCENDING) //
                                != (sortTypes[sortIndex[j]] & SortOrder.DESCENDING);
                        if (j == 0) {
                            reverse = different;
                        } else {
                            mismatch = different != reverse;
                        }
                    }
                }
                if (mismatch) {
                    if (j > 0) {
                        indexSorts.add(new IndexSort(index, j, reverse));
                    }
                    continue loop;
                }
            }
            indexSorts.add(new IndexSort(index, needMore || count < sortedColumns ? count : IndexSort.FULLY_SORTED,
                    reverse));
        }
        indexSorts.sort(null);
        return indexSorts;
    }

    private void queryDistinct(ResultTarget result, long offset, long limitRows, boolean withTies,
            boolean quickOffset) {
        if (limitRows > 0 && offset > 0) {
            limitRows += offset;
            if (limitRows < 0) {
                // Overflow
                limitRows = Long.MAX_VALUE;
            }
        }
        long rowNumber = 0;
        setCurrentRowNumber(0);
        Index index = topTableFilter.getIndex();
        SearchRow first = null;
        int columnIndex = index.getColumns()[0].getColumnId();
        if (!quickOffset) {
            offset = 0;
        }
        for (;;) {
            setCurrentRowNumber(++rowNumber);
            Cursor cursor = index.findNext(session, first, null);
            if (!cursor.next()) {
                break;
            }
            SearchRow found = cursor.getSearchRow();
            Value value = found.getValue(columnIndex);
            if (first == null) {
                first = index.getRowFactory().createRow();
            }
            first.setValue(columnIndex, value);
            if (offset > 0) {
                offset--;
                continue;
            }
            result.addRow(value);
            if ((sort == null || indexSortedColumns == IndexSort.FULLY_SORTED) && limitRows > 0
                    && rowNumber >= limitRows && !withTies) {
                break;
            }
        }
    }

    private LazyResult queryFlat(int columnCount, ResultTarget result, long offset, long limitRows, boolean withTies,
            QuickOffset quickOffset) {
        if (limitRows > 0 && offset > 0 && quickOffset != QuickOffset.YES) {
            limitRows += offset;
            if (limitRows < 0) {
                // Overflow
                limitRows = Long.MAX_VALUE;
            }
        }
        LazyResultQueryFlat lazyResult = new LazyResultQueryFlat(expressionArray, columnCount, forUpdate != null);
        skipOffset(lazyResult, offset, quickOffset == QuickOffset.YES);
        if (result == null) {
            return lazyResult;
        }
        if (limitRows == Long.MAX_VALUE || limitRows < 0 || sort != null && indexSortedColumns == 0
                || withTies && quickOffset == QuickOffset.NO) {
            while (lazyResult.next()) {
                result.addRow(lazyResult.currentRow());
            }
        } else {
            readWithLimit(result, limitRows, withTies, lazyResult);
        }
        return null;
    }

    private void readWithLimit(ResultTarget result, long limitRows, boolean withTies, LazyResultQueryFlat lazyResult) {
        Value[] last = null;
        while (result.getRowCount() < limitRows && lazyResult.next()) {
            last = lazyResult.currentRow();
            result.addRow(last);
        }
        if (sort != null && last != null) {
            if (indexSortedColumns < IndexSort.FULLY_SORTED) {
                while (lazyResult.next()) {
                    Value[] row = lazyResult.currentRow();
                    if (sort.compare(last, row, indexSortedColumns) != 0) {
                        break;
                    }
                    result.addRow(row);
                }
            } else if (withTies) {
                while (lazyResult.next()) {
                    Value[] row = lazyResult.currentRow();
                    if (sort.compare(last, row) != 0) {
                        break;
                    }
                    result.addRow(row);
                }
                result.limitsWereApplied();
            }
        }
    }

    private static void skipOffset(LazyResultSelect lazyResult, long offset, boolean quickOffset) {
        if (quickOffset) {
            while (offset > 0 && lazyResult.skip()) {
                offset--;
            }
        }
    }

    private void queryQuick(int columnCount, ResultTarget result, boolean skipResult) {
        Value[] row = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Expression expr = expressions.get(i);
            row[i] = expr.getValue(session);
        }
        if (!skipResult) {
            result.addRow(row);
        }
    }

    @Override
    protected ResultInterface queryWithoutCache(long maxRows, ResultTarget target) {
        disableLazyForJoinSubqueries(topTableFilter);
        OffsetFetch offsetFetch = getOffsetFetch(maxRows);
        long offset = offsetFetch.offset;
        long fetch = offsetFetch.fetch;
        boolean fetchPercent = offsetFetch.fetchPercent;
        boolean lazy = session.isLazyQueryExecution() &&
                target == null && forUpdate == null && !isQuickAggregateQuery &&
                fetch != 0 && !fetchPercent && !withTies && offset == 0 && isReadOnly();
        int columnCount = expressions.size();
        LocalResult result = null;
        if (!lazy && (target == null ||
                !getDatabase().getSettings().optimizeInsertFromSelect)) {
            result = createLocalResult(result);
        }
        // Do not add rows before OFFSET to result if possible
        QuickOffset quickOffset = fetchPercent ? QuickOffset.NO : QuickOffset.YES;
        if (sort != null && (indexSortedColumns != IndexSort.FULLY_SORTED || isAnyDistinct())) {
            result = createLocalResult(result);
            result.setSortOrder(sort);
            if (indexSortedColumns != IndexSort.FULLY_SORTED) {
                quickOffset = indexSortedColumns > 0 ? QuickOffset.PARTIAL : QuickOffset.NO;
            }
        }
        if (distinct) {
            result = createLocalResult(result);
            if (!isDistinctQuery) {
                quickOffset = QuickOffset.NO;
                result.setDistinct();
            }
        } else if (distinctExpressions != null) {
            quickOffset = QuickOffset.NO;
            result = createLocalResult(result);
            result.setDistinct(distinctIndexes);
        }
        if (isWindowQuery || isGroupQuery && !isGroupSortedQuery) {
            result = createLocalResult(result);
        }
        if (!lazy && (fetch >= 0 || offset > 0)) {
            result = createLocalResult(result);
        }
        topTableFilter.startQuery(session);
        topTableFilter.reset();
        topTableFilter.lock(session);
        ResultTarget to = result != null ? result : target;
        lazy &= to == null;
        LazyResult lazyResult = null;
        if (fetch != 0) {
            // Cannot apply limit now if percent is specified
            long limit = fetchPercent ? -1 : fetch;
            if (isQuickAggregateQuery) {
                queryQuick(columnCount, to, quickOffset == QuickOffset.YES && offset > 0);
            } else if (isWindowQuery) {
                if (isGroupQuery) {
                    queryGroupWindow(columnCount, result, offset, quickOffset == QuickOffset.YES);
                } else {
                    queryWindow(columnCount, result, offset, quickOffset == QuickOffset.YES);
                }
            } else if (isGroupQuery) {
                if (isGroupSortedQuery) {
                    lazyResult = queryGroupSorted(columnCount, to, offset, quickOffset == QuickOffset.YES);
                } else {
                    queryGroup(columnCount, result, offset, quickOffset == QuickOffset.YES);
                }
            } else if (isDistinctQuery) {
                queryDistinct(to, offset, limit, withTies, quickOffset == QuickOffset.YES);
            } else {
                lazyResult = queryFlat(columnCount, to, offset, limit, withTies, quickOffset);
            }
            if (quickOffset == QuickOffset.YES) {
                offset = 0;
            }
        }
        assert lazy == (lazyResult != null) : lazy;
        if (lazyResult != null) {
            if (fetch > 0) {
                lazyResult.setLimit(fetch);
            }
            if (randomAccessResult) {
                return convertToDistinct(lazyResult);
            } else {
                return lazyResult;
            }
        }
        if (result != null) {
            return finishResult(result, offset, fetch, fetchPercent, target);
        }
        return null;
    }

    private void disableLazyForJoinSubqueries(final TableFilter top) {
        if (session.isLazyQueryExecution()) {
            top.visit(f -> {
                if (f != top && f.getTable().getTableType() == TableType.VIEW) {
                    QueryExpressionIndex idx = (QueryExpressionIndex) f.getIndex();
                    if (idx != null && idx.getQuery() != null) {
                        idx.getQuery().setNeverLazy(true);
                    }
                }
            });
        }
    }

    private LocalResult createLocalResult(LocalResult old) {
        return old != null ? old : new LocalResult(session, expressionArray, visibleColumnCount, resultColumnCount);
    }

    private void expandColumnList() {
        // the expressions may change within the loop
        for (int i = 0; i < expressions.size();) {
            Expression expr = expressions.get(i);
            if (!(expr instanceof Wildcard)) {
                i++;
                continue;
            }
            expressions.remove(i);
            Wildcard w = (Wildcard) expr;
            String tableAlias = w.getTableAlias();
            boolean hasExceptColumns = w.getExceptColumns() != null;
            HashMap<Column, ExpressionColumn> exceptTableColumns = null;
            if (tableAlias == null) {
                if (hasExceptColumns) {
                    for (TableFilter filter : filters) {
                        w.mapColumns(filter, 1, Expression.MAP_INITIAL);
                    }
                    exceptTableColumns = w.mapExceptColumns();
                }
                for (TableFilter filter : filters) {
                    i = expandColumnList(filter, i, false, exceptTableColumns);
                }
            } else {
                Database db = getDatabase();
                String schemaName = w.getSchemaName();
                TableFilter filter = null;
                for (TableFilter f : filters) {
                    if (db.equalsIdentifiers(tableAlias, f.getTableAlias())) {
                        if (schemaName == null || db.equalsIdentifiers(schemaName, f.getSchemaName())) {
                            if (hasExceptColumns) {
                                w.mapColumns(f, 1, Expression.MAP_INITIAL);
                                exceptTableColumns = w.mapExceptColumns();
                            }
                            filter = f;
                            break;
                        }
                    }
                }
                if (filter == null) {
                    throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableAlias);
                }
                i = expandColumnList(filter, i, true, exceptTableColumns);
            }
        }
    }

    private int expandColumnList(TableFilter filter, int index, boolean forAlias,
            HashMap<Column, ExpressionColumn> except) {
        String schema = filter.getSchemaName();
        String alias = filter.getTableAlias();
        if (forAlias) {
            for (Column c : filter.getTable().getColumns()) {
                index = addExpandedColumn(filter, index, except, schema, alias, c);
            }
        } else {
            LinkedHashMap<Column, Column> commonJoinColumns = filter.getCommonJoinColumns();
            if (commonJoinColumns != null) {
                TableFilter replacementFilter = filter.getCommonJoinColumnsFilter();
                String replacementSchema = replacementFilter.getSchemaName();
                String replacementAlias = replacementFilter.getTableAlias();
                for (Entry<Column, Column> entry : commonJoinColumns.entrySet()) {
                    Column left = entry.getKey(), right = entry.getValue();
                    if (!filter.isCommonJoinColumnToExclude(right)
                            && (except == null || except.remove(left) == null && except.remove(right) == null)) {
                        Database database = getDatabase();
                        Expression e;
                        if (left == right
                                || DataType.hasTotalOrdering(left.getType().getValueType())
                                && DataType.hasTotalOrdering(right.getType().getValueType())) {
                            e = new ExpressionColumn(database, replacementSchema, replacementAlias,
                                    replacementFilter.getColumnName(right));
                        } else {
                            e = new Alias(new CoalesceFunction(CoalesceFunction.COALESCE,
                                    new ExpressionColumn(database, schema, alias, filter.getColumnName(left)),
                                    new ExpressionColumn(database, replacementSchema, replacementAlias,
                                            replacementFilter.getColumnName(right))), //
                                    left.getName(), true);
                        }
                        expressions.add(index++, e);
                    }
                }
            }
            for (Column c : filter.getTable().getColumns()) {
                if (commonJoinColumns == null || !commonJoinColumns.containsKey(c)) {
                    if (!filter.isCommonJoinColumnToExclude(c)) {
                        index = addExpandedColumn(filter, index, except, schema, alias, c);
                    }
                }
            }
        }
        return index;
    }

    private int addExpandedColumn(TableFilter filter, int index, HashMap<Column, ExpressionColumn> except,
            String schema, String alias, Column c) {
        if ((except == null || except.remove(c) == null) && c.getVisible()) {
            ExpressionColumn ec = new ExpressionColumn(getDatabase(), schema, alias, filter.getColumnName(c));
            expressions.add(index++, ec);
        }
        return index;
    }

    @Override
    public void init() {
        if (checkInit) {
            throw DbException.getInternalError();
        }
        filters.sort(TableFilter.ORDER_IN_FROM_COMPARATOR);
        expandColumnList();
        if ((visibleColumnCount = expressions.size()) > Constants.MAX_COLUMNS) {
            throw DbException.get(ErrorCode.TOO_MANY_COLUMNS_1, "" + Constants.MAX_COLUMNS);
        }
        ArrayList<String> expressionSQL;
        if (distinctExpressions != null || orderList != null || group != null) {
            expressionSQL = new ArrayList<>(visibleColumnCount);
            for (int i = 0; i < visibleColumnCount; i++) {
                Expression expr = expressions.get(i);
                expr = expr.getNonAliasExpression();
                expressionSQL.add(expr.getSQL(DEFAULT_SQL_FLAGS, WITHOUT_PARENTHESES));
            }
        } else {
            expressionSQL = null;
        }
        if (distinctExpressions != null) {
            BitSet set = new BitSet();
            for (Expression e : distinctExpressions) {
                set.set(initExpression(expressionSQL, e, false, filters));
            }
            int idx = 0, cnt = set.cardinality();
            distinctIndexes = new int[cnt];
            for (int i = 0; i < cnt; i++) {
                idx = set.nextSetBit(idx);
                distinctIndexes[i] = idx;
                idx++;
            }
        }
        if (orderList != null) {
            initOrder(expressionSQL, isAnyDistinct(), filters);
        }
        resultColumnCount = expressions.size();
        if (having != null) {
            expressions.add(having);
            havingIndex = expressions.size() - 1;
            having = null;
        } else {
            havingIndex = -1;
        }
        if (qualify != null) {
            expressions.add(qualify);
            qualifyIndex = expressions.size() - 1;
            qualify = null;
        } else {
            qualifyIndex = -1;
        }

        if (withTies && !hasOrder()) {
            throw DbException.get(ErrorCode.WITH_TIES_WITHOUT_ORDER_BY);
        }

        Database db = getDatabase();

        // first the select list (visible columns),
        // then 'ORDER BY' expressions,
        // then 'HAVING' expressions,
        // and 'GROUP BY' expressions at the end
        if (group != null) {
            int size = group.size();
            int expSize = expressionSQL.size();
            int fullExpSize = expressions.size();
            if (fullExpSize > expSize) {
                expressionSQL.ensureCapacity(fullExpSize);
                for (int i = expSize; i < fullExpSize; i++) {
                    expressionSQL.add(expressions.get(i).getSQL(DEFAULT_SQL_FLAGS, WITHOUT_PARENTHESES));
                }
            }
            groupIndex = new int[size];
            for (int i = 0; i < size; i++) {
                Expression expr = group.get(i);
                String sql = expr.getSQL(DEFAULT_SQL_FLAGS, WITHOUT_PARENTHESES);
                int found = -1;
                for (int j = 0; j < expSize; j++) {
                    String s2 = expressionSQL.get(j);
                    if (db.equalsIdentifiers(s2, sql)) {
                        found = mergeGroupByExpressions(db, j, expressionSQL, false);
                        break;
                    }
                }
                if (found < 0) {
                    // special case: GROUP BY a column alias
                    for (int j = 0; j < expSize; j++) {
                        Expression e = expressions.get(j);
                        if (db.equalsIdentifiers(sql, e.getAlias(session, j))) {
                            found = mergeGroupByExpressions(db, j, expressionSQL, true);
                            break;
                        }
                        sql = expr.getAlias(session, j);
                        if (db.equalsIdentifiers(sql, e.getAlias(session, j))) {
                            found = mergeGroupByExpressions(db, j, expressionSQL, true);
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
            checkUsed: if (groupByCopies != null) {
                for (int i : groupByCopies) {
                    if (i >= 0) {
                        break checkUsed;
                    }
                }
                groupByCopies = null;
            }
            groupByExpression = new boolean[expressions.size()];
            for (int gi : groupIndex) {
                groupByExpression[gi] = true;
            }
            group = null;
        }
        // map columns in select list and condition
        for (TableFilter f : filters) {
            mapColumns(f, 0, false);
        }
        mapCondition(havingIndex);
        mapCondition(qualifyIndex);
        checkInit = true;
    }

    private void mapCondition(int index) {
        if (index >= 0) {
            Expression expr = expressions.get(index);
            SelectListColumnResolver res = new SelectListColumnResolver(this);
            expr.mapColumns(res, 0, Expression.MAP_INITIAL);
        }
    }

    private int mergeGroupByExpressions(Database db, int index, ArrayList<String> expressionSQL, //
            boolean scanPrevious) {

        /*
         * -1: uniqueness of expression is not known yet
         *
         * -2: expression that is used as a source for a copy or does not have
         * copies
         *
         * >=0: expression is a copy of expression at this index
         */
        if (groupByCopies != null) {
            int c = groupByCopies[index];
            if (c >= 0) {
                return c;
            } else if (c == -2) {
                return index;
            }
        } else {
            groupByCopies = new int[expressionSQL.size()];
            Arrays.fill(groupByCopies, -1);
        }
        String sql = expressionSQL.get(index);
        if (scanPrevious) {
            /*
             * If expression was matched using an alias previous expressions may
             * be identical.
             */
            for (int i = 0; i < index; i++) {
                if (db.equalsIdentifiers(sql, expressionSQL.get(i))) {
                    index = i;
                    break;
                }
            }
        }
        int l = expressionSQL.size();
        for (int i = index + 1; i < l; i++) {
            if (db.equalsIdentifiers(sql, expressionSQL.get(i))) {
                groupByCopies[i] = index;
            }
        }
        groupByCopies[index] = -2;
        return index;
    }

    @Override
    public void prepareExpressions() {
        if (orderList != null) {
            prepareOrder(orderList, expressions.size());
        }
        ExpressionNames expressionNames = session.getMode().expressionNames;
        if (expressionNames == ExpressionNames.ORIGINAL_SQL || expressionNames == ExpressionNames.POSTGRESQL_STYLE) {
            optimizeExpressionsAndPreserveAliases();
        } else {
            for (int i = 0; i < expressions.size(); i++) {
                expressions.set(i, expressions.get(i).optimize(session));
            }
        }
        if (sort != null) {
            cleanupOrder();
        }
        if (condition != null) {
            condition = condition.optimizeCondition(session);
        }
        if (isGroupQuery && groupIndex == null && havingIndex < 0 && qualifyIndex < 0 && condition == null
                && filters.size() == 1) {
            isQuickAggregateQuery = isEverything(ExpressionVisitor.getOptimizableVisitor(filters.get(0).getTable()));
        }
        expressionArray = expressions.toArray(new Expression[0]);
    }

    @Override
    public void preparePlan() {
        if (condition != null) {
            for (TableFilter f : filters) {
                // outer joins: must not add index conditions such as
                // "c is null" - example:
                // create table parent(p int primary key) as select 1;
                // create table child(c int primary key, pc int);
                // insert into child values(2, 1);
                // select p, c from parent
                // left outer join child on p = pc where c is null;
                if (!f.isJoinOuter() && !f.isJoinOuterIndirect()) {
                    condition.createIndexConditions(session, f);
                }
            }
        }
        cost = preparePlan(session.isParsingCreateView());
        if (distinct && getDatabase().getSettings().optimizeDistinct && !isGroupQuery && filters.size() == 1
                && expressions.size() == 1 && condition == null) {
            Expression expr = expressions.get(0);
            expr = expr.getNonAliasExpression();
            if (expr instanceof ExpressionColumn) {
                Column column = ((ExpressionColumn) expr).getColumn();
                int selectivity = column.getSelectivity();
                Index columnIndex = topTableFilter.getTable().getIndexForColumn(column, false, true);
                if (columnIndex != null && selectivity != Constants.SELECTIVITY_DEFAULT && selectivity < 20) {
                    Index current = topTableFilter.getIndex();
                    // if another index is faster
                    if (current == null || current.getIndexType().isScan() || columnIndex == current) {
                        topTableFilter.setIndex(columnIndex, false);
                        isDistinctQuery = true;
                    }
                }
            }
        }
        if (sort != null && !isQuickAggregateQuery && !isGroupQuery) {
            List<IndexSort> sortIndexes = getIndexSorts();
            Index current = topTableFilter.getIndex();
            if (sortIndexes != null && current != null) {
                loop: for (IndexSort sortIndex : sortIndexes) {
                    Index index = sortIndex.getIndex();
                    boolean reverse = sortIndex.isReverse();
                    if (current.getIndexType().isScan() || current == index) {
                        topTableFilter.setIndex(index, reverse);
                        if (!topTableFilter.hasInComparisons()) {
                            // in(select ...) and in(1,2,3) may return the key in
                            // another order
                            indexSortedColumns = sortIndex.getSortedColumns();
                            break;
                        }
                    } else if (index.getIndexColumns() != null
                            && index.getIndexColumns().length >= current
                                    .getIndexColumns().length) {
                        IndexColumn[] sortColumns = index.getIndexColumns();
                        IndexColumn[] currentColumns = current.getIndexColumns();
                        boolean swapIndex = false;
                        for (int i = 0; i < currentColumns.length; i++) {
                            if (sortColumns[i].column != currentColumns[i].column) {
                                continue loop;
                            }
                            if (sortColumns[i].sortType != currentColumns[i].sortType) {
                                swapIndex = true;
                            }
                        }
                        if (swapIndex) {
                            topTableFilter.setIndex(index, reverse);
                            indexSortedColumns = sortIndex.getSortedColumns();
                            break;
                        }
                    }
                }
            }
            if (indexSortedColumns > 0 && forUpdate != null && !topTableFilter.getIndex().isRowIdIndex()) {
                indexSortedColumns = 0;
            }
        }
        if (!isQuickAggregateQuery && isGroupQuery) {
            Index index = getGroupSortedIndex();
            if (index != null) {
                Index current = topTableFilter.getIndex();
                if (current != null && (current.getIndexType().isScan() || current == index)) {
                    topTableFilter.setIndex(index, false);
                    isGroupSortedQuery = true;
                }
            }
        }
        isPrepared = true;
    }

    private void optimizeExpressionsAndPreserveAliases() {
        for (int i = 0; i < expressions.size(); i++) {
            Expression original = expressions.get(i);
            /*
             * TODO cannot evaluate optimized now, because some optimize()
             * methods violate their contract and modify the original
             * expression.
             */
            Expression optimized;
            if (i < visibleColumnCount) {
                String alias = original.getAlias(session, i);
                optimized = original.optimize(session);
                if (!optimized.getAlias(session, i).equals(alias)) {
                    optimized = new Alias(optimized, alias, true);
                }
            } else {
                optimized = original.optimize(session);
            }
            expressions.set(i, optimized);
        }
    }

    @Override
    public double getCost() {
        return cost;
    }

    @Override
    public HashSet<Table> getTables() {
        HashSet<Table> set = new HashSet<>();
        for (TableFilter filter : filters) {
            set.add(filter.getTable());
        }
        return set;
    }

    @Override
    public void fireBeforeSelectTriggers() {
        for (TableFilter filter : filters) {
            filter.getTable().fire(session, Trigger.SELECT, true);
        }
    }

    private double preparePlan(boolean parse) {
        TableFilter[] topArray = topFilters.toArray(new TableFilter[0]);
        for (TableFilter t : topArray) {
            t.createIndexConditions();
            t.setFullCondition(condition);
        }

        Optimizer optimizer = new Optimizer(topArray, condition, session);
        optimizer.optimize(parse);
        topTableFilter = optimizer.getTopFilter();
        double planCost = optimizer.getCost();

        setEvaluatableRecursive(topTableFilter);

        if (!parse) {
            topTableFilter.prepare();
        }
        return planCost;
    }

    private void setEvaluatableRecursive(TableFilter f) {
        for (; f != null; f = f.getJoin()) {
            f.setEvaluatable(f, true);
            if (condition != null) {
                condition.setEvaluatable(f, true);
            }
            TableFilter n = f.getNestedJoin();
            if (n != null) {
                setEvaluatableRecursive(n);
            }
            Expression on = f.getJoinCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
                    // need to check that all added are bound to a table
                    on = on.optimize(session);
                    if (!f.isJoinOuter() && !f.isJoinOuterIndirect()) {
                        f.removeJoinCondition();
                        addCondition(on);
                    }
                }
            }
            on = f.getFilterCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
                    f.removeFilterCondition();
                    addCondition(on);
                }
            }
            // this is only important for subqueries, so they know
            // the result columns are evaluatable
            for (Expression e : expressions) {
                e.setEvaluatable(f, true);
            }
        }
    }

    @Override
    public StringBuilder getPlanSQL(StringBuilder builder, int sqlFlags) {
        writeWithList(builder, sqlFlags);
        // can not use the field sqlStatement because the parameter
        // indexes may be incorrect: ? may be in fact ?2 for a subquery
        // but indexes may be set manually as well
        Expression[] exprList = expressions.toArray(new Expression[0]);
        if (isExplicitTable) {
            builder.append("TABLE ");
            filters.get(0).getPlanSQL(builder, false, sqlFlags);
        } else {
            builder.append("SELECT");
            if (isAnyDistinct()) {
                builder.append(" DISTINCT");
                if (distinctExpressions != null) {
                    Expression.writeExpressions(builder.append(" ON("), distinctExpressions, sqlFlags).append(')');
                }
            }
            for (int i = 0; i < visibleColumnCount; i++) {
                if (i > 0) {
                    builder.append(',');
                }
                builder.append('\n');
                StringUtils.indent(builder, exprList[i].getSQL(sqlFlags, WITHOUT_PARENTHESES), 4, false);
            }
            TableFilter filter = topTableFilter;
            if (filter == null) {
                int count = topFilters.size();
                if (count != 1 || !topFilters.get(0).isNoFromClauseFilter()) {
                    builder.append("\nFROM ");
                    boolean isJoin = false;
                    for (int i = 0; i < count; i++) {
                        isJoin = getPlanFromFilter(builder, sqlFlags, topFilters.get(i), isJoin);
                    }
                }
            } else if (!filter.isNoFromClauseFilter()) {
                getPlanFromFilter(builder.append("\nFROM "), sqlFlags, filter, false);
            }
            if (condition != null) {
                getFilterSQL(builder, "\nWHERE ", condition, sqlFlags);
            }
            if (groupIndex != null) {
                builder.append("\nGROUP BY ");
                for (int i = 0, l = groupIndex.length; i < l; i++) {
                    if (i > 0) {
                        builder.append(", ");
                    }
                    exprList[groupIndex[i]].getNonAliasExpression().getUnenclosedSQL(builder, sqlFlags);
                }
            } else if (group != null) {
                builder.append("\nGROUP BY ");
                for (int i = 0, l = group.size(); i < l; i++) {
                    if (i > 0) {
                        builder.append(", ");
                    }
                    group.get(i).getUnenclosedSQL(builder, sqlFlags);
                }
            } else emptyGroupingSet: if (isGroupQuery && having == null && havingIndex < 0) {
                for (int i = 0; i < visibleColumnCount; i++) {
                    if (containsAggregate(exprList[i])) {
                        break emptyGroupingSet;
                    }
                }
                builder.append("\nGROUP BY ()");
            }
            getFilterSQL(builder, "\nHAVING ", exprList, having, havingIndex, sqlFlags);
            getFilterSQL(builder, "\nQUALIFY ", exprList, qualify, qualifyIndex, sqlFlags);
        }
        appendEndOfQueryToSQL(builder, sqlFlags, exprList);
        if (forUpdate != null) {
            forUpdate.getSQL(builder, sqlFlags);
        }
        if ((sqlFlags & ADD_PLAN_INFORMATION) != 0) {
            if (isQuickAggregateQuery) {
                builder.append("\n/* direct lookup */");
            }
            if (isDistinctQuery) {
                builder.append("\n/* distinct */");
            }
            if (indexSortedColumns == IndexSort.FULLY_SORTED) {
                builder.append("\n/* index sorted */");
            } else if (indexSortedColumns > 0) {
                builder.append("\n/* index sorted: ").append(indexSortedColumns).append(" of ") //
                        .append(sort.getOrderList().size()).append(" columns */");
            }
            if (isGroupQuery) {
                if (isGroupSortedQuery) {
                    builder.append("\n/* group sorted */");
                }
            }
            // builder.append("\n/* cost: " + cost + " */");
        }
        return builder;
    }

    private static boolean getPlanFromFilter(StringBuilder builder, int sqlFlags, TableFilter f, boolean isJoin) {
        do {
            if (isJoin) {
                builder.append('\n');
            }
            f.getPlanSQL(builder, isJoin, sqlFlags);
            isJoin = true;
        } while ((f = f.getJoin()) != null);
        return isJoin;
    }

    private static void getFilterSQL(StringBuilder builder, String sql, Expression[] exprList, Expression condition,
            int conditionIndex, int sqlFlags) {
        if (condition != null) {
            getFilterSQL(builder, sql, condition, sqlFlags);
        } else if (conditionIndex >= 0) {
            getFilterSQL(builder, sql, exprList[conditionIndex], sqlFlags);
        }
    }

    private static void getFilterSQL(StringBuilder builder, String sql, Expression condition, int sqlFlags) {
        condition.getNonAliasExpression().getUnenclosedSQL(builder.append(sql), sqlFlags);
    }

    private static boolean containsAggregate(Expression expression) {
        if (expression instanceof DataAnalysisOperation) {
            if (((DataAnalysisOperation) expression).isAggregate()) {
                return true;
            }
        }
        for (int i = 0, l = expression.getSubexpressionCount(); i < l; i++) {
            if (containsAggregate(expression.getSubexpression(i))) {
                return true;
            }
        }
        return false;
    }

    public void setHaving(Expression having) {
        this.having = having;
    }

    public Expression getHaving() {
        return having;
    }

    public void setQualify(Expression qualify) {
        this.qualify = qualify;
    }

    public Expression getQualify() {
        return qualify;
    }

    public TableFilter getTopTableFilter() {
        return topTableFilter;
    }

    @Override
    public ForUpdate getForUpdate() {
        return forUpdate;
    }

    @Override
    public void setForUpdate(ForUpdate b) {
        if (b != null && (isAnyDistinct() || isGroupQuery)) {
            throw DbException.get(ErrorCode.FOR_UPDATE_IS_NOT_ALLOWED_IN_DISTINCT_OR_GROUPED_SELECT);
        }
        this.forUpdate = b;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, boolean outer) {
        for (Expression e : expressions) {
            e.mapColumns(resolver, level, Expression.MAP_INITIAL);
        }
        if (condition != null) {
            condition.mapColumns(resolver, level, Expression.MAP_INITIAL);
        }
        for (TableFilter tableFilter : topFilters) {
            tableFilter.mapColumns(resolver, level, outer);
        }
    }

    @Override
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

    /**
     * Checks if this query is a group query.
     *
     * @return whether this query is a group query.
     */
    public boolean isGroupQuery() {
        return isGroupQuery;
    }

    /**
     * Checks if this query contains window functions.
     *
     * @return whether this query contains window functions
     */
    public boolean isWindowQuery() {
        return isWindowQuery;
    }

    /**
     * Checks if window stage of group window query is performed. If true,
     * column resolver may not be used.
     *
     * @return true if window stage of group window query is performed
     */
    public boolean isGroupWindowStage2() {
        return isGroupWindowStage2;
    }

    @Override
    public void addGlobalCondition(Parameter param, int columnId, int comparisonType) {
        addParameter(param);
        Expression comp;
        Expression col = expressions.get(columnId);
        col = col.getNonAliasExpression();
        if (col.isEverything(ExpressionVisitor.QUERY_COMPARABLE_VISITOR)) {
            comp = new Comparison(comparisonType, col, param, false);
        } else {
            // this condition will always evaluate to true, but need to
            // add the parameter, so it can be set later
            comp = new Comparison(Comparison.EQUAL_NULL_SAFE, param, param, false);
        }
        comp = comp.optimize(session);
        if (isWindowQuery) {
            qualify = addGlobalCondition(qualify, comp);
        } else if (isGroupQuery) {
            for (int i = 0; groupIndex != null && i < groupIndex.length; i++) {
                if (groupIndex[i] == columnId) {
                    condition = addGlobalCondition(condition, comp);
                    return;
                }
            }
            if (havingIndex >= 0) {
                having = expressions.get(havingIndex);
            }
            having = addGlobalCondition(having, comp);
        } else {
            condition = addGlobalCondition(condition, comp);
        }
    }

    private static Expression addGlobalCondition(Expression condition, Expression additional) {
        if (!(condition instanceof ConditionLocalAndGlobal)) {
            return new ConditionLocalAndGlobal(condition, additional);
        }
        Expression oldLocal, oldGlobal;
        if (condition.getSubexpressionCount() == 1) {
            oldLocal = null;
            oldGlobal = condition.getSubexpression(0);
        } else {
            oldLocal = condition.getSubexpression(0);
            oldGlobal = condition.getSubexpression(1);
        }
        return new ConditionLocalAndGlobal(oldLocal, new ConditionAndOr(ConditionAndOr.AND, oldGlobal, additional));
    }

    @Override
    public void updateAggregate(SessionLocal s, int stage) {
        for (Expression e : expressions) {
            e.updateAggregate(s, stage);
        }
        if (condition != null) {
            condition.updateAggregate(s, stage);
        }
        if (having != null) {
            having.updateAggregate(s, stage);
        }
        if (qualify != null) {
            qualify.updateAggregate(s, stage);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC: {
            if (forUpdate != null) {
                return false;
            }
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
            if (!getDatabase().getSettings().optimizeEvaluatableSubqueries) {
                return false;
            }
            break;
        }
        case ExpressionVisitor.GET_DEPENDENCIES: {
            for (TableFilter f : filters) {
                Table table = f.getTable();
                visitor.addDependency(table);
                table.addDependencies(visitor.getDependencies());
            }
            break;
        }
        default:
        }
        ExpressionVisitor v2 = visitor.incrementQueryLevel(1);
        for (Expression e : expressions) {
            if (!e.isEverything(v2)) {
                return false;
            }
        }
        for (TableFilter f : filters) {
            Expression c = f.getJoinCondition();
            if (c != null && !c.isEverything(v2)) {
                return false;
            }
        }
        if (condition != null && !condition.isEverything(v2)) {
            return false;
        }
        if (having != null && !having.isEverything(v2)) {
            return false;
        }
        if (qualify != null && !qualify.isEverything(v2)) {
            return false;
        }
        return true;
    }


    @Override
    public boolean isCacheable() {
        return forUpdate == null;
    }

    @Override
    public boolean allowGlobalConditions() {
        return offsetExpr == null && fetchExpr == null && distinctExpressions == null;
    }

    public SortOrder getSortOrder() {
        return sort;
    }

    /**
     * Returns parent select, or null.
     *
     * @return parent select, or null
     */
    public Select getParentSelect() {
        return parentSelect;
    }

    @Override
    public boolean isConstantQuery() {
        if (!super.isConstantQuery() || distinctExpressions != null || condition != null || isGroupQuery
                || isWindowQuery || !isNoFromClause()) {
            return false;
        }
        for (int i = 0; i < visibleColumnCount; i++) {
            if (!expressions.get(i).isConstant()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Expression getIfSingleRow() {
        if (offsetExpr != null || fetchExpr != null || condition != null || isGroupQuery || isWindowQuery
                || !isNoFromClause()) {
            return null;
        }
        if (visibleColumnCount == 1) {
            return expressions.get(0);
        }
        Expression[] array = new Expression[visibleColumnCount];
        for (int i = 0; i < visibleColumnCount; i++) {
            array[i] = expressions.get(i);
        }
        return new ExpressionList(array, false);
    }

    private boolean isNoFromClause() {
        if (topTableFilter != null) {
            return topTableFilter.isNoFromClauseFilter();
        } else if (topFilters.size() == 1) {
            return topFilters.get(0).isNoFromClauseFilter();
        }
        return false;
    }

    /**
     * Lazy execution for this select.
     */
    private abstract class LazyResultSelect extends LazyResult {

        long rowNumber;
        int columnCount;

        LazyResultSelect(Expression[] expressions, int columnCount) {
            super(getSession(), expressions);
            this.columnCount = columnCount;
            setCurrentRowNumber(0);
        }

        @Override
        public final int getVisibleColumnCount() {
            return visibleColumnCount;
        }

        @Override
        public void reset() {
            super.reset();
            topTableFilter.reset();
            setCurrentRowNumber(0);
            rowNumber = 0;
        }
    }

    /**
     * Lazy execution for a flat query.
     */
    private final class LazyResultQueryFlat extends LazyResultSelect {

        private boolean forUpdate;

        LazyResultQueryFlat(Expression[] expressions, int columnCount, boolean forUpdate) {
            super(expressions, columnCount);
            this.forUpdate = forUpdate;
        }

        @Override
        protected Value[] fetchNextRow() {
            while (topTableFilter.next()) {
                setCurrentRowNumber(rowNumber + 1);
                // This method may lock rows
                if (forUpdate ? isConditionMetForUpdate() : isConditionMet()) {
                    ++rowNumber;
                    Value[] row = new Value[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        Expression expr = expressions.get(i);
                        row[i] = expr.getValue(getSession());
                    }
                    return row;
                }
            }
            return null;
        }

        @Override
        protected boolean skipNextRow() {
            while (topTableFilter.next()) {
                setCurrentRowNumber(rowNumber + 1);
                // This method does not lock rows
                if (isConditionMet()) {
                    ++rowNumber;
                    return true;
                }
            }
            return false;
        }

    }

    /**
     * Lazy execution for a group sorted query.
     */
    private final class LazyResultGroupSorted extends LazyResultSelect {

        private Value[] previousKeyValues;

        LazyResultGroupSorted(Expression[] expressions, int columnCount) {
            super(expressions, columnCount);
            if (groupData == null) {
                setGroupData(SelectGroups.getInstance(getSession(), Select.this.expressions, isGroupQuery,
                        groupIndex));
            } else {
                updateAgg(columnCount, DataAnalysisOperation.STAGE_RESET);
                groupData.resetLazy();
            }
        }

        @Override
        public void reset() {
            super.reset();
            groupData.resetLazy();
            previousKeyValues = null;
        }

        @Override
        protected Value[] fetchNextRow() {
            while (topTableFilter.next()) {
                setCurrentRowNumber(rowNumber + 1);
                if (isConditionMet()) {
                    rowNumber++;
                    int groupSize = groupIndex.length;
                    Value[] keyValues = new Value[groupSize];
                    // update group
                    for (int i = 0; i < groupSize; i++) {
                        int idx = groupIndex[i];
                        Expression expr = expressions.get(idx);
                        keyValues[i] = expr.getValue(getSession());
                    }

                    Value[] row = null;
                    if (previousKeyValues == null) {
                        previousKeyValues = keyValues;
                        groupData.nextLazyGroup();
                    } else {
                        SessionLocal session = getSession();
                        for (int i = 0; i < groupSize; i++) {
                            if (session.compare(previousKeyValues[i], keyValues[i]) != 0) {
                                row = createGroupSortedRow(previousKeyValues, columnCount);
                                previousKeyValues = keyValues;
                                groupData.nextLazyGroup();
                                break;
                            }
                        }
                    }
                    groupData.nextLazyRow();
                    updateAgg(columnCount, DataAnalysisOperation.STAGE_GROUP);
                    if (row != null) {
                        return row;
                    }
                }
            }
            Value[] row = null;
            if (previousKeyValues != null) {
                row = createGroupSortedRow(previousKeyValues, columnCount);
                previousKeyValues = null;
            }
            return row;
        }
    }

}
