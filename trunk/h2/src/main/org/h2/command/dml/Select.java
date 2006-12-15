/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;

import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.expression.Comparison;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.ValueExpression;
import org.h2.expression.Wildcard;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.util.ValueHashMap;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueInt;
import org.h2.value.ValueNull;


/**
 * visibleColumnCount <= distinctColumnCount <= expressionCount
 * Sortable count could include ORDER BY expressions that are not in the select list
 * Expression count could include GROUP BY expressions
 *
 * init
 * (maybe additional mapColumns if it's a subquery)
 * prepare
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
    private boolean isGroupQuery;
    private boolean isForUpdate;
    private double cost;
    private boolean isQuickQuery;
    private boolean isPrepared, checkInit;
    private SortOrder sort;

    public Select(Session session) {
        super(session);
    }

    public void addTableFilter(TableFilter filter, boolean isTop) {
        // TODO compatibility: it seems oracle doesn't check on duplicate aliases; do other databases check it?
//        String alias = filter.getAlias();
//        if(filterNames.contains(alias)) {
//            throw Message.getSQLException(Message.DUPLICATE_TABLE_ALIAS, alias);
//        }
//        filterNames.add(alias);
        filters.add(filter);
        if(isTop) {
            topFilters.add(filter);
        }
    }

    public ObjectArray getTopFilters() {
        return topFilters;
    }

    public void setExpressions(ObjectArray expressions) {
        this.expressions = expressions;
    }

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

    public void addCondition(Expression cond) {
        if(condition == null) {
            condition = cond;
        } else {
            condition = new ConditionAndOr(ConditionAndOr.AND, cond, condition);
        }
    }

    private void queryGroup(int columnCount, LocalResult result) throws SQLException {
        ValueHashMap groups = new ValueHashMap(session.getDatabase());
        int rowNumber = 0;
        setCurrentRowNumber(0);
        ValueArray defaultGroup = ValueArray.get(new Value[0]);
        while (topTableFilter.next()) {
            checkCancelled();
            setCurrentRowNumber(rowNumber+1);
            if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                Value key;
                rowNumber++;
                if(groupIndex == null) {
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
                HashMap values = (HashMap)groups.get(key);
                if(values == null) {
                    values = new HashMap();
                    groups.put(key, values);
                }
                currentGroup = values;
                int len = columnCount;
                for (int i = 0; i < len; i++) {
                    if(groupByExpression == null || !groupByExpression[i]) {
                        Expression expr = (Expression) expressions.get(i);
                        expr.updateAggregate(session);
                    }
                }
                if(sampleSize > 0 && rowNumber >= sampleSize) {
                    break;
                }
            }
        }
        if(groupIndex == null && groups.size()==0) {
            groups.put(defaultGroup, new HashMap());
        }
        ObjectArray keys = groups.keys();
        for (int i=0; i<keys.size(); i++) {
            ValueArray key = (ValueArray) keys.get(i);
            currentGroup = (HashMap) groups.get(key);
            Value[] keyValues = key.getList();
            Value[] row = new Value[columnCount];
            for (int j=0; groupIndex != null && j<groupIndex.length; j++) {
                row[groupIndex[j]] = keyValues[j];
            }
            for (int j = 0; j < columnCount; j++) {
                if(groupByExpression != null && groupByExpression[j]) {
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
            if(columnCount != distinctColumnCount) {
                // remove columns so that 'distinct' can filter duplicate rows
                Value[] r2 = new Value[distinctColumnCount];
                System.arraycopy(row, 0, r2, 0, distinctColumnCount);
                row = r2;
            }
            result.addRow(row);
        }
    }

    private Index getSortIndex() throws SQLException {
        if(sort == null) {
            return null;
        }
        int[] sortTypes = sort.getSortTypes();
        for(int i=0; i<sortTypes.length; i++) {
            if((sortTypes[i] & (SortOrder.DESCENDING | SortOrder.NULLS_LAST)) != 0) {
                return null;
            }
        }
        int[] indexes = sort.getIndexes();
        ObjectArray sortColumns = new ObjectArray();
        for(int i=0; i<indexes.length; i++) {
            int idx = indexes[i];
            if(idx < 0 || idx >= expressions.size()) {
                throw Message.getInvalidValueException("order by", ""+idx);
            }
            Expression expr = (Expression) expressions.get(idx);
            expr = expr.getNonAliasExpression();
            if(expr.isConstant()) {
                continue;
            }
            if(!(expr instanceof ExpressionColumn)) {
                return null;
            }
            Column col = ((ExpressionColumn) expr).getColumn();
            if(col.getTable() != topTableFilter.getTable()) {
                return null;
            }
            sortColumns.add(col);
        }
        Column[] sortCols = new Column[sortColumns.size()];
        sortColumns.toArray(sortCols);
        if(sortCols.length == 0) {
            // sort just on constants - can use scan index
            return topTableFilter.getTable().getScanIndex(session);
        }
        ObjectArray list = topTableFilter.getTable().getIndexes();
        for(int i=0; list != null && i<list.size(); i++) {
            Index index = (Index) list.get(i);
            if(index.getCreateSQL() == null) {
                // can't use the scan index
                continue;
            }
            if(index.indexType.isHash()) {
                continue;
            }
            Column[] indexCols = index.getColumns();
            if(indexCols.length < sortCols.length) {
                continue;
            }
            boolean ok = true;
            for(int j=0; j<sortCols.length; j++) {
                // the index and the sort order must start with the exact same columns
                if(indexCols[j] != sortCols[j]) {
                    ok = false;
                    break;
                }
            }
            if(ok) {
                return index;
            }
        }
        return null;
    }

    private void queryFlat(int columnCount, LocalResult result) throws SQLException {
        int limitRows;
        if(limit == null) {
            limitRows = 0;
        } else {
            limitRows = limit.getValue(session).getInt();
            if(offset != null) {
                limitRows += offset.getValue(session).getInt();
            }
        }
        int rowNumber = 0;
        setCurrentRowNumber(0);
        while (topTableFilter.next()) {
            checkCancelled();
            setCurrentRowNumber(rowNumber+1);
            if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                Value[] row = new Value[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    Expression expr = (Expression) expressions.get(i);
                    row[i] = expr.getValue(session);
                }
                result.addRow(row);
                rowNumber++;
                if(sort == null && limitRows != 0 && result.getRowCount() >= limitRows) {
                    break;
                }
                if(sampleSize > 0 && rowNumber >= sampleSize) {
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

    public LocalResult queryWithoutCache(int maxrows) throws SQLException {
        if(maxrows != 0) {
            if(limit != null) {
                maxrows = Math.min(limit.getValue(session).getInt(), maxrows);
            }
            limit = ValueExpression.get(ValueInt.get(maxrows));
        }
        int columnCount = expressions.size();
        LocalResult result = new LocalResult(session, expressions, visibleColumnCount);
        result.setSortOrder(sort);
        if(distinct) {
            result.setDistinct();
        }
        topTableFilter.startQuery();
        topTableFilter.reset();
        // TODO lock tables of subqueries
        topTableFilter.lock(session, isForUpdate);
        if(isQuickQuery) {
            queryQuick(columnCount, result);
        } else if(isGroupQuery) {
            queryGroup(columnCount, result);
        } else {
            queryFlat(columnCount, result);
        }
        if(offset != null) {
            result.setOffset(offset.getValue(session).getInt());
        }
        if(limit != null) {
            result.setLimit(limit.getValue(session).getInt());
        }
        result.done();
        return result;
    }

    private void expandColumnList() throws SQLException {
        // TODO this works: select distinct count(*) from system_columns group by table
        for (int i = 0; i < expressions.size(); i++) {
            Expression expr = (Expression) expressions.get(i);
            if (!expr.isWildcard()) {
                continue;
            }
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
                        filter = f;
                        break;
                    }
                }
                if (filter == null) {
                    throw Message.getSQLException(Message.TABLE_OR_VIEW_NOT_FOUND_1, tableAlias);
                }
                Table t = filter.getTable();
                String alias = filter.getTableAlias();
                expressions.remove(i);
                Column[] columns = t.getColumns();
                for (int j = 0; j < columns.length; j++) {
                    Column c = columns[j];
                    ExpressionColumn ec = new ExpressionColumn(session.getDatabase(), this, null, alias, c.getName());
                    expressions.add(i++, ec);
                }
                i--;
            }
        }
    }

    public void init() throws SQLException {
        if(Constants.CHECK && checkInit) {
            throw Message.getInternalError();
        }
        checkInit = true;
        expandColumnList();
        visibleColumnCount = expressions.size();
        if(orderList != null) {
            sort = initOrder(expressions, orderList, visibleColumnCount, distinct);
            orderList = null;
        }
        distinctColumnCount = expressions.size();

        if(having != null) {
            expressions.add(having);
            havingIndex = expressions.size()-1;
            having = null;
        } else {
            havingIndex = -1;
        }

        // first visible columns, then order by, then having, and then group by at the end
        if(group != null) {
            groupIndex = new int[group.size()];
            ObjectArray expressionSQL = new ObjectArray();
            for(int i=0; i<expressions.size(); i++) {
                Expression expr = (Expression) expressions.get(i);
                expr = expr.getNonAliasExpression();
                String sql = expr.getSQL();
                expressionSQL.add(sql);
            }
            for(int i=0; i<group.size(); i++) {
                Expression expr = (Expression) group.get(i);
                String sql = expr.getSQL();
                int found = -1;
                for(int j=0; j<expressionSQL.size(); j++) {
                    String s2 = (String) expressionSQL.get(j);
                    if(s2.equals(sql)) {
                        found = j;
                        break;
                    }
                }
                if(found < 0) {
                    int index = expressions.size();
                    groupIndex[i] = index;
                    expressions.add(expr);
                } else {
                    groupIndex[i] = found;
                }
            }
            groupByExpression = new boolean[expressions.size()];
            for(int i=0; i<groupIndex.length; i++) {
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
    }

    public void prepare() throws SQLException {
        if(isPrepared) {
            // TODO optimization: sometimes a subquery is prepared twice. why?
            return;
        }
        if(Constants.CHECK && !checkInit) {
            throw Message.getInternalError("already prepared");
        }
        isPrepared = true;
        for(int i=0; i<expressions.size(); i++) {
            Expression e = (Expression) expressions.get(i);
            expressions.set(i, e.optimize(session));
        }
        if(condition != null) {
            condition = condition.optimize(session);
            for (int j = 0; j < filters.size(); j++) {
                TableFilter f = (TableFilter) filters.get(j);
                condition.createIndexConditions(f);
            }
        }
        if(condition == null && isGroupQuery && groupIndex == null && havingIndex<0 && filters.size()==1) {
            ExpressionVisitor optimizable = ExpressionVisitor.get(ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL);
            optimizable.table = ((TableFilter)filters.get(0)).getTable();
            isQuickQuery = isEverything(optimizable);
        }
        cost = preparePlan();
        if(sort != null && !isQuickQuery && !isGroupQuery) {
            Index index = getSortIndex();
            Index current = topTableFilter.getIndex();
            if(index != null && (current.indexType.isScan() || current == index)) {
                topTableFilter.setIndex(index);
                sort = null;
            }
        }
    }

    public double getCost() {
        return cost;
    }

    public HashSet getTables() {
        HashSet set = new HashSet();
        for(int i=0; i<filters.size(); i++) {
            TableFilter filter = (TableFilter) filters.get(i);
            set.add(filter.getTable());
        }
        return set;
    }

    private double preparePlan() throws SQLException {

        TableFilter[] topArray = new TableFilter[topFilters.size()];
        topFilters.toArray(topArray);
        for(int i=0; i<topArray.length; i++) {
            topArray[i].setFullCondition(condition);
        }

        Optimizer optimizer = new Optimizer(topArray, condition, session);
        optimizer.optimize();
        topTableFilter = optimizer.getTopFilter();
        double cost = optimizer.getCost();

        TableFilter f = topTableFilter;
        while(f != null) {
            f.setEvaluatable(f, true);
            if(condition != null) {
                condition.setEvaluatable(f, true);
            }
            Expression on = f.getJoinCondition();
            if(on != null) {
                if(!on.isEverything(ExpressionVisitor.EVALUATABLE)) {
                    f.removeJoinCondition();
                    addCondition(on);
                }
            }
            on = f.getFilterCondition();
            if(on != null) {
                if(!on.isEverything(ExpressionVisitor.EVALUATABLE)) {
                    f.removeFilterCondition();
                    addCondition(on);
                }
            }
            f = f.getJoin();
        }

        topTableFilter.prepare();
        return cost;
    }

    public String getPlan() {
        StringBuffer buff = new StringBuffer();
        Expression[] exprList = new Expression[expressions.size()];
        expressions.toArray(exprList);
        buff.append("SELECT ");
        if(distinct) {
            buff.append("DISTINCT ");
        }
        for(int i=0; i<visibleColumnCount; i++) {
            if(i>0) {
                buff.append(", ");
            }
            Expression expr = exprList[i];
            buff.append(StringUtils.unEnclose(expr.getSQL()));
        }
        buff.append("\nFROM ");
        TableFilter filter = topTableFilter;
        boolean join = false;
        int id=0;
        do {
            if(id > 0) {
                buff.append('\n');
            }
            id++;
            buff.append(filter.getPlanSQL(join));
            join = true;
            filter = filter.getJoin();
        } while(filter != null);
        if(condition != null) {
            buff.append("\nWHERE " + StringUtils.unEnclose(condition.getSQL()));
        }
        if(groupIndex != null) {
            buff.append("\nGROUP BY ");
            for(int i=0; i<groupIndex.length; i++) {
                Expression gro = exprList[groupIndex[i]];
                if(i>0) {
                    buff.append(", ");
                }
                buff.append(StringUtils.unEnclose(gro.getSQL()));
            }
        }
        if(havingIndex >= 0) {
            Expression hav = exprList[havingIndex];
            buff.append("\nHAVING " + StringUtils.unEnclose(hav.getSQL()));
        }
        if(sort != null) {
            buff.append("\nORDER BY ");
            buff.append(sort.getSQL(exprList, visibleColumnCount));
        }
        if(limit != null) {
            buff.append("\nLIMIT ");
            buff.append(StringUtils.unEnclose(limit.getSQL()));
            if(offset != null) {
                buff.append(" OFFSET ");
                buff.append(StringUtils.unEnclose(offset.getSQL()));
            }
        }
        if(isForUpdate) {
            buff.append("\nFOR UPDATE");
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

    public Expression getCondition() {
        return condition;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public ObjectArray getGroupBy() {
        return group;
    }

    public void setForUpdate(boolean b) {
        this.isForUpdate = b;
    }

    public void mapColumns(ColumnResolver resolver, int level) throws SQLException {
        for(int i=0; i<expressions.size(); i++) {
            Expression e = (Expression) expressions.get(i);
            e.mapColumns(resolver, level);
        }
        if(condition != null) {
            condition.mapColumns(resolver, level);
        }
    }

    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for(int i=0; i<expressions.size(); i++) {
            Expression e = (Expression) expressions.get(i);
            e.setEvaluatable(tableFilter, b);
        }
        if(condition != null) {
            condition.setEvaluatable(tableFilter, b);
        }
    }

    public boolean isQuickQuery() {
        return isQuickQuery;
    }

    public void addGlobalCondition(Expression expr, int columnId, int comparisonType) throws SQLException {
        Expression col = (Expression)expressions.get(columnId);
        Expression comp = new Comparison(session, comparisonType, col, expr);
        comp = comp.optimize(session);
        if(isGroupQuery) {
            if(having == null) {
                having = comp;
            } else {
                having = new ConditionAndOr(ConditionAndOr.AND, having, comp);
            }
        } else {
            if(condition == null) {
                condition = comp;
            } else {
                condition = new ConditionAndOr(ConditionAndOr.AND, condition, comp);
            }
        }
    }

    public boolean isEverything(ExpressionVisitor visitor) {
        if(visitor.type == ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID) {
            for(int i=0; i<filters.size(); i++) {
                TableFilter f = (TableFilter) filters.get(i);
                long m = f.getTable().getMaxDataModificationId();
                visitor.addDataModificationId(m);
            }
        }
        if(visitor.type == ExpressionVisitor.EVALUATABLE) {
            if(!Constants.OPTIMIZE_EVALUATABLE_SUBQUERIES) {
                return false;
            }
        }
        if(visitor.type != ExpressionVisitor.EVALUATABLE) {
            visitor.queryLevel(1);
        }
        boolean result = true;
        for(int i=0; i<expressions.size(); i++) {
            Expression e = (Expression) expressions.get(i);
            if(!e.isEverything(visitor)) {
                result = false;
                break;
            }
        }
        if(result && condition != null && !condition.isEverything(visitor)) {
            result = false;
        }
        if(result && having != null && !having.isEverything(visitor)) {
            result = false;
        }
        if(visitor.type != ExpressionVisitor.EVALUATABLE) {
            visitor.queryLevel(-1);
        }
        return result;
    }

}
