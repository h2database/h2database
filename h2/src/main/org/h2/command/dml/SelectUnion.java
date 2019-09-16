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
import org.h2.result.LazyResult;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.ColumnNamer;
import org.h2.value.Value;

/**
 * Represents a union SELECT statement.
 */
public class SelectUnion extends Query {

    public enum UnionType {
        /**
         * The type of a UNION statement.
         */
        UNION,

        /**
         * The type of a UNION ALL statement.
         */
        UNION_ALL,

        /**
         * The type of an EXCEPT statement.
         */
        EXCEPT,

        /**
         * The type of an INTERSECT statement.
         */
        INTERSECT
    }

    private final UnionType unionType;

    /**
     * The left hand side of the union (the first subquery).
     */
    final Query left;

    /**
     * The right hand side of the union (the second subquery).
     */
    final Query right;

    private boolean isPrepared, checkInit;
    private boolean isForUpdate;

    public SelectUnion(Session session, UnionType unionType, Query query, Query right) {
        super(session);
        this.unionType = unionType;
        this.left = query;
        this.right = right;
    }

    @Override
    public boolean isUnion() {
        return true;
    }

    @Override
    public void prepareJoinBatch() {
        left.prepareJoinBatch();
        right.prepareJoinBatch();
    }

    public UnionType getUnionType() {
        return unionType;
    }

    public Query getLeft() {
        return left;
    }

    public Query getRight() {
        return right;
    }

    private Value[] convert(Value[] values, int columnCount) {
        Value[] newValues;
        if (columnCount == values.length) {
            // re-use the array if possible
            newValues = values;
        } else {
            // create a new array if needed,
            // for the value hash set
            newValues = new Value[columnCount];
        }
        for (int i = 0; i < columnCount; i++) {
            Expression e = expressions.get(i);
            newValues[i] = values[i].convertTo(e.getType(), session, false, null);
        }
        return newValues;
    }

    public LocalResult getEmptyResult() {
        int columnCount = left.getColumnCount();
        return createLocalResult(columnCount);
    }

    @Override
    protected ResultInterface queryWithoutCache(int maxRows, ResultTarget target) {
        OffsetFetch offsetFetch = getOffsetFetch(maxRows);
        long offset = offsetFetch.offset;
        int fetch = offsetFetch.fetch;
        boolean fetchPercent = offsetFetch.fetchPercent;
        Database db = session.getDatabase();
        if (db.getSettings().optimizeInsertFromSelect) {
            if (unionType == UnionType.UNION_ALL && target != null) {
                if (sort == null && !distinct && fetch < 0 && offset == 0) {
                    left.query(0, target);
                    right.query(0, target);
                    return null;
                }
            }
        }
        int columnCount = left.getColumnCount();
        if (session.isLazyQueryExecution() && unionType == UnionType.UNION_ALL && !distinct &&
                sort == null && !randomAccessResult && !isForUpdate &&
                offset == 0 && !fetchPercent && !withTies && isReadOnly()) {
            // limit 0 means no rows
            if (fetch != 0) {
                LazyResultUnion lazyResult = new LazyResultUnion(expressionArray, columnCount);
                if (fetch > 0) {
                    lazyResult.setLimit(fetch);
                }
                return lazyResult;
            }
        }
        LocalResult result = createLocalResult(columnCount);
        if (sort != null) {
            result.setSortOrder(sort);
        }
        if (distinct) {
            left.setDistinctIfPossible();
            right.setDistinctIfPossible();
            result.setDistinct();
        }
        switch (unionType) {
        case UNION:
        case EXCEPT:
            left.setDistinctIfPossible();
            right.setDistinctIfPossible();
            result.setDistinct();
            break;
        case UNION_ALL:
            break;
        case INTERSECT:
            left.setDistinctIfPossible();
            right.setDistinctIfPossible();
            break;
        default:
            DbException.throwInternalError("type=" + unionType);
        }
        ResultInterface l = left.query(0);
        ResultInterface r = right.query(0);
        l.reset();
        r.reset();
        switch (unionType) {
        case UNION_ALL:
        case UNION: {
            while (l.next()) {
                result.addRow(convert(l.currentRow(), columnCount));
            }
            while (r.next()) {
                result.addRow(convert(r.currentRow(), columnCount));
            }
            break;
        }
        case EXCEPT: {
            while (l.next()) {
                result.addRow(convert(l.currentRow(), columnCount));
            }
            while (r.next()) {
                result.removeDistinct(convert(r.currentRow(), columnCount));
            }
            break;
        }
        case INTERSECT: {
            LocalResult temp = createLocalResult(columnCount);
            temp.setDistinct();
            while (l.next()) {
                temp.addRow(convert(l.currentRow(), columnCount));
            }
            while (r.next()) {
                Value[] values = convert(r.currentRow(), columnCount);
                if (temp.containsDistinct(values)) {
                    result.addRow(values);
                }
            }
            temp.close();
            break;
        }
        default:
            DbException.throwInternalError("type=" + unionType);
        }
        l.close();
        r.close();
        return finishResult(result, offset, fetch, fetchPercent, target);
    }

    private LocalResult createLocalResult(int columnCount) {
        return session.getDatabase().getResultFactory().create(session, expressionArray, columnCount, columnCount);
    }

    @Override
    public void init() {
        if (checkInit) {
            DbException.throwInternalError();
        }
        checkInit = true;
        left.init();
        right.init();
        int len = left.getColumnCount();
        if (len != right.getColumnCount()) {
            throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
        }
        ArrayList<Expression> le = left.getExpressions();
        // set the expressions to get the right column count and names,
        // but can't validate at this time
        expressions = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            Expression l = le.get(i);
            expressions.add(l);
        }
        visibleColumnCount = len;
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
        left.prepare();
        right.prepare();
        int len = left.getColumnCount();
        // set the correct expressions now
        expressions = new ArrayList<>(len);
        ArrayList<Expression> le = left.getExpressions();
        ArrayList<Expression> re = right.getExpressions();
        ColumnNamer columnNamer= new ColumnNamer(session);
        for (int i = 0; i < len; i++) {
            Expression l = le.get(i);
            Expression r = re.get(i);
            String columnName = columnNamer.getColumnName(l, i, l.getAlias());
            Column col = new Column(columnName, Value.getHigherType(l.getType(), r.getType()));
            Expression e = new ExpressionColumn(session.getDatabase(), col);
            expressions.add(e);
        }
        if (orderList != null) {
            initOrder(session, expressions, null, orderList, getColumnCount(), true, null);
            sort = prepareOrder(orderList, expressions.size());
            orderList = null;
        }
        resultColumnCount = expressions.size();
        expressionArray = expressions.toArray(new Expression[0]);
    }

    @Override
    public double getCost() {
        return left.getCost() + right.getCost();
    }

    @Override
    public HashSet<Table> getTables() {
        HashSet<Table> set = left.getTables();
        set.addAll(right.getTables());
        return set;
    }

    @Override
    public void setForUpdate(boolean forUpdate) {
        left.setForUpdate(forUpdate);
        right.setForUpdate(forUpdate);
        isForUpdate = forUpdate;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        left.mapColumns(resolver, level);
        right.mapColumns(resolver, level);
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        right.setEvaluatable(tableFilter, b);
    }

    @Override
    public void addGlobalCondition(Parameter param, int columnId,
            int comparisonType) {
        addParameter(param);
        switch (unionType) {
        case UNION_ALL:
        case UNION:
        case INTERSECT: {
            left.addGlobalCondition(param, columnId, comparisonType);
            right.addGlobalCondition(param, columnId, comparisonType);
            break;
        }
        case EXCEPT: {
            left.addGlobalCondition(param, columnId, comparisonType);
            break;
        }
        default:
            DbException.throwInternalError("type=" + unionType);
        }
    }

    @Override
    public String getPlanSQL(boolean alwaysQuote) {
        StringBuilder buff = new StringBuilder();
        buff.append('(').append(left.getPlanSQL(alwaysQuote)).append(')');
        switch (unionType) {
        case UNION_ALL:
            buff.append("\nUNION ALL\n");
            break;
        case UNION:
            buff.append("\nUNION\n");
            break;
        case INTERSECT:
            buff.append("\nINTERSECT\n");
            break;
        case EXCEPT:
            buff.append("\nEXCEPT\n");
            break;
        default:
            DbException.throwInternalError("type=" + unionType);
        }
        buff.append('(').append(right.getPlanSQL(alwaysQuote)).append(')');
        appendEndOfQueryToSQL(buff, alwaysQuote, expressions.toArray(new Expression[0]));
        if (sampleSizeExpr != null) {
            buff.append("\nSAMPLE_SIZE ");
            sampleSizeExpr.getUnenclosedSQL(buff, alwaysQuote);
        }
        if (isForUpdate) {
            buff.append("\nFOR UPDATE");
        }
        return buff.toString();
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && right.isEverything(visitor);
    }

    @Override
    public void updateAggregate(Session s, int stage) {
        left.updateAggregate(s, stage);
        right.updateAggregate(s, stage);
    }

    @Override
    public void fireBeforeSelectTriggers() {
        left.fireBeforeSelectTriggers();
        right.fireBeforeSelectTriggers();
    }

    @Override
    public boolean allowGlobalConditions() {
        return left.allowGlobalConditions() && right.allowGlobalConditions();
    }

    /**
     * Lazy execution for this union.
     */
    private final class LazyResultUnion extends LazyResult {

        int columnCount;
        ResultInterface l;
        ResultInterface r;
        boolean leftDone;
        boolean rightDone;

        LazyResultUnion(Expression[] expressions, int columnCount) {
            super(expressions);
            this.columnCount = columnCount;
        }

        @Override
        public int getVisibleColumnCount() {
            return columnCount;
        }

        @Override
        protected Value[] fetchNextRow() {
            if (rightDone) {
                return null;
            }
            if (!leftDone) {
                if (l == null) {
                    l = left.query(0);
                    l.reset();
                }
                if (l.next()) {
                    return l.currentRow();
                }
                leftDone = true;
            }
            if (r == null) {
                r = right.query(0);
                r.reset();
            }
            if (r.next()) {
                return r.currentRow();
            }
            rightDone = true;
            return null;
        }

        @Override
        public void close() {
            super.close();
            if (l != null) {
                l.close();
            }
            if (r != null) {
                r.close();
            }
        }

        @Override
        public void reset() {
            super.reset();
            if (l != null) {
                l.reset();
            }
            if (r != null) {
                r.reset();
            }
            leftDone = false;
            rightDone = false;
        }
    }
}
