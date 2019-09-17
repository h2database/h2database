/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import org.h2.api.ErrorCode;
import org.h2.command.dml.Query;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.index.IndexCondition;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;

/**
 * An IN() condition with a subquery, as in WHERE ID IN(SELECT ...)
 */
public class ConditionInQuery extends PredicateWithSubquery {

    private final Database database;
    private Expression left;
    private final boolean all;
    private final int compareType;

    public ConditionInQuery(Database database, Expression left, Query query, boolean all, int compareType) {
        super(query);
        this.database = database;
        this.left = left;
        /*
         * Need to do it now because other methods may be invoked in different
         * order.
         */
        query.setRandomAccessResult(true);
        this.all = all;
        this.compareType = compareType;
    }

    @Override
    public Value getValue(Session session) {
        query.setSession(session);
        // We need a LocalResult
        query.setNeverLazy(true);
        query.setDistinctIfPossible();
        LocalResult rows = (LocalResult) query.query(0);
        Value l = left.getValue(session);
        if (!rows.hasNext()) {
            return ValueBoolean.get(all);
        } else if (l.containsNull()) {
            return ValueNull.INSTANCE;
        }
        if (!database.getSettings().optimizeInSelect) {
            return getValueSlow(rows, l);
        }
        if (all || compareType != Comparison.EQUAL) {
            return getValueSlow(rows, l);
        }
        int columnCount = query.getColumnCount();
        if (columnCount != 1) {
            l = l.convertTo(Value.ROW);
            Value[] leftValue = ((ValueRow) l).getList();
            if (columnCount == leftValue.length && rows.containsDistinct(leftValue)) {
                return ValueBoolean.TRUE;
            }
        } else {
            TypeInfo colType = rows.getColumnType(0);
            if (colType.getValueType() == Value.NULL) {
                return ValueNull.INSTANCE;
            }
            if (l.getValueType() == Value.ROW) {
                Value[] leftList = ((ValueRow) l).getList();
                if (leftList.length != 1) {
                    throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
                }
                l = leftList[0];
            }
            l = l.convertTo(colType, database, true, null);
            if (rows.containsDistinct(new Value[] { l })) {
                return ValueBoolean.TRUE;
            }
        }
        if (rows.containsNull()) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.FALSE;
    }

    private Value getValueSlow(ResultInterface rows, Value l) {
        // this only returns the correct result if the result has at least one
        // row, and if l is not null
        boolean hasNull = false;
        if (all) {
            while (rows.next()) {
                Value cmp = compare(l, rows);
                if (cmp == ValueNull.INSTANCE) {
                    hasNull = true;
                } else if (cmp == ValueBoolean.FALSE) {
                    return cmp;
                }
            }
        } else {
            while (rows.next()) {
                Value cmp = compare(l, rows);
                if (cmp == ValueNull.INSTANCE) {
                    hasNull = true;
                } else if (cmp == ValueBoolean.TRUE) {
                    return cmp;
                }
            }
        }
        if (hasNull) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(all);
    }

    private Value compare(Value l, ResultInterface rows) {
        Value[] currentRow = rows.currentRow();
        Value r = l.getValueType() != Value.ROW && query.getColumnCount() == 1 ? currentRow[0]
                : ValueRow.get(currentRow);
        return Comparison.compare(database, l, r, compareType);
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
        super.mapColumns(resolver, level, state);
    }

    @Override
    public Expression optimize(Session session) {
        left = left.optimize(session);
        return super.optimize(session);
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        super.setEvaluatable(tableFilter, b);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        builder.append('(');
        left.getSQL(builder, alwaysQuote).append(' ');
        if (all) {
            builder.append(Comparison.getCompareOperator(compareType)).
                append(" ALL");
        } else {
            if (compareType == Comparison.EQUAL) {
                builder.append("IN");
            } else {
                builder.append(Comparison.getCompareOperator(compareType)).
                    append(" ANY");
            }
        }
        return super.getSQL(builder, alwaysQuote).append(')');
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        left.updateAggregate(session, stage);
        super.updateAggregate(session, stage);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && super.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return left.getCost() + super.getCost();
    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
        if (!session.getDatabase().getSettings().optimizeInList) {
            return;
        }
        if (compareType != Comparison.EQUAL) {
            return;
        }
        if (query.getColumnCount() != 1) {
            return;
        }
        if (!(left instanceof ExpressionColumn)) {
            return;
        }
        ExpressionColumn l = (ExpressionColumn) left;
        if (filter != l.getTableFilter()) {
            return;
        }
        ExpressionVisitor visitor = ExpressionVisitor.getNotFromResolverVisitor(filter);
        if (!query.isEverything(visitor)) {
            return;
        }
        filter.addIndexCondition(IndexCondition.getInQuery(l, query));
    }

}
