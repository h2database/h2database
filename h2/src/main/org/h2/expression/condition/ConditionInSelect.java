/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
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
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;

/**
 * An 'in' condition with a subquery, as in WHERE ID IN(SELECT ...)
 */
public class ConditionInSelect extends Condition {

    private final Database database;
    private Expression left;
    private final Query query;
    private final boolean all;
    private final int compareType;

    public ConditionInSelect(Database database, Expression left, Query query,
            boolean all, int compareType) {
        this.database = database;
        this.left = left;
        this.query = query;
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
        if (all || (compareType != Comparison.EQUAL &&
                compareType != Comparison.EQUAL_NULL_SAFE)) {
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
            int dataType = rows.getColumnType(0);
            if (dataType == Value.NULL) {
                return ValueBoolean.FALSE;
            }
            if (l.getType() == Value.ROW) {
                Value[] leftList = ((ValueRow) l).getList();
                if (leftList.length != 1) {
                    throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
                }
                l = leftList[0];
            }
            l = l.convertTo(dataType, database.getMode());
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
        if (all) {
            while (rows.next()) {
                Value[] currentRow = rows.currentRow();
                Value r = query.getColumnCount() == 1 ? currentRow[0] : ValueRow.get(currentRow);
                Value cmp = Comparison.compare(database, l, r, compareType);
                if (cmp == ValueNull.INSTANCE) {
                    return ValueNull.INSTANCE;
                } else if (cmp == ValueBoolean.FALSE) {
                    return cmp;
                }
            }
            return ValueBoolean.TRUE;
        } else {
            boolean hasNull = false;
            while (rows.next()) {
                Value[] currentRow = rows.currentRow();
                Value r = query.getColumnCount() == 1 ? currentRow[0] : ValueRow.get(currentRow);
                Value cmp = Comparison.compare(database, l, r, compareType);
                if (cmp == ValueNull.INSTANCE) {
                    hasNull = true;
                } else if (cmp == ValueBoolean.TRUE) {
                    return cmp;
                }
            }
            if (hasNull) {
                return ValueNull.INSTANCE;
            }
            return ValueBoolean.FALSE;
        }
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
        query.mapColumns(resolver, level + 1);
    }

    @Override
    public Expression optimize(Session session) {
        left = left.optimize(session);
        query.setRandomAccessResult(true);
        session.optimizeQueryExpression(query);
        // Can not optimize: the data may change
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        query.setEvaluatable(tableFilter, b);
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder) {
        builder.append('(');
        left.getSQL(builder).append(' ');
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
        builder.append("(\n");
        return StringUtils.indent(builder, query.getPlanSQL(), 4, false).append("))");
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        left.updateAggregate(session, stage);
        query.updateAggregate(session, stage);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && query.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return left.getCost() + query.getCostAsExpression();
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
