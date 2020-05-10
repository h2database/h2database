/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import org.h2.api.ErrorCode;
import org.h2.command.query.Query;
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
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;
import org.h2.value.ValueRow;

/**
 * An IN() condition with a subquery, as in WHERE ID IN(SELECT ...)
 */
public final class ConditionInQuery extends PredicateWithSubquery {

    private Expression left;
    private final boolean not;
    private final boolean whenOperand;
    private final boolean all;
    private final int compareType;

    public ConditionInQuery(Expression left, boolean not, boolean whenOperand, Query query, boolean all,
            int compareType) {
        super(query);
        this.left = left;
        this.not = not;
        this.whenOperand = whenOperand;
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
        return getValue(session, left.getValue(session));
    }

    @Override
    public boolean getWhenValue(Session session, Value left) {
        if (!whenOperand) {
            return super.getWhenValue(session, left);
        }
        return getValue(session, left).getBoolean();
    }

    private Value getValue(Session session, Value left) {
        query.setSession(session);
        // We need a LocalResult
        query.setNeverLazy(true);
        query.setDistinctIfPossible();
        LocalResult rows = (LocalResult) query.query(0);
        if (!rows.hasNext()) {
            return ValueBoolean.get(not ^ all);
        } else if (left.containsNull()) {
            return ValueNull.INSTANCE;
        }
        if (!session.getDatabase().getSettings().optimizeInSelect) {
            return getValueSlow(session, rows, left);
        }
        if (all || compareType != Comparison.EQUAL) {
            return getValueSlow(session, rows, left);
        }
        int columnCount = query.getColumnCount();
        if (columnCount != 1) {
            left = left.convertTo(TypeInfo.TYPE_ROW);
            Value[] leftValue = ((ValueRow) left).getList();
            if (columnCount == leftValue.length && rows.containsDistinct(leftValue)) {
                return ValueBoolean.get(!not);
            }
        } else {
            TypeInfo colType = rows.getColumnType(0);
            if (colType.getValueType() == Value.NULL) {
                return ValueNull.INSTANCE;
            }
            if (left.getValueType() == Value.ROW) {
                Value[] leftList = ((ValueRow) left).getList();
                if (leftList.length != 1) {
                    throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
                }
                left = leftList[0];
            }
            left = left.convertTo(colType, session);
            if (rows.containsDistinct(new Value[] { left })) {
                return ValueBoolean.get(!not);
            }
        }
        if (rows.containsNull()) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(not);
    }

    private Value getValueSlow(Session session, ResultInterface rows, Value l) {
        // this only returns the correct result if the result has at least one
        // row, and if l is not null
        boolean hasNull = false;
        if (all) {
            while (rows.next()) {
                Value cmp = compare(session, l, rows);
                if (cmp == ValueNull.INSTANCE) {
                    hasNull = true;
                } else if (cmp == ValueBoolean.FALSE) {
                    return ValueBoolean.get(not);
                }
            }
        } else {
            while (rows.next()) {
                Value cmp = compare(session, l, rows);
                if (cmp == ValueNull.INSTANCE) {
                    hasNull = true;
                } else if (cmp == ValueBoolean.TRUE) {
                    return ValueBoolean.get(!not);
                }
            }
        }
        if (hasNull) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(not ^ all);
    }

    private Value compare(Session session, Value l, ResultInterface rows) {
        Value[] currentRow = rows.currentRow();
        Value r = l.getValueType() != Value.ROW && query.getColumnCount() == 1 ? currentRow[0]
                : ValueRow.get(currentRow);
        return Comparison.compare(session, l, r, compareType);
    }

    @Override
    public Expression getNotIfPossible(Session session) {
        if (whenOperand) {
            return null;
        }
        return new ConditionInQuery(left, !not, false, query, all, compareType);
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
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        boolean outerNot = not && (all || compareType != Comparison.EQUAL);
        if (outerNot) {
            builder.append("(NOT ");
        }
        builder.append('(');
        left.getSQL(builder, sqlFlags);
        getWhenSQL(builder, sqlFlags);
        if (outerNot) {
            builder.append(')');
        }
        return builder;
    }

    @Override
    public StringBuilder getWhenSQL(StringBuilder builder, int sqlFlags) {
        if (all) {
            builder.append(Comparison.getCompareOperator(compareType)).append(" ALL");
        } else if (compareType == Comparison.EQUAL) {
            if (not) {
                builder.append(" NOT");
            }
            builder.append(" IN");
        } else {
            builder.append(' ').append(Comparison.getCompareOperator(compareType)).append(" ANY");
        }
        return super.getSQL(builder, sqlFlags).append(')');
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
        if (not || compareType != Comparison.EQUAL) {
            return;
        }
        if (query.getColumnCount() != 1) {
            return;
        }
        int leftType = left.getType().getValueType();
        if (!DataType.hasTotalOrdering(leftType)
                && leftType != query.getExpressions().get(0).getType().getValueType()) {
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
