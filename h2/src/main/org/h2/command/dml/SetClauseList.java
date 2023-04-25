/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;
import java.util.Arrays;

import org.h2.api.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionList;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.ResultTarget;
import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.DataChangeDeltaTable.ResultOption;
import org.h2.table.Table;
import org.h2.util.HasSQL;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;

/**
 * Set clause list.
 */
public final class SetClauseList implements HasSQL {

    private final Table table;

    private final UpdateAction[] actions;

    private boolean onUpdate;

    public SetClauseList(Table table) {
        this.table = table;
        actions = new UpdateAction[table.getColumns().length];
    }

    /**
     * Add a single column.
     *
     * @param column the column
     * @param arrayIndexes
     *            non-empty array of indexes for array element assignment, or
     *            {@code null} for simple assignment
     * @param expression the expression
     */
    public void addSingle(Column column, Expression[] arrayIndexes, Expression expression) {
        int id = column.getColumnId();
        if (actions[id] != null) {
            throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, column.getName());
        }
        if (expression != ValueExpression.DEFAULT) {
            actions[id] = new SetSimple(arrayIndexes, expression);
            if (expression instanceof Parameter) {
                ((Parameter) expression).setColumn(column);
            }
        } else {
            actions[id] = SetClauseList.UpdateAction.SET_DEFAULT;
        }
    }

    /**
     * Add multiple columns.
     *
     * @param columns the columns
     * @param allIndexes
     *            list of non-empty arrays of indexes for array element
     *            assignments, or {@code null} values for simple assignments
     * @param expression the expression (e.g. an expression list)
     */
    public void addMultiple(ArrayList<Column> columns, ArrayList<Expression[]> allIndexes, Expression expression) {
        int columnCount = columns.size();
        if (expression instanceof ExpressionList) {
            ExpressionList expressions = (ExpressionList) expression;
            if (!expressions.isArray()) {
                if (columnCount != expressions.getSubexpressionCount()) {
                    throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
                }
                for (int i = 0; i < columnCount; i++) {
                    addSingle(columns.get(i), allIndexes.get(i), expressions.getSubexpression(i));
                }
                return;
            }
        }
        if (columnCount == 1) {
            // Row value special case
            addSingle(columns.get(0), allIndexes.get(0), expression);
        } else {
            int[] cols = new int[columnCount];
            RowExpression row = new RowExpression(expression, cols);
            int minId = table.getColumns().length - 1, maxId = 0;
            for (int i = 0; i < columnCount; i++) {
                int id = columns.get(i).getColumnId();
                if (id < minId) {
                    minId = id;
                }
                if (id > maxId) {
                    maxId = id;
                }
            }
            for (int i = 0; i < columnCount; i++) {
                Column column = columns.get(i);
                int id = column.getColumnId();
                cols[i] = id;
                if (actions[id] != null) {
                    throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, column.getName());
                }
                actions[id] = new SetMultiple(allIndexes.get(i), row, i, id == minId, id == maxId);
            }
        }
    }

    boolean prepareUpdate(Table table, SessionLocal session, ResultTarget deltaChangeCollector,
            ResultOption deltaChangeCollectionMode, LocalResult rows, Row oldRow,
            boolean updateToCurrentValuesReturnsZero) {
        Column[] columns = table.getColumns();
        int columnCount = columns.length;
        Row newRow = table.getTemplateRow();
        for (int i = 0; i < columnCount; i++) {
            UpdateAction action = actions[i];
            Column column = columns[i];
            Value oldValue = oldRow.getValue(i), newValue;
            if (action == null || action == UpdateAction.ON_UPDATE) {
                newValue = column.isGenerated() ? null : oldValue;
            } else if (action == UpdateAction.SET_DEFAULT) {
                newValue = !column.isIdentity() ? null : oldValue;
            } else {
                newValue = action.update(session, oldValue);
                if (newValue == ValueNull.INSTANCE && column.isDefaultOnNull()) {
                    newValue = !column.isIdentity() ? null : oldValue;
                } else if (column.isGeneratedAlways()) {
                    throw DbException.get(ErrorCode.GENERATED_COLUMN_CANNOT_BE_ASSIGNED_1,
                            column.getSQLWithTable(new StringBuilder(), TRACE_SQL_FLAGS).toString());
                }
            }
            newRow.setValue(i, newValue);
        }
        newRow.setKey(oldRow.getKey());
        table.convertUpdateRow(session, newRow, false);
        boolean result = true;
        if (onUpdate) {
            if (!oldRow.hasSameValues(newRow)) {
                for (int i = 0; i < columnCount; i++) {
                    if (actions[i] == UpdateAction.ON_UPDATE) {
                        newRow.setValue(i, columns[i].getEffectiveOnUpdateExpression().getValue(session));
                    } else if (columns[i].isGenerated()) {
                        newRow.setValue(i, null);
                    }
                }
                // Convert on update expressions and reevaluate
                // generated columns
                table.convertUpdateRow(session, newRow, false);
            } else if (updateToCurrentValuesReturnsZero) {
                result = false;
            }
        } else if (updateToCurrentValuesReturnsZero && oldRow.hasSameValues(newRow)) {
            result = false;
        }
        if (deltaChangeCollectionMode == ResultOption.OLD) {
            deltaChangeCollector.addRow(oldRow.getValueList());
        } else if (deltaChangeCollectionMode == ResultOption.NEW) {
            deltaChangeCollector.addRow(newRow.getValueList().clone());
        }
        if (!table.fireRow() || !table.fireBeforeRow(session, oldRow, newRow)) {
            rows.addRowForTable(oldRow);
            rows.addRowForTable(newRow);
        }
        if (deltaChangeCollectionMode == ResultOption.FINAL) {
            deltaChangeCollector.addRow(newRow.getValueList());
        }
        return result;
    }

    /**
     * Check if this expression and all sub-expressions can fulfill a criteria.
     * If any part returns false, the result is false.
     *
     * @param visitor
     *            the visitor
     * @return if the criteria can be fulfilled
     */
    boolean isEverything(ExpressionVisitor visitor) {
        for (UpdateAction action : actions) {
            if (action != null) {
                if (!action.isEverything(visitor)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Map the columns and optimize expressions.
     *
     * @param session
     *            the session
     * @param resolver1
     *            the first column resolver
     * @param resolver2
     *            the second column resolver, or {@code null}
     */
    void mapAndOptimize(SessionLocal session, ColumnResolver resolver1, ColumnResolver resolver2) {
        Column[] columns = table.getColumns();
        boolean onUpdate = false;
        for (int i = 0; i < actions.length; i++) {
            UpdateAction action = actions[i];
            if (action != null) {
                action.mapAndOptimize(session, resolver1, resolver2);
            } else {
                Column column = columns[i];
                if (column.getEffectiveOnUpdateExpression() != null) {
                    actions[i] = UpdateAction.ON_UPDATE;
                    onUpdate = true;
                }
            }
        }
        this.onUpdate = onUpdate;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        Column[] columns = table.getColumns();
        builder.append("\nSET\n    ");
        boolean f = false;
        for (int i = 0; i < actions.length; i++) {
            UpdateAction action = actions[i];
            if (action != null && action != UpdateAction.ON_UPDATE) {
                if (action.getClass() == SetMultiple.class) {
                    SetMultiple multiple = (SetMultiple) action;
                    if (multiple.first) {
                        if (f) {
                            builder.append(",\n    ");
                        }
                        f = true;
                        RowExpression r = multiple.row;
                        builder.append('(');
                        int[] cols = r.columns;
                        for (int j = 0, l = cols.length; j < l; j++) {
                            if (j > 0) {
                                builder.append(", ");
                            }
                            columns[cols[j]].getSQL(builder, sqlFlags);
                        }
                        r.expression.getUnenclosedSQL(builder.append(") = "), sqlFlags);
                    }
                } else {
                    if (f) {
                        builder.append(",\n    ");
                    }
                    f = true;
                    Column column = columns[i];
                    if (action != UpdateAction.SET_DEFAULT) {
                        action.getSQL(builder, sqlFlags, column);
                    } else {
                        column.getSQL(builder, sqlFlags).append(" = DEFAULT");
                    }
                }
            }
        }
        return builder;
    }

    private static class UpdateAction {

        static UpdateAction ON_UPDATE = new UpdateAction();

        static UpdateAction SET_DEFAULT = new UpdateAction();

        UpdateAction() {
        }

        Value update(SessionLocal session, Value oldValue) {
            throw DbException.getInternalError();
        }

        boolean isEverything(ExpressionVisitor visitor) {
            return true;
        }

        void mapAndOptimize(SessionLocal session, ColumnResolver resolver1, ColumnResolver resolver2) {
            // Do nothing
        }

        void getSQL(StringBuilder builder, int sqlFlags, Column column) {
            throw DbException.getInternalError();
        }

    }

    private static abstract class SetAction extends UpdateAction {

        private final Expression[] arrayIndexes;

        SetAction(Expression[] arrayIndexes) {
            this.arrayIndexes = arrayIndexes;
        }

        @Override
        boolean isEverything(ExpressionVisitor visitor) {
            if (arrayIndexes != null) {
                for (Expression e : arrayIndexes) {
                    if (!e.isEverything(visitor)) {
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        void mapAndOptimize(SessionLocal session, ColumnResolver resolver1, ColumnResolver resolver2) {
            if (arrayIndexes != null) {
                for (int i = 0, l = arrayIndexes.length; i < l; i++) {
                    Expression e = arrayIndexes[i];
                    e.mapColumns(resolver1, 0, Expression.MAP_INITIAL);
                    if (resolver2 != null) {
                        e.mapColumns(resolver2, 0, Expression.MAP_INITIAL);
                    }
                    arrayIndexes[i] = e.optimize(session);
                }
            }
        }

        @Override
        final Value update(SessionLocal session, Value oldValue) {
            Value newValue = update(session);
            if (arrayIndexes != null) {
                newValue = updateArray(session, oldValue, newValue, 0);
            }
            return newValue;
        }

        private Value updateArray(SessionLocal session, Value oldValue, Value newValue, int indexNumber) {
            int index = arrayIndexes[indexNumber++].getValue(session).getInt();
            int cardinality = Constants.MAX_ARRAY_CARDINALITY;
            if (index < 0 || index > cardinality) {
                throw DbException.get(ErrorCode.ARRAY_ELEMENT_ERROR_2, Integer.toString(index),
                        "1.." + cardinality);
            }
            Value[] values;
            if (oldValue == null) {
                values = new Value[index];
                for (int i = 0; i < index - 1; i++) {
                    values[i] = ValueNull.INSTANCE;
                }
            } else if (oldValue == ValueNull.INSTANCE) {
                throw DbException.get(ErrorCode.NULL_VALUE_IN_ARRAY_TARGET);
            } else if (oldValue.getValueType() != Value.ARRAY) {
                throw DbException.get(ErrorCode.ARRAY_ELEMENT_ERROR_2, oldValue.getType().getTraceSQL(),
                        "ARRAY");
            } else {
                values = ((ValueArray) oldValue).getList();
                int length = values.length;
                if (index <= length) {
                    values = values.clone();
                } else {
                    values = Arrays.copyOf(values, index);
                    for (int i = length; i < index - 1; i++) {
                        values[i] = ValueNull.INSTANCE;
                    }
                }
            }
            values[index - 1] = indexNumber == arrayIndexes.length ? newValue
                    : updateArray(session, values[index - 1], newValue, indexNumber);
            return ValueArray.get(values, session);
        }

        abstract Value update(SessionLocal session);

    }

    private static final class SetSimple extends SetAction {

        private Expression expression;

        SetSimple(Expression[] arrayIndexes, Expression expression) {
            super(arrayIndexes);
            this.expression = expression;
        }

        @Override
        Value update(SessionLocal session) {
            return expression.getValue(session);
        }

        @Override
        boolean isEverything(ExpressionVisitor visitor) {
            return super.isEverything(visitor) && expression.isEverything(visitor);
        }

        @Override
        void mapAndOptimize(SessionLocal session, ColumnResolver resolver1, ColumnResolver resolver2) {
            super.mapAndOptimize(session, resolver1, resolver2);
            expression.mapColumns(resolver1, 0, Expression.MAP_INITIAL);
            if (resolver2 != null) {
                expression.mapColumns(resolver2, 0, Expression.MAP_INITIAL);
            }
            expression = expression.optimize(session);
        }

        @Override
        void getSQL(StringBuilder builder, int sqlFlags, Column column) {
            expression.getUnenclosedSQL(column.getSQL(builder, sqlFlags).append(" = "), sqlFlags);
        }

    }

    private static final class RowExpression {

        Expression expression;

        final int[] columns;

        Value[] values;

        RowExpression(Expression expression, int[] columns) {
            this.expression = expression;
            this.columns = columns;
        }

        boolean isEverything(ExpressionVisitor visitor) {
            return expression.isEverything(visitor);
        }

        void mapAndOptimize(SessionLocal session, ColumnResolver resolver1, ColumnResolver resolver2) {
            expression.mapColumns(resolver1, 0, Expression.MAP_INITIAL);
            if (resolver2 != null) {
                expression.mapColumns(resolver2, 0, Expression.MAP_INITIAL);
            }
            expression = expression.optimize(session);
        }
    }

    private static final class SetMultiple extends SetAction {

        final RowExpression row;

        private final int position;

        boolean first;

        private boolean last;

        SetMultiple(Expression[] arrayIndexes, RowExpression row, int position, boolean first, boolean last) {
            super(arrayIndexes);
            this.row = row;
            this.position = position;
            this.first = first;
            this.last = last;
        }

        @Override
        Value update(SessionLocal session) {
            Value[] v;
            if (first) {
                Value value = row.expression.getValue(session);
                if (value == ValueNull.INSTANCE) {
                    throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, "NULL to assigned row value");
                }
                row.values = v = value.convertToAnyRow().getList();
                if (v.length != row.columns.length) {
                    throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
                }
            } else {
                v = row.values;
                if (last) {
                    row.values = null;
                }
            }
            return v[position];
        }

        @Override
        boolean isEverything(ExpressionVisitor visitor) {
            return super.isEverything(visitor) && (!first || row.isEverything(visitor));
        }

        @Override
        void mapAndOptimize(SessionLocal session, ColumnResolver resolver1, ColumnResolver resolver2) {
            super.mapAndOptimize(session, resolver1, resolver2);
            if (first) {
                row.mapAndOptimize(session, resolver1, resolver2);
            }
        }

    }

}
