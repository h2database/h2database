/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Map.Entry;

import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.engine.UndoLogRecord;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.table.Column;
import org.h2.table.PlanItem;
import org.h2.table.DataChangeDeltaTable.ResultOption;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.HasSQL;
import org.h2.util.Utils;
import org.h2.value.Value;

/**
 * This class represents the statement syntax
 * MERGE INTO table alias USING...
 *
 * It does not replace the MERGE INTO... KEYS... form.
 */
public class MergeUsing extends Prepared implements DataChangeStatement {

    /**
     * Target table filter.
     */
    TableFilter targetTableFilter;

    /**
     * Source table filter.
     */
    TableFilter sourceTableFilter;

    /**
     * ON condition expression.
     */
    Expression onCondition;

    private ArrayList<When> when = Utils.newSmallArrayList();

    ResultTarget deltaChangeCollector;

    ResultOption deltaChangeCollectionMode;

    /**
     * Contains _ROWID_ of processed rows. Row
     * identities are remembered to prevent duplicate updates of the same row.
     */
    private final HashSet<Long> targetRowidsRemembered = new HashSet<>();

    public MergeUsing(Session session, TableFilter targetTableFilter) {
        super(session);
        this.targetTableFilter = targetTableFilter;
    }

    @Override
    public void setDeltaChangeCollector(ResultTarget deltaChangeCollector, ResultOption deltaChangeCollectionMode) {
        this.deltaChangeCollector = deltaChangeCollector;
        this.deltaChangeCollectionMode = deltaChangeCollectionMode;
    }

    @Override
    public int update() {
        int countUpdatedRows = 0;
        targetRowidsRemembered.clear();
        checkRights();
        setCurrentRowNumber(0);
        sourceTableFilter.startQuery(session);
        sourceTableFilter.reset();
        Table table = targetTableFilter.getTable();
        table.fire(session, evaluateTriggerMasks(), true);
        table.lock(session, true, false);
        setCurrentRowNumber(0);
        int count = 0;
        Row previousSource = null, missedSource = null;
        boolean hasRowId = table.getRowIdColumn() != null;
        while (sourceTableFilter.next()) {
            Row source = sourceTableFilter.get();
            if (missedSource != null) {
                if (source != missedSource) {
                    Row backupTarget = targetTableFilter.get();
                    sourceTableFilter.set(missedSource);
                    targetTableFilter.set(table.getNullRow());
                    countUpdatedRows += merge(true);
                    sourceTableFilter.set(source);
                    targetTableFilter.set(backupTarget);
                    count++;
                }
                missedSource = null;
            }
            setCurrentRowNumber(count + 1);
            boolean nullRow = targetTableFilter.isNullRow();
            if (!nullRow) {
                Row targetRow = targetTableFilter.get();
                if (table.isMVStore()) {
                    Row lockedRow = table.lockRow(session, targetRow);
                    if (lockedRow == null) {
                        if (previousSource != source) {
                            missedSource = source;
                        }
                        continue;
                    }
                    if (!targetRow.hasSharedData(lockedRow)) {
                        targetRow = lockedRow;
                        targetTableFilter.set(targetRow);
                        if (!onCondition.getBooleanValue(session)) {
                            if (previousSource != source) {
                                missedSource = source;
                            }
                            continue;
                        }
                    }
                }
                if (hasRowId) {
                    long targetRowId = targetRow.getKey();
                    if (!targetRowidsRemembered.add(targetRowId)) {
                        throw DbException.get(ErrorCode.DUPLICATE_KEY_1,
                                "Merge using ON column expression, " +
                                "duplicate _ROWID_ target record already processed:_ROWID_="
                                        + targetRowId + ":in:"
                                        + targetTableFilter.getTable());
                    }
                }
            }
            countUpdatedRows += merge(nullRow);
            count++;
            previousSource = source;
        }
        if (missedSource != null) {
            sourceTableFilter.set(missedSource);
            targetTableFilter.set(table.getNullRow());
            countUpdatedRows += merge(true);
        }
        targetRowidsRemembered.clear();
        table.fire(session, evaluateTriggerMasks(), false);
        return countUpdatedRows;
    }

    private int merge(boolean nullRow) {
        for (When w : when) {
            if (w.getClass() == WhenNotMatched.class == nullRow) {
                Expression condition = w.andCondition;
                if (condition == null || condition.getBooleanValue(session)) {
                    w.merge(session);
                    return 1;
                }
            }
        }
        return 0;
    }

    private int evaluateTriggerMasks() {
        int masks = 0;
        for (When w : when) {
            masks |= w.evaluateTriggerMasks();
        }
        return masks;
    }

    private void checkRights() {
        for (When w : when) {
            w.checkRights();
        }
        session.getUser().checkRight(targetTableFilter.getTable(), Right.SELECT);
        session.getUser().checkRight(sourceTableFilter.getTable(), Right.SELECT);
    }

    @Override
    public String getPlanSQL(int sqlFlags) {
        StringBuilder builder = new StringBuilder("MERGE INTO ");
        targetTableFilter.getPlanSQL(builder, false, sqlFlags);
        builder.append('\n').append("USING ");
        sourceTableFilter.getPlanSQL(builder, false, sqlFlags);
        for (When w : when) {
            w.getSQL(builder.append('\n'), sqlFlags);
        }
        return builder.toString();
    }

    @Override
    public void prepare() {
        onCondition.addFilterConditions(sourceTableFilter);
        onCondition.addFilterConditions(targetTableFilter);

        onCondition.mapColumns(sourceTableFilter, 0, Expression.MAP_INITIAL);
        onCondition.mapColumns(targetTableFilter, 0, Expression.MAP_INITIAL);

        onCondition = onCondition.optimize(session);
        // Create conditions only for target table
        onCondition.createIndexConditions(session, targetTableFilter);

        TableFilter[] filters = new TableFilter[] { sourceTableFilter, targetTableFilter };
        sourceTableFilter.addJoin(targetTableFilter, true, onCondition);
        PlanItem item = sourceTableFilter.getBestPlanItem(session, filters, 0, new AllColumnsForPlan(filters));
        sourceTableFilter.setPlanItem(item);
        sourceTableFilter.prepare();

        boolean hasFinalNotMatched = false, hasFinalMatched = false;
        for (Iterator<When> i = when.iterator(); i.hasNext();) {
            When w = i.next();
            if (!w.prepare(session)) {
                i.remove();
            } else if (w.getClass() == WhenNotMatched.class) {
                if (hasFinalNotMatched) {
                    i.remove();
                } else if (w.andCondition == null) {
                    hasFinalNotMatched = true;
                }
            } else {
                if (hasFinalMatched) {
                    i.remove();
                } else if (w.andCondition == null) {
                    hasFinalMatched = true;
                }
            }
        }
    }

    public void setSourceTableFilter(TableFilter sourceTableFilter) {
        this.sourceTableFilter = sourceTableFilter;
    }

    public TableFilter getSourceTableFilter() {
        return sourceTableFilter;
    }

    public void setOnCondition(Expression condition) {
        this.onCondition = condition;
    }

    public Expression getOnCondition() {
        return onCondition;
    }

    public ArrayList<When> getWhen() {
        return when;
    }

    /**
     * Adds WHEN command.
     *
     * @param w new WHEN command to add (update, delete or insert).
     */
    public void addWhen(When w) {
        when.add(w);
    }

    @Override
    public Table getTable() {
        return targetTableFilter.getTable();
    }

    public void setTargetTableFilter(TableFilter targetTableFilter) {
        this.targetTableFilter = targetTableFilter;
    }

    public TableFilter getTargetTableFilter() {
        return targetTableFilter;
    }

    // Prepared interface implementations

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    @Override
    public int getType() {
        return CommandInterface.MERGE;
    }

    @Override
    public String getStatementName() {
        return "MERGE";
    }

    @Override
    public void collectDependencies(HashSet<DbObject> dependencies) {
        dependencies.add(targetTableFilter.getTable());
        dependencies.add(sourceTableFilter.getTable());
        ExpressionVisitor visitor = ExpressionVisitor.getDependenciesVisitor(dependencies);
        for (When w : when) {
            w.collectDependencies(visitor);
        }
        onCondition.isEverything(visitor);
    }

    /**
     * Abstract WHEN command of the MERGE statement.
     */
    public abstract static class When implements HasSQL {

        /**
         * The parent MERGE statement.
         */
        final MergeUsing mergeUsing;

        /**
         * AND condition of the command.
         */
        Expression andCondition;

        When(MergeUsing mergeUsing) {
            this.mergeUsing = mergeUsing;
        }

        /**
         * Sets the specified AND condition.
         *
         * @param andCondition AND condition to set
         */
        public void setAndCondition(Expression andCondition) {
            this.andCondition = andCondition;
        }

        /**
         * Merges rows.
         *
         * @param session
         *            the session
         */
        abstract void merge(Session session);

        /**
         * Prepares WHEN command.
         *
         * @param session
         *            the session
         * @return {@code false} if this clause may be removed
         */
        boolean prepare(Session session) {
            if (andCondition != null) {
                andCondition.mapColumns(mergeUsing.targetTableFilter, 0, Expression.MAP_INITIAL);
                andCondition.mapColumns(mergeUsing.sourceTableFilter, 0, Expression.MAP_INITIAL);
                andCondition = andCondition.optimize(session);
                if (andCondition.isConstant()) {
                    if (andCondition.getBooleanValue(session)) {
                        andCondition = null;
                    } else {
                        return false;
                    }
                }
            }
            return true;
        }

        /**
         * Evaluates trigger mask (UPDATE, INSERT, DELETE).
         *
         * @return the trigger mask.
         */
        abstract int evaluateTriggerMasks();

        /**
         * Checks user's INSERT, UPDATE, DELETE permission in appropriate cases.
         */
        abstract void checkRights();

        /**
         * Find and collect all DbObjects, this When object depends on.
         *
         * @param visitor the expression visitor
         */
        void collectDependencies(ExpressionVisitor visitor) {
            if (andCondition != null) {
                andCondition.isEverything(visitor);
            }
        }

        @Override
        public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
            builder.append("WHEN ");
            if (getClass() == WhenNotMatched.class) {
                builder.append("NOT ");
            }
            builder.append("MATCHED");
            if (andCondition != null) {
                andCondition.getSQL(builder.append(" AND "), sqlFlags);
            }
            return builder.append(" THEN ");
        }

    }

    public static final class WhenMatchedThenDelete extends When {

        public WhenMatchedThenDelete(MergeUsing mergeUsing) {
            super(mergeUsing);
        }

        @Override
        void merge(Session session) {
            TableFilter targetTableFilter = mergeUsing.targetTableFilter;
            Table table = targetTableFilter.getTable();
            Row row = targetTableFilter.get();
            if (mergeUsing.deltaChangeCollectionMode == ResultOption.OLD) {
                mergeUsing.deltaChangeCollector.addRow(row.getValueList());
            }
            if (!table.fireRow() || !table.fireBeforeRow(session, row, null)) {
                table.removeRow(session, row);
                session.log(table, UndoLogRecord.DELETE, row);
                table.fireAfterRow(session, row, null, false);
            }
        }

        @Override
        int evaluateTriggerMasks() {
            return Trigger.DELETE;
        }

        @Override
        void checkRights() {
            mergeUsing.getSession().getUser().checkRight(mergeUsing.targetTableFilter.getTable(), Right.DELETE);
        }

        @Override
        public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
            return super.getSQL(builder, sqlFlags).append("DELETE");
        }

    }

    public static final class WhenMatchedThenUpdate extends When implements CommandWithAssignments {

        private final LinkedHashMap<Column, Expression> setClauseMap  = new LinkedHashMap<>();

        public WhenMatchedThenUpdate(MergeUsing mergeUsing) {
            super(mergeUsing);
        }

        @Override
        public void setAssignment(Column column, Expression expression) {
            if (setClauseMap.putIfAbsent(column, expression) != null) {
                throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, column.getName());
            }
            if (expression instanceof Parameter) {
                ((Parameter) expression).setColumn(column);
            }
        }

        @Override
        void merge(Session session) {
            TableFilter targetTableFilter = mergeUsing.targetTableFilter;
            Table table = targetTableFilter.getTable();
            ResultTarget deltaChangeCollector = mergeUsing.deltaChangeCollector;
            ResultOption deltaChangeCollectionMode = mergeUsing.deltaChangeCollectionMode;
            try (RowList rows = new RowList(session, table)) {
                Column[] columns = table.getColumns();
                int columnCount = columns.length;
                Row oldRow = targetTableFilter.get();
                Row newRow = table.getTemplateRow();
                boolean setOnUpdate = false;
                for (int i = 0; i < columnCount; i++) {
                    Column column = columns[i];
                    Expression newExpr = setClauseMap.get(column);
                    Value newValue;
                    if (newExpr == null) {
                        if (column.getOnUpdateExpression() != null) {
                            setOnUpdate = true;
                        }
                        newValue = column.getGenerated() ? null : oldRow.getValue(i);
                    } else if (newExpr == ValueExpression.DEFAULT) {
                        newValue = null;
                    } else {
                        newValue = newExpr.getValue(session);
                    }
                    newRow.setValue(i, newValue);
                }
                newRow.setKey(oldRow.getKey());
                table.validateConvertUpdateSequence(session, newRow);
                if (setOnUpdate) {
                    setOnUpdate = false;
                    for (int i = 0; i < columnCount; i++) {
                        // Use equals here to detect changes from numeric 0 to 0.0 and similar
                        if (!Objects.equals(oldRow.getValue(i), newRow.getValue(i))) {
                            setOnUpdate = true;
                            break;
                        }
                    }
                    if (setOnUpdate) {
                        for (int i = 0; i < columnCount; i++) {
                            Column column = columns[i];
                            if (setClauseMap.get(column) == null) {
                                Expression onUpdate = column.getOnUpdateExpression();
                                if (onUpdate != null) {
                                    newRow.setValue(i, onUpdate.getValue(session));
                                }
                            }
                        }
                        // Convert on update expressions and reevaluate
                        // generated columns
                        table.validateConvertUpdateSequence(session, newRow);
                    }
                }
                if (deltaChangeCollectionMode == ResultOption.OLD) {
                    deltaChangeCollector.addRow(oldRow.getValueList());
                } else if (deltaChangeCollectionMode == ResultOption.NEW) {
                    deltaChangeCollector.addRow(newRow.getValueList().clone());
                }
                if (!table.fireRow() || !table.fireBeforeRow(session, oldRow, newRow)) {
                    rows.add(oldRow);
                    rows.add(newRow);
                }
                if (deltaChangeCollectionMode == ResultOption.FINAL) {
                    deltaChangeCollector.addRow(newRow.getValueList());
                }
                table.updateRows(mergeUsing, session, rows);
                if (table.fireRow()) {
                    for (rows.reset(); rows.hasNext();) {
                        Row o = rows.next();
                        Row n = rows.next();
                        table.fireAfterRow(session, o, n, false);
                    }
                }
            }
        }

        @Override
        boolean prepare(Session session) {
            boolean result = super.prepare(session);
            TableFilter targetTableFilter = mergeUsing.targetTableFilter,
                    sourceTableFilter = mergeUsing.sourceTableFilter;
            for (Entry<Column, Expression> entry : setClauseMap.entrySet()) {
                Expression e = entry.getValue();
                e.mapColumns(targetTableFilter, 0, Expression.MAP_INITIAL);
                e.mapColumns(sourceTableFilter, 0, Expression.MAP_INITIAL);
                entry.setValue(e.optimize(session));
            }
            return result;
        }

        @Override
        int evaluateTriggerMasks() {
            return Trigger.UPDATE;
        }

        @Override
        void checkRights() {
            mergeUsing.getSession().getUser().checkRight(mergeUsing.targetTableFilter.getTable(), Right.UPDATE);
        }

        @Override
        void collectDependencies(ExpressionVisitor visitor) {
            super.collectDependencies(visitor);
            for (Entry<Column, Expression> entry : setClauseMap.entrySet()) {
                entry.getValue().isEverything(visitor);
            }
        }

        @Override
        public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
            super.getSQL(builder, sqlFlags).append("UPDATE");
            Update.getSetClauseSQL(builder, setClauseMap, sqlFlags);
            return builder;
        }

    }

    public static final class WhenNotMatched extends When {

        private Column[] columns;

        private Expression[] values;

        public WhenNotMatched(MergeUsing mergeUsing, Column[] columns, Expression[] values) {
            super(mergeUsing);
            this.columns = columns;
            this.values = values;
        }

        @Override
        void merge(Session session) {
            Table table = mergeUsing.targetTableFilter.getTable();
            ResultTarget deltaChangeCollector = mergeUsing.deltaChangeCollector;
            ResultOption deltaChangeCollectionMode = mergeUsing.deltaChangeCollectionMode;
            Row newRow = table.getTemplateRow();
            Expression[] expr = values;
            for (int i = 0, len = columns.length; i < len; i++) {
                Column c = columns[i];
                int index = c.getColumnId();
                Expression e = expr[i];
                if (e != ValueExpression.DEFAULT) {
                    try {
                        newRow.setValue(index, e.getValue(session));
                    } catch (DbException ex) {
                        ex.addSQL("INSERT -- " + getSimpleSQL(expr));
                        throw ex;
                    }
                }
            }
            table.validateConvertUpdateSequence(session, newRow);
            if (deltaChangeCollectionMode == ResultOption.NEW) {
                deltaChangeCollector.addRow(newRow.getValueList().clone());
            }
            if (!table.fireBeforeRow(session, null, newRow)) {
                table.addRow(session, newRow);
                if (deltaChangeCollectionMode == ResultOption.FINAL) {
                    deltaChangeCollector.addRow(newRow.getValueList());
                }
                session.log(table, UndoLogRecord.INSERT, newRow);
                table.fireAfterRow(session, null, newRow, false);
            } else if (deltaChangeCollectionMode == ResultOption.FINAL) {
                deltaChangeCollector.addRow(newRow.getValueList());
            }
        }

        @Override
        boolean prepare(Session session) {
            boolean result = super.prepare(session);
            TableFilter targetTableFilter = mergeUsing.targetTableFilter,
                    sourceTableFilter = mergeUsing.sourceTableFilter;
            if (columns == null) {
                columns = targetTableFilter.getTable().getColumns();
            }
            if (values.length != columns.length) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
            for (int i = 0, len = values.length; i < len; i++) {
                Expression e = values[i];
                e.mapColumns(targetTableFilter, 0, Expression.MAP_INITIAL);
                e.mapColumns(sourceTableFilter, 0, Expression.MAP_INITIAL);
                e = e.optimize(session);
                if (e instanceof Parameter) {
                    ((Parameter) e).setColumn(columns[i]);
                }
                values[i] = e;
            }
            return result;
        }

        @Override
        int evaluateTriggerMasks() {
            return Trigger.INSERT;
        }

        @Override
        void checkRights() {
            mergeUsing.getSession().getUser().checkRight(mergeUsing.targetTableFilter.getTable(), Right.INSERT);
        }

        @Override
        void collectDependencies(ExpressionVisitor visitor) {
            super.collectDependencies(visitor);
            for (Expression e : values) {
                e.isEverything(visitor);
            }
        }

        @Override
        public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
            super.getSQL(builder, sqlFlags).append("INSERT (");
            Column.writeColumns(builder, columns, sqlFlags);
            builder.append(")\nVALUES (");
            Expression.writeExpressions(builder, values, sqlFlags);
            return builder.append(')');
        }

    }

}
