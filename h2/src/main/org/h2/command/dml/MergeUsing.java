/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.command.CommandInterface;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.ResultTarget;
import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.table.DataChangeDeltaTable;
import org.h2.table.DataChangeDeltaTable.ResultOption;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.HasSQL;
import org.h2.util.Utils;

/**
 * This class represents the statement syntax
 * MERGE INTO table alias USING...
 *
 * It does not replace the MERGE INTO... KEYS... form.
 */
public final class MergeUsing extends DataChangeStatement {

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

    /**
     * Contains _ROWID_ of processed rows. Row
     * identities are remembered to prevent duplicate updates of the same row.
     */
    private final HashSet<Long> targetRowidsRemembered = new HashSet<>();

    public MergeUsing(SessionLocal session, TableFilter targetTableFilter) {
        super(session);
        this.targetTableFilter = targetTableFilter;
    }

    @Override
    public long update(ResultTarget deltaChangeCollector, ResultOption deltaChangeCollectionMode) {
        long countUpdatedRows = 0;
        targetRowidsRemembered.clear();
        checkRights();
        setCurrentRowNumber(0);
        sourceTableFilter.startQuery(session);
        sourceTableFilter.reset();
        Table table = targetTableFilter.getTable();
        table.fire(session, evaluateTriggerMasks(), true);
        table.lock(session, Table.WRITE_LOCK);
        setCurrentRowNumber(0);
        long count = 0;
        Row previousSource = null, missedSource = null;
        boolean hasRowId = table.getRowIdColumn() != null;
        while (sourceTableFilter.next()) {
            Row source = sourceTableFilter.get();
            if (missedSource != null) {
                if (source != missedSource) {
                    Row backupTarget = targetTableFilter.get();
                    sourceTableFilter.set(missedSource);
                    targetTableFilter.set(table.getNullRow());
                    countUpdatedRows += merge(true, deltaChangeCollector, deltaChangeCollectionMode);
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
                if (table.isRowLockable()) {
                    Row lockedRow = table.lockRow(session, targetRow, -1);
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
            countUpdatedRows += merge(nullRow, deltaChangeCollector, deltaChangeCollectionMode);
            count++;
            previousSource = source;
        }
        if (missedSource != null) {
            sourceTableFilter.set(missedSource);
            targetTableFilter.set(table.getNullRow());
            countUpdatedRows += merge(true, deltaChangeCollector, deltaChangeCollectionMode);
        }
        targetRowidsRemembered.clear();
        table.fire(session, evaluateTriggerMasks(), false);
        return countUpdatedRows;
    }

    private int merge(boolean nullRow, ResultTarget deltaChangeCollector, ResultOption deltaChangeCollectionMode) {
        for (When w : when) {
            if (w.getClass() == WhenNotMatched.class == nullRow) {
                Expression condition = w.andCondition;
                if (condition == null || condition.getBooleanValue(session)) {
                    w.merge(session, deltaChangeCollector, deltaChangeCollectionMode);
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
        session.getUser().checkTableRight(targetTableFilter.getTable(), Right.SELECT);
        session.getUser().checkTableRight(sourceTableFilter.getTable(), Right.SELECT);
    }

    @Override
    public String getPlanSQL(int sqlFlags) {
        StringBuilder builder = new StringBuilder("MERGE INTO ");
        targetTableFilter.getPlanSQL(builder, false, sqlFlags);
        sourceTableFilter.getPlanSQL(builder.append('\n').append("USING "), false, sqlFlags);
        onCondition.getSQL(builder.append('\n').append("ON "), sqlFlags);
        for (When w : when) {
            w.getSQL(builder.append('\n'), sqlFlags);
        }
        return builder.toString();
    }

    @Override
    void doPrepare() {
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
    public abstract class When implements HasSQL {

        /**
         * AND condition of the command.
         */
        Expression andCondition;

        When() {
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
         * @param deltaChangeCollector
         *            target result
         * @param deltaChangeCollectionMode
         *            collection mode
         */
        abstract void merge(SessionLocal session, ResultTarget deltaChangeCollector,
                ResultOption deltaChangeCollectionMode);

        /**
         * Prepares WHEN command.
         *
         * @param session
         *            the session
         * @return {@code false} if this clause may be removed
         */
        boolean prepare(SessionLocal session) {
            if (andCondition != null) {
                andCondition.mapColumns(targetTableFilter, 0, Expression.MAP_INITIAL);
                andCondition.mapColumns(sourceTableFilter, 0, Expression.MAP_INITIAL);
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
                andCondition.getUnenclosedSQL(builder.append(" AND "), sqlFlags);
            }
            return builder.append(" THEN ");
        }

    }

    public final class WhenMatchedThenDelete extends When {

        @Override
        void merge(SessionLocal session, ResultTarget deltaChangeCollector, ResultOption deltaChangeCollectionMode) {
            TableFilter targetTableFilter = MergeUsing.this.targetTableFilter;
            Table table = targetTableFilter.getTable();
            Row row = targetTableFilter.get();
            if (deltaChangeCollectionMode == ResultOption.OLD) {
                deltaChangeCollector.addRow(row.getValueList());
            }
            if (!table.fireRow() || !table.fireBeforeRow(session, row, null)) {
                table.removeRow(session, row);
                table.fireAfterRow(session, row, null, false);
            }
        }

        @Override
        int evaluateTriggerMasks() {
            return Trigger.DELETE;
        }

        @Override
        void checkRights() {
            getSession().getUser().checkTableRight(targetTableFilter.getTable(), Right.DELETE);
        }

        @Override
        public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
            return super.getSQL(builder, sqlFlags).append("DELETE");
        }

    }

    public final class WhenMatchedThenUpdate extends When {

        private SetClauseList setClauseList;

        public void setSetClauseList(SetClauseList setClauseList) {
            this.setClauseList = setClauseList;
        }

        @Override
        void merge(SessionLocal session, ResultTarget deltaChangeCollector, ResultOption deltaChangeCollectionMode) {
            TableFilter targetTableFilter = MergeUsing.this.targetTableFilter;
            Table table = targetTableFilter.getTable();
            try (LocalResult rows = LocalResult.forTable(session, table)) {
                setClauseList.prepareUpdate(table, session, deltaChangeCollector, deltaChangeCollectionMode, rows,
                        targetTableFilter.get(), false);
                Update.doUpdate(MergeUsing.this, session, table, rows);
            }
        }

        @Override
        boolean prepare(SessionLocal session) {
            boolean result = super.prepare(session);
            setClauseList.mapAndOptimize(session, targetTableFilter, sourceTableFilter);
            return result;
        }

        @Override
        int evaluateTriggerMasks() {
            return Trigger.UPDATE;
        }

        @Override
        void checkRights() {
            getSession().getUser().checkTableRight(targetTableFilter.getTable(), Right.UPDATE);
        }

        @Override
        void collectDependencies(ExpressionVisitor visitor) {
            super.collectDependencies(visitor);
            setClauseList.isEverything(visitor);
        }

        @Override
        public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
            return setClauseList.getSQL(super.getSQL(builder, sqlFlags).append("UPDATE"), sqlFlags);
        }

    }

    public final class WhenNotMatched extends When {

        private Column[] columns;

        private final Boolean overridingSystem;

        private final Expression[] values;

        public WhenNotMatched(Column[] columns, Boolean overridingSystem, Expression[] values) {
            this.columns = columns;
            this.overridingSystem = overridingSystem;
            this.values = values;
        }

        @Override
        void merge(SessionLocal session, ResultTarget deltaChangeCollector, ResultOption deltaChangeCollectionMode) {
            Table table = targetTableFilter.getTable();
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
            table.convertInsertRow(session, newRow, overridingSystem);
            if (deltaChangeCollectionMode == ResultOption.NEW) {
                deltaChangeCollector.addRow(newRow.getValueList().clone());
            }
            if (!table.fireBeforeRow(session, null, newRow)) {
                table.addRow(session, newRow);
                DataChangeDeltaTable.collectInsertedFinalRow(session, table, deltaChangeCollector,
                        deltaChangeCollectionMode, newRow);
                table.fireAfterRow(session, null, newRow, false);
            } else {
                DataChangeDeltaTable.collectInsertedFinalRow(session, table, deltaChangeCollector,
                        deltaChangeCollectionMode, newRow);
            }
        }

        @Override
        boolean prepare(SessionLocal session) {
            boolean result = super.prepare(session);
            TableFilter targetTableFilter = MergeUsing.this.targetTableFilter,
                    sourceTableFilter = MergeUsing.this.sourceTableFilter;
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
            getSession().getUser().checkTableRight(targetTableFilter.getTable(), Right.INSERT);
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
            Column.writeColumns(builder, columns, sqlFlags).append(")\nVALUES (");
            return Expression.writeExpressions(builder, values, sqlFlags).append(')');
        }

    }

}
