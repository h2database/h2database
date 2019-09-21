/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Objects;

import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.Session;
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
import org.h2.table.DataChangeDeltaTable.ResultOption;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This class represents the statement
 * UPDATE
 */
public class Update extends Prepared implements DataChangeStatement {

    private Expression condition;
    private TableFilter targetTableFilter;// target of update
    /**
     * This table filter is for MERGE..USING support - not used in stand-alone DML
     */
    private TableFilter sourceTableFilter;

    /** The limit expression as specified in the LIMIT clause. */
    private Expression limitExpr;

    private boolean updateToCurrentValuesReturnsZero;

    private final LinkedHashMap<Column, Expression> setClauseMap  = new LinkedHashMap<>();

    private HashSet<Long> updatedKeysCollector;

    private ResultTarget deltaChangeCollector;

    private ResultOption deltaChangeCollectionMode;

    public Update(Session session) {
        super(session);
    }

    @Override
    public Table getTable() {
        return targetTableFilter.getTable();
    }

    public void setTableFilter(TableFilter tableFilter) {
        this.targetTableFilter = tableFilter;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    public Expression getCondition( ) {
        return this.condition;
    }

    /**
     * Add an assignment of the form column = expression.
     *
     * @param column the column
     * @param expression the expression
     */
    public void setAssignment(Column column, Expression expression) {
        if (setClauseMap.put(column, expression) != null) {
            throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, column.getName());
        }
        if (expression instanceof Parameter) {
            Parameter p = (Parameter) expression;
            p.setColumn(column);
        }
    }

    /**
     * Sets the collector of updated keys.
     *
     * @param updatedKeysCollector the collector of updated keys
     */
    public void setUpdatedKeysCollector(HashSet<Long> updatedKeysCollector) {
        this.updatedKeysCollector = updatedKeysCollector;
    }

    @Override
    public void setDeltaChangeCollector(ResultTarget deltaChangeCollector, ResultOption deltaChangeCollectionMode) {
        this.deltaChangeCollector = deltaChangeCollector;
        this.deltaChangeCollectionMode = deltaChangeCollectionMode;
    }

    @Override
    public int update() {
        targetTableFilter.startQuery(session);
        targetTableFilter.reset();
        try (RowList rows = new RowList(session)) {
            Table table = targetTableFilter.getTable();
            session.getUser().checkRight(table, Right.UPDATE);
            table.fire(session, Trigger.UPDATE, true);
            table.lock(session, true, false);
            // get the old rows, compute the new rows
            setCurrentRowNumber(0);
            int count = 0;
            Column[] columns = table.getColumns();
            int columnCount = columns.length;
            int limitRows = -1;
            if (limitExpr != null) {
                Value v = limitExpr.getValue(session);
                if (v != ValueNull.INSTANCE) {
                    limitRows = v.getInt();
                }
            }
            while (targetTableFilter.next()) {
                setCurrentRowNumber(count+1);
                if (limitRows >= 0 && count >= limitRows) {
                    break;
                }
                if (condition == null || condition.getBooleanValue(session)) {
                    Row oldRow = targetTableFilter.get();
                    if (table.isMVStore()) {
                        Row lockedRow = table.lockRow(session, oldRow);
                        if (lockedRow == null) {
                            continue;
                        }
                        if (!oldRow.hasSharedData(lockedRow)) {
                            oldRow = lockedRow;
                            targetTableFilter.set(oldRow);
                            if (condition != null && !condition.getBooleanValue(session)) {
                                continue;
                            }
                        }
                    }
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
                            newValue = oldRow.getValue(i);
                        } else if (newExpr == ValueExpression.getDefault()) {
                            newValue = table.getDefaultValue(session, column);
                        } else {
                            newValue = newExpr.getValue(session);
                        }
                        newRow.setValue(i, newValue);
                    }
                    long key = oldRow.getKey();
                    newRow.setKey(key);
                    table.validateConvertUpdateSequence(session, newRow);
                    if (setOnUpdate || updateToCurrentValuesReturnsZero) {
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
                                    if (column.getOnUpdateExpression() != null) {
                                        newRow.setValue(i, table.getOnUpdateValue(session, column));
                                    }
                                }
                            }
                        } else if (updateToCurrentValuesReturnsZero) {
                            count--;
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
                        if (updatedKeysCollector != null) {
                            updatedKeysCollector.add(key);
                        }
                        if (deltaChangeCollectionMode == ResultOption.FINAL) {
                            deltaChangeCollector.addRow(newRow.getValueList());
                        }
                    }
                    count++;
                }
            }
            // TODO self referencing referential integrity constraints
            // don't work if update is multi-row and 'inversed' the condition!
            // probably need multi-row triggers with 'deleted' and 'inserted'
            // at the same time. anyway good for sql compatibility
            // TODO update in-place (but if the key changes,
            // we need to update all indexes) before row triggers

            // the cached row is already updated - we need the old values
            table.updateRows(this, session, rows);
            if (table.fireRow()) {
                for (rows.reset(); rows.hasNext();) {
                    Row o = rows.next();
                    Row n = rows.next();
                    table.fireAfterRow(session, o, n, false);
                }
            }
            table.fire(session, Trigger.UPDATE, false);
            return count;
        }
    }

    @Override
    public String getPlanSQL(boolean alwaysQuote) {
        StringBuilder builder = new StringBuilder("UPDATE ");
        targetTableFilter.getPlanSQL(builder, false, alwaysQuote).append("\nSET\n    ");
        boolean f = false;
        for (Entry<Column, Expression> entry : setClauseMap.entrySet()) {
            if (f) {
                builder.append(",\n    ");
            }
            f = true;
            entry.getKey().getSQL(builder, alwaysQuote).append(" = ");
            entry.getValue().getSQL(builder, alwaysQuote);
        }
        if (condition != null) {
            builder.append("\nWHERE ");
            condition.getUnenclosedSQL(builder, alwaysQuote);
        }
        if (limitExpr != null) {
            builder.append("\nLIMIT ");
            limitExpr.getUnenclosedSQL(builder, alwaysQuote);
        }
        return builder.toString();
    }

    @Override
    public void prepare() {
        if (condition != null) {
            condition.mapColumns(targetTableFilter, 0, Expression.MAP_INITIAL);
            condition = condition.optimize(session);
            condition.createIndexConditions(session, targetTableFilter);
        }
        for (Entry<Column, Expression> entry : setClauseMap.entrySet()) {
            Expression e = entry.getValue();
            e.mapColumns(targetTableFilter, 0, Expression.MAP_INITIAL);
            if (sourceTableFilter!=null){
                e.mapColumns(sourceTableFilter, 0, Expression.MAP_INITIAL);
            }
            entry.setValue(e.optimize(session));
        }
        TableFilter[] filters;
        if(sourceTableFilter==null){
            filters = new TableFilter[] { targetTableFilter };
        }
        else{
            filters = new TableFilter[] { targetTableFilter, sourceTableFilter };
        }
        PlanItem item = targetTableFilter.getBestPlanItem(session, filters, 0,
                new AllColumnsForPlan(filters));
        targetTableFilter.setPlanItem(item);
        targetTableFilter.prepare();
    }

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
        return CommandInterface.UPDATE;
    }

    @Override
    public String getStatementName() {
        return "UPDATE";
    }

    public void setLimit(Expression limit) {
        this.limitExpr = limit;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    public TableFilter getSourceTableFilter() {
        return sourceTableFilter;
    }

    public void setSourceTableFilter(TableFilter sourceTableFilter) {
        this.sourceTableFilter = sourceTableFilter;
    }

    /**
     * Sets expected update count for update to current values case.
     *
     * @param updateToCurrentValuesReturnsZero if zero should be returned as update
     *        count if update set row to current values
     */
    public void setUpdateToCurrentValuesReturnsZero(boolean updateToCurrentValuesReturnsZero) {
        this.updateToCurrentValuesReturnsZero = updateToCurrentValuesReturnsZero;
    }

    @Override
    public void collectDependencies(HashSet<DbObject> dependencies) {
        ExpressionVisitor visitor = ExpressionVisitor.getDependenciesVisitor(dependencies);
        if (condition != null) {
            condition.isEverything(visitor);
        }
        if (sourceTableFilter != null) {
            Select select = sourceTableFilter.getSelect();
            if (select != null) {
                select.isEverything(visitor);
            }
        }
    }
}
