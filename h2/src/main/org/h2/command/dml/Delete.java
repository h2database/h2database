/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.HashSet;

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
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.table.DataChangeDeltaTable.ResultOption;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This class represents the statement
 * DELETE
 */
public class Delete extends Prepared implements DataChangeStatement {

    private Expression condition;
    private TableFilter targetTableFilter;

    /**
     * The limit expression as specified in the LIMIT or TOP clause.
     */
    private Expression limitExpr;

    private ResultTarget deltaChangeCollector;

    private ResultOption deltaChangeCollectionMode;

    public Delete(Session session) {
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

    public Expression getCondition() {
        return this.condition;
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
        Table table = targetTableFilter.getTable();
        session.getUser().checkRight(table, Right.DELETE);
        table.fire(session, Trigger.DELETE, true);
        table.lock(session, true, false);
        int limitRows = -1;
        if (limitExpr != null) {
            Value v = limitExpr.getValue(session);
            if (v != ValueNull.INSTANCE) {
                limitRows = v.getInt();
            }
        }
        try (RowList rows = new RowList(session, table)) {
            setCurrentRowNumber(0);
            int count = 0;
            while (limitRows != 0 && targetTableFilter.next()) {
                setCurrentRowNumber(rows.size() + 1);
                if (condition == null || condition.getBooleanValue(session)) {
                    Row row = targetTableFilter.get();
                    if (table.isMVStore()) {
                        Row lockedRow = table.lockRow(session, row);
                        if (lockedRow == null) {
                            continue;
                        }
                        if (!row.hasSharedData(lockedRow)) {
                            row = lockedRow;
                            targetTableFilter.set(row);
                            if (condition != null && !condition.getBooleanValue(session)) {
                                continue;
                            }
                        }
                    }
                    if (deltaChangeCollectionMode == ResultOption.OLD) {
                        deltaChangeCollector.addRow(row.getValueList());
                    }
                    if (!table.fireRow() || !table.fireBeforeRow(session, row, null)) {
                        rows.add(row);
                    }
                    count++;
                    if (limitRows >= 0 && count >= limitRows) {
                        break;
                    }
                }
            }
            int rowScanCount = 0;
            for (rows.reset(); rows.hasNext();) {
                if ((++rowScanCount & 127) == 0) {
                    checkCanceled();
                }
                Row row = rows.next();
                table.removeRow(session, row);
                session.log(table, UndoLogRecord.DELETE, row);
            }
            if (table.fireRow()) {
                for (rows.reset(); rows.hasNext();) {
                    Row row = rows.next();
                    table.fireAfterRow(session, row, null, false);
                }
            }
            table.fire(session, Trigger.DELETE, false);
            return count;
        }
    }

    @Override
    public String getPlanSQL(int sqlFlags) {
        StringBuilder buff = new StringBuilder();
        buff.append("DELETE FROM ");
        targetTableFilter.getPlanSQL(buff, false, sqlFlags);
        if (condition != null) {
            buff.append("\nWHERE ");
            condition.getUnenclosedSQL(buff, sqlFlags);
        }
        if (limitExpr != null) {
            buff.append("\nLIMIT (");
            limitExpr.getUnenclosedSQL(buff, sqlFlags).append(')');
        }
        return buff.toString();
    }

    @Override
    public void prepare() {
        if (condition != null) {
            condition.mapColumns(targetTableFilter, 0, Expression.MAP_INITIAL);
            condition = condition.optimizeCondition(session);
            if (condition != null) {
                condition.createIndexConditions(session, targetTableFilter);
            }
        }
        TableFilter[] filters = new TableFilter[] { targetTableFilter };
        PlanItem item = targetTableFilter.getBestPlanItem(session, filters, 0, new AllColumnsForPlan(filters));
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
        return CommandInterface.DELETE;
    }

    @Override
    public String getStatementName() {
        return "DELETE";
    }

    public void setLimit(Expression limit) {
        this.limitExpr = limit;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    public TableFilter getTableFilter() {
        return targetTableFilter;
    }

    @Override
    public void collectDependencies(HashSet<DbObject> dependencies) {
        ExpressionVisitor visitor = ExpressionVisitor.getDependenciesVisitor(dependencies);
        if (condition != null) {
            condition.isEverything(visitor);
        }
    }
}
