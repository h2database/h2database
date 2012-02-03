/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import org.h2.api.Trigger;
import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.table.Column;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.value.Value;

/**
 * This class represents the statement
 * UPDATE
 */
public class Update extends Prepared {

    private Expression condition;
    private TableFilter tableFilter;
    private Expression[] expressions;

    public Update(Session session) {
        super(session);
    }

    public void setTableFilter(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
        Table table = tableFilter.getTable();
        expressions = new Expression[table.getColumns().length];
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    /**
     * Add an assignment of the form column = expression.
     *
     * @param column the column
     * @param expression the expression
     */
    public void setAssignment(Column column, Expression expression) {
        int id = column.getColumnId();
        if (expressions[id] != null) {
            throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, column
                    .getName());
        }
        expressions[id] = expression;
        if (expression instanceof Parameter) {
            Parameter p = (Parameter) expression;
            p.setColumn(column);
        }
    }

    public int update() {
        tableFilter.startQuery(session);
        tableFilter.reset();
        RowList rows = new RowList(session);
        try {
            Table table = tableFilter.getTable();
            session.getUser().checkRight(table, Right.UPDATE);
            table.fire(session, Trigger.UPDATE, true);
            table.lock(session, true, false);
            int columnCount = table.getColumns().length;
            // get the old rows, compute the new rows
            setCurrentRowNumber(0);
            int count = 0;
            while (tableFilter.next()) {
                setCurrentRowNumber(count+1);
                if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                    Row oldRow = tableFilter.get();
                    Row newRow = table.getTemplateRow();
                    for (int i = 0; i < columnCount; i++) {
                        Expression newExpr = expressions[i];
                        Value newValue;
                        if (newExpr == null) {
                            newValue = oldRow.getValue(i);
                        } else if (newExpr == ValueExpression.getDefault()) {
                            Column column = table.getColumn(i);
                            newValue = table.getDefaultValue(session, column);
                        } else {
                            Column column = table.getColumn(i);
                            newValue = column.convert(newExpr.getValue(session));
                        }
                        newRow.setValue(i, newValue);
                    }
                    table.validateConvertUpdateSequence(session, newRow);
                    boolean done = false;
                    if (table.fireRow()) {
                        done = table.fireBeforeRow(session, oldRow, newRow);
                    }
                    if (!done) {
                        rows.add(oldRow);
                        rows.add(newRow);
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
                rows.invalidateCache();
                for (rows.reset(); rows.hasNext();) {
                    Row o = rows.next();
                    Row n = rows.next();
                    table.fireAfterRow(session, o, n, false);
                }
            }
            table.fire(session, Trigger.UPDATE, false);
            return count;
        } finally {
            rows.close();
        }
    }

    public String getPlanSQL() {
        StatementBuilder buff = new StatementBuilder("UPDATE ");
        buff.append(tableFilter.getPlanSQL(false)).append("\nSET ");
        Table table = tableFilter.getTable();
        int columnCount = table.getColumns().length;
        for (int i = 0; i < columnCount; i++) {
            Expression newExpr = expressions[i];
            if (newExpr != null) {
                Column column = table.getColumn(i);
                buff.appendExceptFirst(",\n");
                buff.append(column.getName()).append(" = ").append(newExpr.getSQL());
            }
        }
        if (condition != null) {
            buff.append("\nWHERE ").append(StringUtils.unEnclose(condition.getSQL()));
        }
        return buff.toString();
    }

    public void prepare() {
        if (condition != null) {
            condition.mapColumns(tableFilter, 0);
            condition = condition.optimize(session);
            condition.createIndexConditions(session, tableFilter);
        }
        for (int i = 0; i < expressions.length; i++) {
            Expression expr = expressions[i];
            if (expr != null) {
                expr.mapColumns(tableFilter, 0);
                expressions[i] = expr.optimize(session);
            }
        }
        PlanItem item = tableFilter.getBestPlanItem(session, 1);
        tableFilter.setPlanItem(item);
        tableFilter.prepare();
    }

    public boolean isTransactional() {
        return true;
    }

    public ResultInterface queryMeta() {
        return null;
    }

}
