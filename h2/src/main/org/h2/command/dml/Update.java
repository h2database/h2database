/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.table.Column;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2.value.Value;

/**
 * Represents a union UPDATE statement.
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

    public void setAssignment(Column column, Expression expression)
            throws SQLException {
        int id = column.getColumnId();
        if (expressions[id] != null) {
            throw Message.getSQLException(ErrorCode.DUPLICATE_COLUMN_NAME_1, column
                    .getName());
        }
        expressions[id] = expression;
    }

    public int update() throws SQLException {
        tableFilter.startQuery(session);
        tableFilter.reset();
        RowList rows = new RowList(session);
        try {
            Table table = tableFilter.getTable();
            session.getUser().checkRight(table, Right.UPDATE);
            table.fireBefore(session);
            table.lock(session, true, false);
            int columnCount = table.getColumns().length;
            // get the old rows, compute the new rows
            setCurrentRowNumber(0);
            int count = 0;
            while (tableFilter.next()) {
                checkCancelled();
                setCurrentRowNumber(count+1);
                if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                    Row oldRow = tableFilter.get();
                    Row newRow = table.getTemplateRow();
                    for (int i = 0; i < columnCount; i++) {
                        Expression newExpr = expressions[i];
                        Value newValue;
                        if (newExpr == null) {
                            newValue = oldRow.getValue(i);
                        } else {
                            Column column = table.getColumn(i);
                            newValue = newExpr.getValue(session).convertTo(column.getType());
                        }
                        newRow.setValue(i, newValue);
                    }
                    table.validateConvertUpdateSequence(session, newRow);
                    if (table.fireRow()) {
                        table.fireBeforeRow(session, oldRow, newRow);
                    }
                    rows.add(oldRow);
                    rows.add(newRow);
                    count++;
                }
            }
            // TODO self referencing referential integrity constraints don't work if update is multi-row and 'inversed' the condition! 
            // probably need multi-row triggers with 'deleted' and 'inserted' at the same time. anyway good for sql compatibility
            // TODO update in-place (but if the position changes, we need to update all indexes) before row triggers
            
            // the cached row is already updated - we need the old values
            table.updateRows(this, session, rows);
            if (table.fireRow()) {
                rows.invalidateCache();
                for (rows.reset(); rows.hasNext();) {
                    checkCancelled();
                    Row o = rows.next();
                    Row n = rows.next();
                    table.fireAfterRow(session, o, n);
                }
            }
            table.fireAfter(session);
            return count;
        } finally {
            rows.close();
        }
    }
    
    public String getPlanSQL() {
        StringBuffer buff = new StringBuffer();
        buff.append("UPDATE ");
        buff.append(tableFilter.getPlanSQL(false));
        buff.append("\nSET ");
        Table table = tableFilter.getTable();
        int columnCount = table.getColumns().length;
        for (int i = 0, j = 0; i < columnCount; i++) {
            Expression newExpr = expressions[i];
            if (newExpr != null) {
                if (j > 0) {
                    buff.append(",\n");
                }
                j++;
                Column column = table.getColumn(i);
                buff.append(column.getName());
                buff.append(" = ");
                buff.append(newExpr.getSQL());
            }
        }
        if (condition != null) {
            buff.append("\nWHERE " + StringUtils.unEnclose(condition.getSQL()));
        }
        return buff.toString();
    }

    public void prepare() throws SQLException {
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
        PlanItem item = tableFilter.getBestPlanItem(session);
        tableFilter.setPlanItem(item);
        tableFilter.prepare();
    }

    public boolean isTransactional() {
        return true;
    }

    public LocalResult queryMeta() {
        return null;
    }

}
