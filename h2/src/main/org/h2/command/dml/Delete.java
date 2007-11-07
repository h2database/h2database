/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.log.UndoLogRecord;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;

/**
 * Represents a DELETE statement.
 */
public class Delete extends Prepared {

    private Expression condition;
    private TableFilter tableFilter;

    public Delete(Session session) {
        super(session);
    }

    public void setTableFilter(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    public int update() throws SQLException {
        tableFilter.startQuery(session);
        tableFilter.reset();
        Table table = tableFilter.getTable();
        session.getUser().checkRight(table, Right.DELETE);
        table.fireBefore(session);
        table.lock(session, true, false);
        ObjectArray rows = new ObjectArray();
        setCurrentRowNumber(0);
        while (tableFilter.next()) {
            checkCancelled();
            setCurrentRowNumber(rows.size() + 1);
            if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                Row row = tableFilter.get();
                rows.add(row);
            }
        }
        if (table.fireRow()) {
            for (int i = 0; i < rows.size(); i++) {
                checkCancelled();
                Row row = (Row) rows.get(i);
                table.fireBeforeRow(session, row, null);
            }
        }
        for (int i = 0; i < rows.size(); i++) {
            checkCancelled();
            Row row = (Row) rows.get(i);
            table.removeRow(session, row);
            session.log(table, UndoLogRecord.DELETE, row);
        }
        if (table.fireRow()) {
            for (int i = 0; i < rows.size(); i++) {
                checkCancelled();
                Row row = (Row) rows.get(i);
                table.fireAfterRow(session, row, null);
            }
        }
        table.fireAfter(session);
        return rows.size();
    }

    public String getPlanSQL() {
        StringBuffer buff = new StringBuffer();
        buff.append("DELETE FROM ");
        buff.append(tableFilter.getPlanSQL(false));
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
