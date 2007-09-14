/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;

import org.h2.command.Command;
import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.index.Index;
import org.h2.log.UndoLogRecord;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

/**
 * @author Thomas
 */
public class Merge extends Prepared {

    private Table table;
    private Column[] columns;
    private Column[] keys;
    private ObjectArray list = new ObjectArray();
    private Query query;
    private Prepared update;

    public Merge(Session session) {
        super(session);
    }

    public void setCommand(Command command) {
        super.setCommand(command);
        if (query != null) {
            query.setCommand(command);
        }
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
    }

    public void setKeys(Column[] keys) {
        this.keys = keys;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public void addRow(Expression[] expr) {
        list.add(expr);
    }

    public int update() throws SQLException {
        int count;
        session.getUser().checkRight(table, Right.INSERT);
        session.getUser().checkRight(table, Right.UPDATE);
        if (keys == null) {
            Index idx = table.getPrimaryKey();
            if (idx == null) {
                throw Message.getSQLException(ErrorCode.CONSTRAINT_NOT_FOUND_1, "PRIMARY KEY");
            }
            keys = idx.getColumns();
        }
        StringBuffer buff = new StringBuffer("UPDATE ");
        buff.append(table.getSQL());
        buff.append(" SET ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(columns[i].getSQL());
            buff.append("=?");
        }
        buff.append(" WHERE ");
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) {
                buff.append(" AND ");
            }
            buff.append(keys[i].getSQL());
            buff.append("=?");
        }
        String sql = buff.toString();
        update = session.prepare(sql);
        setCurrentRowNumber(0);
        if (list.size() > 0) {
            count = 0;
            for (int x = 0; x < list.size(); x++) {
                setCurrentRowNumber(x + 1);
                Expression[] expr = (Expression[]) list.get(x);
                Row newRow = table.getTemplateRow();
                for (int i = 0; i < columns.length; i++) {
                    Column c = columns[i];
                    int index = c.getColumnId();
                    Expression e = expr[i];
                    if (e != null) {
                        // e can be null (DEFAULT)
                        Value v = expr[i].getValue(session).convertTo(c.getType());
                        newRow.setValue(index, v);
                    }
                }
                merge(newRow);
                count++;
            }
        } else {
            LocalResult rows = query.query(0);
            count = 0;
            table.fireBefore(session);
            table.lock(session, true, false);
            while (rows.next()) {
                checkCancelled();
                count++;
                Value[] r = rows.currentRow();
                Row newRow = table.getTemplateRow();
                setCurrentRowNumber(count);
                for (int j = 0; j < columns.length; j++) {
                    Column c = columns[j];
                    int index = c.getColumnId();
                    Value v = r[j].convertTo(c.getType());
                    newRow.setValue(index, v);
                }
                merge(newRow);
            }
            rows.close();
            table.fireAfter(session);
        }
        return count;
    }

    private void merge(Row row) throws SQLException {
        ObjectArray k = update.getParameters();
        for (int i = 0; i < columns.length; i++) {
            Column col = columns[i];
            Value v = row.getValue(col.getColumnId());
            Parameter p = (Parameter) k.get(i);
            p.setValue(v);
        }
        for (int i = 0; i < keys.length; i++) {
            Column col = keys[i];
            Value v = row.getValue(col.getColumnId());
            if (v == null) {
                throw Message.getSQLException(ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1, col.getSQL());
            }
            Parameter p = (Parameter) k.get(columns.length + i);
            p.setValue(v);
        }
        int count = update.update();
        if (count == 0) {
            table.fireBefore(session);
            table.validateConvertUpdateSequence(session, row);
            table.fireBeforeRow(session, null, row);
            table.lock(session, true, false);
            table.addRow(session, row);
            session.log(table, UndoLogRecord.INSERT, row);
            table.fireAfter(session);
            table.fireAfterRow(session, null, row);
        } else if (count != 1) {
            throw Message.getSQLException(ErrorCode.DUPLICATE_KEY_1, table.getSQL());
        }
    }

    public String getPlanSQL() {
        StringBuffer buff = new StringBuffer();
        buff.append("MERGE INTO ");
        buff.append(table.getSQL());
        buff.append('(');
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(columns[i].getSQL());
        }
        buff.append(")");
        if (keys != null) {
            buff.append(" KEY(");
            for (int i = 0; i < keys.length; i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append(keys[i].getSQL());
            }
            buff.append(")");
        }
        buff.append('\n');
        if (list.size() > 0) {
            buff.append("VALUES ");
            for (int x = 0; x < list.size(); x++) {
                Expression[] expr = (Expression[]) list.get(x);
                if (x > 0) {
                    buff.append(", ");
                }
                buff.append("(");
                for (int i = 0; i < columns.length; i++) {
                    if (i > 0) {
                        buff.append(", ");
                    }
                    Expression e = expr[i];
                    if (e == null) {
                        buff.append("DEFAULT");
                    } else {
                        buff.append(e.getSQL());
                    }
                }
                buff.append(')');
            }
        } else {
            buff.append(query.getPlanSQL());
        }
        return buff.toString();
    }

    public void prepare() throws SQLException {
        if (columns == null) {
            if (list.size() > 0 && ((Expression[]) list.get(0)).length == 0) {
                // special case where table is used as a sequence
                columns = new Column[0];
            } else {
                columns = table.getColumns();
            }
        }
        if (list.size() > 0) {
            for (int x = 0; x < list.size(); x++) {
                Expression[] expr = (Expression[]) list.get(x);
                if (expr.length != columns.length) {
                    throw Message.getSQLException(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
                }
                for (int i = 0; i < expr.length; i++) {
                    Expression e = expr[i];
                    if (e != null) {
                        expr[i] = e.optimize(session);
                    }
                }
            }
        } else {
            query.prepare();
            if (query.getColumnCount() != columns.length) {
                throw Message.getSQLException(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
        }
    }

    public boolean isTransactional() {
        return true;
    }

    public LocalResult queryMeta() {
        return null;
    }
    
}
