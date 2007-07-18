/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;

import org.h2.command.Command;
import org.h2.command.Prepared;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.store.UndoLogRecord;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

/**
 * @author Thomas
 */
public class Insert extends Prepared {

    private Table table;
    private Column[] columns;
    private ObjectArray list = new ObjectArray();
    private Query query;

    public Insert(Session session) {
        super(session);
    }

    public void setCommand(Command command) {
        super.setCommand(command);
        if(query != null) {
            query.setCommand(command);
        }
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
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
        setCurrentRowNumber(0);
        if(list.size() > 0) {
            count = 0;
            for(int x=0; x<list.size(); x++) {
                Expression[] expr = (Expression[])list.get(x);
                Row newRow = table.getTemplateRow();
                setCurrentRowNumber(x+1);
                for (int i = 0; i < columns.length; i++) {
                    Column c = columns[i];
                    int index = c.getColumnId();
                    Expression e = expr[i];
                    if(e != null) {
                        // e can be null (DEFAULT)
                        e = e.optimize(session);
                        Value v = e.getValue(session).convertTo(c.getType());
                        newRow.setValue(index, v);
                    }
                }
                // TODO insert: set default values before calling triggers?
                checkCancelled();
                table.fireBefore(session);
                table.validateConvertUpdateSequence(session, newRow);
                table.fireBeforeRow(session, null, newRow);
                table.lock(session, true);
                table.addRow(session, newRow);
                session.log(table, UndoLogRecord.INSERT, newRow);
                table.fireAfter(session);
                table.fireAfterRow(session, null, newRow);
                count++;
            }
        } else {
            LocalResult rows = query.query(0);
            count = 0;
            table.fireBefore(session);
            table.lock(session, true);
            while(rows.next()) {
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
                table.validateConvertUpdateSequence(session, newRow);
                table.fireBeforeRow(session, null, newRow);
                table.addRow(session, newRow);
                session.log(table, UndoLogRecord.INSERT, newRow);
                table.fireAfterRow(session, null, newRow);
            }
            rows.close();
            table.fireAfter(session);
        }
        return count;
    }

    public String getPlanSQL() {
        StringBuffer buff = new StringBuffer();
        buff.append("INSERT INTO ");
        buff.append(table.getSQL());
        buff.append('(');
        for(int i=0; i<columns.length; i++) {
            if(i>0) {
                buff.append(", ");
            }
            buff.append(columns[i].getSQL());
        }
        buff.append(")\n");
        if(list.size() > 0) {
            buff.append("VALUES ");
            for(int x=0; x<list.size(); x++) {
                Expression[] expr = (Expression[])list.get(x);
                if(x > 0) {
                    buff.append(", ");
                }
                buff.append("(");
                for (int i = 0; i < columns.length; i++) {
                    if(i>0) {
                        buff.append(", ");
                    }
                    Expression e = expr[i];
                    if(e == null) {
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
            if(list.size() > 0 && ((Expression[])list.get(0)).length == 0) {
                // special case where table is used as a sequence
                columns = new Column[0];
            } else {
                columns = table.getColumns();
            }
        }
        if(list.size() > 0) {
            for(int x=0; x<list.size(); x++) {
                Expression[] expr = (Expression[])list.get(x);
                if(expr.length != columns.length) {
                    throw Message.getSQLException(Message.COLUMN_COUNT_DOES_NOT_MATCH);
                }
                for(int i=0; i<expr.length; i++) {
                    Expression e = expr[i];
                    if(e != null) {
                        expr[i] = e.optimize(session);
                    }
                }
            }
        } else {
            query.prepare();
            if(query.getColumnCount() != columns.length) {
                throw Message.getSQLException(Message.COLUMN_COUNT_DOES_NOT_MATCH);
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
