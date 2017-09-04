/*
 * Copyright 2004-2017 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;
import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.command.Prepared;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.engine.UndoLogRecord;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StatementBuilder;
import org.h2.value.Value;

/**
 * This class represents the statement syntax
 * MERGE table alias USING...
 */
public class MergeUsing extends Merge {
    
    private TableFilter sourceTableFilter;
    private ArrayList<Expression> conditions = new ArrayList<Expression>();
    private Prepared updateCommand;
    private Prepared deleteCommand;
    private Insert insertCommand;
    private String queryAlias;

    public MergeUsing(Merge merge) {
        super(merge.getSession());
        
        // bring across only the already parsed data from Merge...
        this.targetTable = merge.targetTable;
        this.targetTableFilter = merge.targetTableFilter;

    }

  
    @Override
    public int update() {
        int count;
        checkRights();
        setCurrentRowNumber(0);

        // process select query data for row creation
        ResultInterface rows = query.query(0);
        count = 0;
        targetTable.fire(session, Trigger.UPDATE | Trigger.INSERT, true);
        targetTable.lock(session, true, false);
        while (rows.next()) {
            count++;
            Value[] r = rows.currentRow();
            Row newRow = targetTable.getTemplateRow();
            setCurrentRowNumber(count);
            for (int j = 0; j < columns.length; j++) {
                Column c = columns[j];
                int index = c.getColumnId();
                try {
                    Value v = c.convert(r[j]);
                    newRow.setValue(index, v);
                } catch (DbException ex) {
                    throw setRow(ex, count, getSQL(r));
                }
            }
            merge(newRow);
        }
        rows.close();
        targetTable.fire(session, Trigger.UPDATE | Trigger.INSERT, false);
        return count;
    }

    private void checkRights() {
        if(insertCommand!=null){
            session.getUser().checkRight(targetTable, Right.INSERT);
        }
        if(updateCommand!=null){
            session.getUser().checkRight(targetTable, Right.UPDATE);
        }
        if(deleteCommand!=null){
            session.getUser().checkRight(targetTable, Right.DELETE);
        }
    }

    @Override
    protected void merge(Row row) {
        ArrayList<Parameter> k = update.getParameters();
        for (int i = 0; i < columns.length; i++) {
            Column col = columns[i];
            Value v = row.getValue(col.getColumnId());
            Parameter p = k.get(i);
            p.setValue(v);
        }
        for (int i = 0; i < keys.length; i++) {
            Column col = keys[i];
            Value v = row.getValue(col.getColumnId());
            if (v == null) {
                throw DbException.get(ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1, col.getSQL());
            }
            Parameter p = k.get(columns.length + i);
            p.setValue(v);
        }
        
        // try and update
        int count = 0;
        if(updateCommand!=null){
            count+=updateCommand.update();
        }
        if(deleteCommand!=null){
            count+=deleteCommand.update();
        }
        
        // if update does nothing, try an insert
        if (count == 0) {
            try {
                targetTable.validateConvertUpdateSequence(session, row);
                boolean done = targetTable.fireBeforeRow(session, null, row);
                if (!done) {
                    targetTable.lock(session, true, false);
                    //targetTable.addRow(session, row);
                    addRowByInsert(session,row);
                    session.log(targetTable, UndoLogRecord.INSERT, row);
                    targetTable.fireAfterRow(session, null, row, false);
                }
            } catch (DbException e) {
                if (e.getErrorCode() == ErrorCode.DUPLICATE_KEY_1) {
                    // possibly a concurrent merge or insert
                    Index index = (Index) e.getSource();
                    if (index != null) {
                        // verify the index columns match the key
                        Column[] indexColumns = index.getColumns();
                        boolean indexMatchesKeys = true;
                        if (indexColumns.length <= keys.length) {
                            for (int i = 0; i < indexColumns.length; i++) {
                                if (indexColumns[i] != keys[i]) {
                                    indexMatchesKeys = false;
                                    break;
                                }
                            }
                        }
                        if (indexMatchesKeys) {
                            throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, targetTable.getName());
                        }
                    }
                }
                throw e;
            }
        } else if (count != 1) {
            throw DbException.get(ErrorCode.DUPLICATE_KEY_1, targetTable.getSQL());
        }
    }

    private void addRowByInsert(Session session, Row row) {
        // TODO Auto-generated method stub
        targetTable.addRow(session, row);
    }


    @Override
    public String getPlanSQL() {
        StatementBuilder buff = new StatementBuilder("MERGE INTO ");
        buff.append(targetTable.getSQL()).append('(');
        for (Column c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(')');
        if (keys != null) {
            buff.append(" KEY(");
            buff.resetCount();
            for (Column c : keys) {
                buff.appendExceptFirst(", ");
                buff.append(c.getSQL());
            }
            buff.append(')');
        }
        buff.append('\n');
        if (valuesExpressionList.size() > 0) {
            buff.append("VALUES ");
            int row = 0;
            for (Expression[] expr : valuesExpressionList) {
                if (row++ > 0) {
                    buff.append(", ");
                }
                buff.append('(');
                buff.resetCount();
                for (Expression e : expr) {
                    buff.appendExceptFirst(", ");
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

    @Override
    public void prepare() {
        if (columns == null) {
            if (valuesExpressionList.size() > 0 && valuesExpressionList.get(0).length == 0) {
                // special case where table is used as a sequence
                columns = new Column[0];
            } else {
                columns = targetTable.getColumns();
            }
        }
        if (valuesExpressionList.size() > 0) {
            for (Expression[] expr : valuesExpressionList) {
                if (expr.length != columns.length) {
                    throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
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
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
        }
        if (keys == null) {
            Index idx = targetTable.getPrimaryKey();
            if (idx == null) {
                throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, "PRIMARY KEY");
            }
            keys = idx.getColumns();
        }
        StatementBuilder buff = new StatementBuilder("UPDATE ");
        buff.append(targetTable.getSQL()).append(" SET ");
        for (Column c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL()).append("=?");
        }
        buff.append(" WHERE ");
        buff.resetCount();
        for (Column c : keys) {
            buff.appendExceptFirst(" AND ");
            buff.append(c.getSQL()).append("=?");
        }
        String sql = buff.toString();
        update = session.prepare(sql);
    }
    
    public void setSourceTableFilter(TableFilter sourceTableFilter) {
        this.sourceTableFilter = sourceTableFilter;        
    }

    public void addCondition(Expression condition) {
        this.conditions .add(condition);        
    }
    
    public Prepared getUpdateCommand() {
        return updateCommand;
    }

    public void setUpdateCommand(Prepared updateCommand) {
        this.updateCommand = updateCommand;
    }
    
    public Prepared getDeleteCommand() {
        return deleteCommand;
    }

    public void setDeleteCommand(Prepared deleteCommand) {
        this.deleteCommand = deleteCommand;
    }
    
    public Insert getInsertCommand() {
        return insertCommand;
    }

    public void setInsertCommand(Insert insertCommand) {
        this.insertCommand = insertCommand;
    }

    public void setQueryAlias(String alias) {
        this.queryAlias = alias;

    }    
}
