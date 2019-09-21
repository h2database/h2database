/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;
import java.util.HashSet;
import org.h2.api.ErrorCode;
import org.h2.api.Trigger;
import org.h2.command.Command;
import org.h2.command.CommandInterface;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.engine.UndoLogRecord;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.mvstore.db.MVPrimaryIndex;
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.table.DataChangeDeltaTable.ResultOption;
import org.h2.table.Table;
import org.h2.value.Value;

/**
 * This class represents the statement
 * MERGE
 * or the MySQL compatibility statement
 * REPLACE
 */
public class Merge extends CommandWithValues implements DataChangeStatement {

    private boolean isReplace;

    private Table table;
    private Column[] columns;
    private Column[] keys;
    private Query query;
    private Update update;

    private ResultTarget deltaChangeCollector;

    private ResultOption deltaChangeCollectionMode;

    public Merge(Session session, boolean isReplace) {
        super(session);
        this.isReplace = isReplace;
    }

    @Override
    public void setCommand(Command command) {
        super.setCommand(command);
        if (query != null) {
            query.setCommand(command);
        }
    }

    @Override
    public Table getTable() {
        return table;
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

    @Override
    public void setDeltaChangeCollector(ResultTarget deltaChangeCollector, ResultOption deltaChangeCollectionMode) {
        this.deltaChangeCollector = deltaChangeCollector;
        this.deltaChangeCollectionMode = deltaChangeCollectionMode;
        update.setDeltaChangeCollector(deltaChangeCollector, deltaChangeCollectionMode);
    }

    @Override
    public int update() {
        int count = 0;
        session.getUser().checkRight(table, Right.INSERT);
        session.getUser().checkRight(table, Right.UPDATE);
        setCurrentRowNumber(0);
        if (!valuesExpressionList.isEmpty()) {
            // process values in list
            for (int x = 0, size = valuesExpressionList.size(); x < size; x++) {
                setCurrentRowNumber(x + 1);
                Expression[] expr = valuesExpressionList.get(x);
                Row newRow = table.getTemplateRow();
                for (int i = 0, len = columns.length; i < len; i++) {
                    Column c = columns[i];
                    int index = c.getColumnId();
                    Expression e = expr[i];
                    if (e != null) {
                        // e can be null (DEFAULT)
                        try {
                            newRow.setValue(index, e.getValue(session));
                        } catch (DbException ex) {
                            throw setRow(ex, count, getSimpleSQL(expr));
                        }
                    }
                }
                count += merge(newRow);
            }
        } else {
            // process select data for list
            query.setNeverLazy(true);
            ResultInterface rows = query.query(0);
            table.fire(session, Trigger.UPDATE | Trigger.INSERT, true);
            table.lock(session, true, false);
            while (rows.next()) {
                Value[] r = rows.currentRow();
                Row newRow = table.getTemplateRow();
                setCurrentRowNumber(count);
                for (int j = 0; j < columns.length; j++) {
                    newRow.setValue(columns[j].getColumnId(), r[j]);
                }
                count += merge(newRow);
            }
            rows.close();
            table.fire(session, Trigger.UPDATE | Trigger.INSERT, false);
        }
        return count;
    }

    /**
     * Updates an existing row or inserts a new one.
     *
     * @param row row to replace
     * @return 1 if row was inserted, 1 if row was updated by a MERGE statement,
     *         and 2 if row was updated by a REPLACE statement
     */
    private int merge(Row row) {
        int count;
        if (update == null) {
            // if there is no valid primary key,
            // the REPLACE statement degenerates to an INSERT
            count = 0;
        } else {
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
                    throw DbException.get(ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1, col.getSQL(false));
                }
                Parameter p = k.get(columns.length + i);
                p.setValue(v);
            }
            count = update.update();
        }
        // if update fails try an insert
        if (count == 0) {
            try {
                table.validateConvertUpdateSequence(session, row);
                if (deltaChangeCollectionMode == ResultOption.NEW) {
                    deltaChangeCollector.addRow(row.getValueList().clone());
                }
                boolean done = table.fireBeforeRow(session, null, row);
                if (!done) {
                    table.lock(session, true, false);
                    table.addRow(session, row);
                    if (deltaChangeCollectionMode == ResultOption.FINAL) {
                        deltaChangeCollector.addRow(row.getValueList());
                    }
                    session.log(table, UndoLogRecord.INSERT, row);
                    table.fireAfterRow(session, null, row, false);
                }
                return 1;
            } catch (DbException e) {
                if (e.getErrorCode() == ErrorCode.DUPLICATE_KEY_1) {
                    // possibly a concurrent merge or insert
                    Index index = (Index) e.getSource();
                    if (index != null) {
                        // verify the index columns match the key
                        Column[] indexColumns;
                        if (index instanceof MVPrimaryIndex) {
                            MVPrimaryIndex foundMV = (MVPrimaryIndex) index;
                            indexColumns = new Column[] {
                                    foundMV.getIndexColumns()[foundMV.getMainIndexColumn()].column };
                        } else {
                            indexColumns = index.getColumns();
                        }
                        boolean indexMatchesKeys;
                        if (indexColumns.length <= keys.length) {
                            indexMatchesKeys = true;
                            for (int i = 0; i < indexColumns.length; i++) {
                                if (indexColumns[i] != keys[i]) {
                                    indexMatchesKeys = false;
                                    break;
                                }
                            }
                        } else {
                            indexMatchesKeys = false;
                        }
                        if (indexMatchesKeys) {
                            throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, table.getName());
                        }
                    }
                }
                throw e;
            }
        } else if (count == 1) {
            return isReplace ? 2 : 1;
        }
        throw DbException.get(ErrorCode.DUPLICATE_KEY_1, table.getSQL(false));
    }

    @Override
    public String getPlanSQL(boolean alwaysQuote) {
        StringBuilder builder = new StringBuilder(isReplace ? "REPLACE INTO " : "MERGE INTO ");
        table.getSQL(builder, alwaysQuote).append('(');
        Column.writeColumns(builder, columns, alwaysQuote);
        builder.append(')');
        if (!isReplace && keys != null) {
            builder.append(" KEY(");
            Column.writeColumns(builder, keys, alwaysQuote);
            builder.append(')');
        }
        builder.append('\n');
        if (!valuesExpressionList.isEmpty()) {
            builder.append("VALUES ");
            int row = 0;
            for (Expression[] expr : valuesExpressionList) {
                if (row++ > 0) {
                    builder.append(", ");
                }
                builder.append('(');
                Expression.writeExpressions(builder, expr, alwaysQuote);
                builder.append(')');
            }
        } else {
            builder.append(query.getPlanSQL(alwaysQuote));
        }
        return builder.toString();
    }

    @Override
    public void prepare() {
        if (columns == null) {
            if (!valuesExpressionList.isEmpty() && valuesExpressionList.get(0).length == 0) {
                // special case where table is used as a sequence
                columns = new Column[0];
            } else {
                columns = table.getColumns();
            }
        }
        if (!valuesExpressionList.isEmpty()) {
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
            Index idx = table.getPrimaryKey();
            if (idx == null) {
                throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, "PRIMARY KEY");
            }
            keys = idx.getColumns();
        }
        if (isReplace) {
            // if there is no valid primary key,
            // the REPLACE statement degenerates to an INSERT
            for (Column key : keys) {
                boolean found = false;
                for (Column column : columns) {
                    if (column.getColumnId() == key.getColumnId()) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return;
                }
            }
        }
        StringBuilder builder = new StringBuilder("UPDATE ");
        table.getSQL(builder, true).append(" SET ");
        Column.writeColumns(builder, columns, ", ", "=?", true).append(" WHERE ");
        Column.writeColumns(builder, keys, " AND ", "=?", true);
        update = (Update) session.prepare(builder.toString());
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
        return isReplace ? CommandInterface.REPLACE : CommandInterface.MERGE;
    }

    @Override
    public String getStatementName() {
        return isReplace ? "REPLACE" : "MERGE";
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    @Override
    public void collectDependencies(HashSet<DbObject> dependencies) {
        if (query != null) {
            query.collectDependencies(dependencies);
        }
    }
}
