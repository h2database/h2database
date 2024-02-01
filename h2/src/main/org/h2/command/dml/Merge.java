/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
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
import org.h2.command.query.Query;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.expression.ValueExpression;
import org.h2.index.Index;
import org.h2.message.DbException;
import org.h2.mvstore.db.MVPrimaryIndex;
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.table.DataChangeDeltaTable;
import org.h2.table.DataChangeDeltaTable.ResultOption;
import org.h2.table.Table;
import org.h2.util.HasSQL;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This class represents the statement
 * MERGE
 * or the MySQL compatibility statement
 * REPLACE
 */
public final class Merge extends CommandWithValues {

    private boolean isReplace;

    private Table table;
    private Column[] columns;
    private Column[] keys;
    private Query query;
    private Update update;

    public Merge(SessionLocal session, boolean isReplace) {
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
    public long update(ResultTarget deltaChangeCollector, ResultOption deltaChangeCollectionMode) {
        long count = 0;
        session.getUser().checkTableRight(table, Right.INSERT);
        session.getUser().checkTableRight(table, Right.UPDATE);
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
                    if (e != ValueExpression.DEFAULT) {
                        try {
                            newRow.setValue(index, e.getValue(session));
                        } catch (DbException ex) {
                            throw setRow(ex, count, getSimpleSQL(expr));
                        }
                    }
                }
                count += merge(newRow, expr, deltaChangeCollector, deltaChangeCollectionMode);
            }
        } else {
            // process select data for list
            query.setNeverLazy(true);
            ResultInterface rows = query.query(0);
            table.fire(session, Trigger.UPDATE | Trigger.INSERT, true);
            table.lock(session, Table.WRITE_LOCK);
            while (rows.next()) {
                Value[] r = rows.currentRow();
                Row newRow = table.getTemplateRow();
                setCurrentRowNumber(count);
                for (int j = 0; j < columns.length; j++) {
                    newRow.setValue(columns[j].getColumnId(), r[j]);
                }
                count += merge(newRow, null, deltaChangeCollector, deltaChangeCollectionMode);
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
     * @param expressions source expressions, or null
     * @param deltaChangeCollector target result
     * @param deltaChangeCollectionMode collection mode
     * @return 1 if row was inserted, 1 if row was updated by a MERGE statement,
     *         and 2 if row was updated by a REPLACE statement
     */
    private int merge(Row row, Expression[] expressions, ResultTarget deltaChangeCollector,
            ResultOption deltaChangeCollectionMode) {
        long count;
        if (update == null) {
            // if there is no valid primary key,
            // the REPLACE statement degenerates to an INSERT
            count = 0;
        } else {
            ArrayList<Parameter> k = update.getParameters();
            int j = 0;
            for (int i = 0, l = columns.length; i < l; i++) {
                Column col = columns[i];
                if (col.isGeneratedAlways()) {
                    if (expressions == null || expressions[i] != ValueExpression.DEFAULT) {
                        throw DbException.get(ErrorCode.GENERATED_COLUMN_CANNOT_BE_ASSIGNED_1,
                                col.getSQLWithTable(new StringBuilder(), HasSQL.TRACE_SQL_FLAGS).toString());
                    }
                } else {
                    Value v = row.getValue(col.getColumnId());
                    if (v == null) {
                        Expression defaultExpression = col.getEffectiveDefaultExpression();
                        v = defaultExpression != null ? defaultExpression.getValue(session) : ValueNull.INSTANCE;
                    }
                    k.get(j++).setValue(v);
                }
            }
            for (Column col : keys) {
                Value v = row.getValue(col.getColumnId());
                if (v == null) {
                    throw DbException.get(ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1, col.getTraceSQL());
                }
                TypeInfo colType = col.getType();
                check: {
                    TypeInfo rightType = v.getType();
                    if (session.getMode().numericWithBooleanComparison) {
                        int lValueType = colType.getValueType();
                        if (lValueType == Value.BOOLEAN) {
                            if (DataType.isNumericType(rightType.getValueType())) {
                                break check;
                            }
                        } else if (DataType.isNumericType(lValueType) && rightType.getValueType() == Value.BOOLEAN) {
                            break check;
                        }
                    }
                    TypeInfo.checkComparable(colType, rightType);
                }
                k.get(j++).setValue(v.convertForAssignTo(colType, session, col));
            }
            count = update.update(deltaChangeCollector, deltaChangeCollectionMode);
        }
        // if update fails try an insert
        if (count == 0) {
            try {
                table.convertInsertRow(session, row, null);
                if (deltaChangeCollectionMode == ResultOption.NEW) {
                    deltaChangeCollector.addRow(row.getValueList().clone());
                }
                if (!table.fireBeforeRow(session, null, row)) {
                    table.lock(session, Table.WRITE_LOCK);
                    table.addRow(session, row);
                    DataChangeDeltaTable.collectInsertedFinalRow(session, table, deltaChangeCollector,
                            deltaChangeCollectionMode, row);
                    table.fireAfterRow(session, null, row, false);
                } else {
                    DataChangeDeltaTable.collectInsertedFinalRow(session, table, deltaChangeCollector,
                            deltaChangeCollectionMode, row);
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
        throw DbException.get(ErrorCode.DUPLICATE_KEY_1, table.getTraceSQL());
    }

    @Override
    public StringBuilder getPlanSQL(StringBuilder builder, int sqlFlags) {
        builder.append(isReplace ? "REPLACE INTO " : "MERGE INTO ");
        table.getSQL(builder, sqlFlags).append('(');
        Column.writeColumns(builder, columns, sqlFlags);
        builder.append(')');
        if (!isReplace && keys != null) {
            builder.append(" KEY(");
            Column.writeColumns(builder, keys, sqlFlags);
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
                Expression.writeExpressions(builder.append('('), expr, sqlFlags).append(')');
            }
        } else {
            query.getPlanSQL(builder, sqlFlags);
        }
        return builder;
    }

    @Override
    void doPrepare() {
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
        StringBuilder builder = table.getSQL(new StringBuilder("UPDATE "), HasSQL.DEFAULT_SQL_FLAGS).append(" SET ");
        boolean hasColumn = false;
        for (int i = 0, l = columns.length; i < l; i++) {
            Column column = columns[i];
            if (!column.isGeneratedAlways()) {
                if (hasColumn) {
                    builder.append(", ");
                }
                hasColumn = true;
                column.getSQL(builder, HasSQL.DEFAULT_SQL_FLAGS).append("=?");
            }
        }
        if (!hasColumn) {
            throw DbException.getSyntaxError(sqlStatement, sqlStatement.length(),
                    "Valid MERGE INTO statement with at least one updatable column");
        }
        Column.writeColumns(builder.append(" WHERE "), keys, " AND ", "=?", HasSQL.DEFAULT_SQL_FLAGS);
        update = (Update) session.prepare(builder.toString());
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
    public void collectDependencies(HashSet<DbObject> dependencies) {
        if (query != null) {
            query.collectDependencies(dependencies);
        }
    }

}
