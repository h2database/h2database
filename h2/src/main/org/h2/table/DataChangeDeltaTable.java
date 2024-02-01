/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.command.dml.DataChangeStatement;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.result.Row;
import org.h2.schema.Schema;

/**
 * A data change delta table.
 */
public class DataChangeDeltaTable extends VirtualConstructedTable {

    /**
     * Result option.
     */
    public enum ResultOption {

        /**
         * OLD row.
         */
        OLD,

        /**
         * NEW row with evaluated default expressions, but before triggers.
         */
        NEW,

        /**
         * FINAL rows after triggers.
         */
        FINAL;

    }

    /**
     * Collects final row for INSERT operations.
     *
     * @param session
     *            the session
     * @param table
     *            the table
     * @param deltaChangeCollector
     *            target result
     * @param deltaChangeCollectionMode
     *            collection mode
     * @param newRow
     *            the inserted row
     */
    public static void collectInsertedFinalRow(SessionLocal session, Table table, ResultTarget deltaChangeCollector,
            ResultOption deltaChangeCollectionMode, Row newRow) {
        if (session.getMode().takeInsertedIdentity) {
            Column column = table.getIdentityColumn();
            if (column != null) {
                session.setLastIdentity(newRow.getValue(column.getColumnId()));
            }
        }
        if (deltaChangeCollectionMode == ResultOption.FINAL) {
            deltaChangeCollector.addRow(newRow.getValueList());
        }
    }

    private final DataChangeStatement statement;

    private final ResultOption resultOption;

    private final Expression[] expressions;

    public DataChangeDeltaTable(Schema schema, SessionLocal session, DataChangeStatement statement,
            ResultOption resultOption) {
        super(schema, 0, statement.getStatementName());
        this.statement = statement;
        this.resultOption = resultOption;
        Table table = statement.getTable();
        Column[] tableColumns = table.getColumns();
        int columnCount = tableColumns.length;
        Column[] c = new Column[columnCount];
        for (int i = 0; i < columnCount; i++) {
            c[i] = tableColumns[i].getClone();
        }
        setColumns(c);
        Expression[] expressions = new Expression[columnCount];
        String tableName = getName();
        for (int i = 0; i < columnCount; i++) {
            expressions[i] = new ExpressionColumn(database, null, tableName, c[i].getName());
        }
        this.expressions = expressions;
    }

    @Override
    public boolean canGetRowCount(SessionLocal session) {
        return false;
    }

    @Override
    public long getRowCount(SessionLocal session) {
        return Long.MAX_VALUE;
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return Long.MAX_VALUE;
    }

    @Override
    public ResultInterface getResult(SessionLocal session) {
        statement.prepare();
        int columnCount = expressions.length;
        LocalResult result = new LocalResult(session, expressions, columnCount, columnCount);
        result.setForDataChangeDeltaTable();
        statement.update(result, resultOption);
        return result;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, int sqlFlags) {
        return builder.append(resultOption.name()).append(" TABLE (").append(statement.getSQL()).append(')');
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

}
