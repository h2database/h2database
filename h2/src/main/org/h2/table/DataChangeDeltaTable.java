/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.command.dml.DataChangeStatement;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
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

    private final DataChangeStatement statement;

    private final ResultOption resultOption;

    private final Expression[] expressions;

    public DataChangeDeltaTable(Schema schema, Session session, DataChangeStatement statement,
            ResultOption resultOption) {
        super(schema, 0, statement.getStatementName());
        this.statement = statement;
        this.resultOption = resultOption;
        Table table = statement.getTable();
        Column[] c = table.getColumns();
        setColumns(c);
        int columnCount = c.length;
        Expression[] expressions = new Expression[columnCount];
        String tableName = getName();
        for (int i = 0; i < columnCount; i++) {
            expressions[i] = new ExpressionColumn(database, null, tableName, c[i].getName(), false);
        }
        this.expressions = expressions;
    }

    @Override
    public boolean canGetRowCount() {
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        return Long.MAX_VALUE;
    }

    @Override
    public long getRowCountApproximation() {
        return Long.MAX_VALUE;
    }

    @Override
    public ResultInterface getResult(Session session) {
        statement.prepare();
        int columnCount = expressions.length;
        LocalResult result = session.getDatabase().getResultFactory().create(session, expressions, columnCount,
                columnCount);
        try {
            statement.setDeltaChangeCollector(result, resultOption);
            statement.update();
        } finally {
            statement.setDeltaChangeCollector(null, null);
        }
        return result;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        return builder.append(resultOption.name()).append(" TABLE (").append(statement.getSQL()).append(')');
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

}
