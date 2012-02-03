/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.FunctionCall;
import org.h2.expression.TableFunction;
import org.h2.index.FunctionIndex;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.util.ObjectArray;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueResultSet;

/**
 * A table backed by a system or user-defined function that returns a result set.
 */
public class FunctionTable extends Table {

    private final FunctionCall function;
    private final long rowCount;
    private Expression functionExpr;
    private LocalResult cachedResult;
    private Value cachedValue;

    public FunctionTable(Schema schema, Session session, Expression functionExpr, FunctionCall function) throws SQLException {
        super(schema, 0, function.getName(), false, true);
        this.functionExpr = functionExpr;
        this.function = function;
        if (function instanceof TableFunction) {
            rowCount = ((TableFunction) function).getRowCount();
        } else {
            rowCount = Long.MAX_VALUE;
        }
        function.optimize(session);
        int type = function.getType();
        if (type != Value.RESULT_SET) {
            throw Message.getSQLException(ErrorCode.FUNCTION_MUST_RETURN_RESULT_SET_1, function.getName());
        }
        int params = function.getParameterCount();
        Expression[] columnListArgs = new Expression[params];
        Expression[] args = function.getArgs();
        for (int i = 0; i < params; i++) {
            args[i] = args[i].optimize(session);
            columnListArgs[i] = args[i];
        }
        ValueResultSet template = function.getValueForColumnList(session, columnListArgs);
        if (template == null) {
            throw Message.getSQLException(ErrorCode.FUNCTION_MUST_RETURN_RESULT_SET_1, function.getName());
        }
        ResultSet rs = template.getResultSet();
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        Column[] cols = new Column[columnCount];
        for (int i = 0; i < columnCount; i++) {
            cols[i] = new Column(meta.getColumnName(i + 1), DataType.convertSQLTypeToValueType(meta
                    .getColumnType(i + 1)), meta.getPrecision(i + 1), meta.getScale(i + 1), meta.getColumnDisplaySize(i + 1));
        }
        setColumns(cols);
    }

    public void lock(Session session, boolean exclusive, boolean force) {
        // nothing to do
    }

    public void close(Session session) {
        // nothing to do
    }

    public void unlock(Session s) {
        // nothing to do
    }

    public boolean isLockedExclusively() {
        return false;
    }

    public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            int headPos, String comment) throws SQLException {
        throw Message.getUnsupportedException("ALIAS");
    }

    public void removeRow(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException("ALIAS");
    }

    public void truncate(Session session) throws SQLException {
        throw Message.getUnsupportedException("ALIAS");
    }

    public boolean canDrop() {
        throw Message.throwInternalError();
    }

    public void addRow(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException("ALIAS");
    }

    public void checkSupportAlter() throws SQLException {
        throw Message.getUnsupportedException("ALIAS");
    }

    public String getTableType() {
        return null;
    }

    public Index getScanIndex(Session session) {
        return new FunctionIndex(this, IndexColumn.wrap(columns));
    }

    public ObjectArray<Index> getIndexes() {
        return null;
    }

    public boolean canGetRowCount() {
        return rowCount != Long.MAX_VALUE;
    }

    public long getRowCount(Session session) {
        return rowCount;
    }

    public String getCreateSQL() {
        return null;
    }

    public String getDropSQL() {
        return null;
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException("ALIAS");
    }

    /**
     * Read the result set from the function.
     *
     * @param session the session
     * @return the result set
     */
    public LocalResult getResult(Session session) throws SQLException {
        functionExpr = functionExpr.optimize(session);
        Value v = functionExpr.getValue(session);
        if (cachedResult != null && cachedValue == v) {
            cachedResult.reset();
            return cachedResult;
        }
        if (v == ValueNull.INSTANCE) {
            return new LocalResult();
        }
        ValueResultSet value = (ValueResultSet) v;
        ResultSet rs = value.getResultSet();
        LocalResult result = LocalResult.read(session,  rs, 0);
        if (function.isDeterministic()) {
            cachedResult = result;
            cachedValue = v;
        }
        return result;
    }

    public long getMaxDataModificationId() {
        // TODO optimization: table-as-a-function currently doesn't know the
        // last modified date
        return Long.MAX_VALUE;
    }

    public Index getUniqueIndex() {
        return null;
    }

    public String getSQL() {
        return function.getSQL();
    }

    public long getRowCountApproximation() {
        return rowCount;
    }

    public boolean isDeterministic() {
        return function.isDeterministic();
    }

}
