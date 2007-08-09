/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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
import org.h2.value.ValueResultSet;

public class FunctionTable extends Table {
    
    private final FunctionCall function;
    
    public FunctionTable(Schema schema, Session session, FunctionCall function) throws SQLException {
        super(schema, 0, function.getName(), false);
        this.function = function;
        function.optimize(session);
        int type = function.getType();
        if(type != Value.RESULT_SET) {
            throw Message.getSQLException(ErrorCode.FUNCTION_MUST_RETURN_RESULT_SET_1, function.getName());
        }
        int params = function.getParameterCount();
        Expression[] columnListArgs = new Expression[params];
        Expression[] args = function.getArgs();
        for(int i=0; i<params; i++) {
            args[i] = args[i].optimize(session);
            columnListArgs[i] = args[i];
        }
        ValueResultSet template = function.getValueForColumnList(session, columnListArgs);
        if(template == null) {
            throw Message.getSQLException(ErrorCode.FUNCTION_MUST_RETURN_RESULT_SET_1, function.getName());
        }
        ResultSet rs = template.getResultSet();
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        Column[] cols = new Column[columnCount];
        for(int i=0; i<columnCount; i++) {
            cols[i] = new Column(meta.getColumnName(i+1), 
                    DataType.convertSQLTypeToValueType(meta.getColumnType(i + 1)), 
                    meta.getPrecision(i + 1), 
                    meta.getScale(i + 1));
        }
        setColumns(cols);        
    }

    public void lock(Session session, boolean exclusive) throws SQLException {
    }

    public void close(Session session) throws SQLException {
    }

    public void unlock(Session s) {
    }
    
    public boolean isLockedExclusively() {
        return false;
    }

    public Index addIndex(Session session, String indexName, int indexId, Column[] cols, IndexType indexType, int headPos, String comment) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void removeRow(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void truncate(Session session) throws SQLException {
        throw Message.getUnsupportedException();
    }
    
    public boolean canDrop() {
        throw Message.getInternalError();
    }

    public void addRow(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void checkSupportAlter() throws SQLException {
        throw Message.getUnsupportedException();
    }

    public String getTableType() {
        throw Message.getInternalError();
    }

    public Index getScanIndex(Session session) throws SQLException {
        return new FunctionIndex(this, columns, function);
    }

    public ObjectArray getIndexes() {
        return null;
    }

    public boolean canGetRowCount() {
        return false;
    }

    public long getRowCount() throws SQLException {
        throw Message.getInternalError();
    }

    public String getCreateSQL() {
        return null;
    }
    
    public String getDropSQL() {
        return null;
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException();
    }
    
    public LocalResult getResult(Session session) throws SQLException {
        function.optimize(session);
        ValueResultSet value = (ValueResultSet) function.getValue(session);
        return LocalResult.read(session, value.getResultSet(), 0);
    }

    public long getMaxDataModificationId() {
        // TODO optimization: table-as-a-function currently doesn't know the last modified date
        return Long.MAX_VALUE;
    }

    public Index getUniqueIndex() {
        return null;
    }

    public String getSQL() {
        return function.getSQL();
    }    

}
