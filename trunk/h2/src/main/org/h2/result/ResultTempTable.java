/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueUuid;

/**
 * This class implements the temp table buffer for the LocalResult class.
 */
public class ResultTempTable implements ResultExternal {
    private static final String COLUMN_NAME = "DATA";
    private Session session;
    private TableData table;
    private SortOrder sort;
    private Index index;
    private Cursor cursor;
    
    public ResultTempTable(Session session, SortOrder sort, int columnCount) throws SQLException {
        this.session = session;
        this.sort = sort;
        String tableName = "TEMP_" + ValueUuid.getNewRandom().toString();
        Schema schema = session.getDatabase().getSchema(Constants.SCHEMA_MAIN);
        Column column = new Column(COLUMN_NAME, Value.ARRAY);
        column.setNullable(false);
        ObjectArray columns = new ObjectArray();
        columns.add(column);
        int tableId = session.getDatabase().allocateObjectId(true, true);
        table = schema.createTable(tableName, tableId, columns, false, false);
        int indexId = session.getDatabase().allocateObjectId(true, true);
        IndexColumn indexColumn = new IndexColumn();
        indexColumn.column = column;
        indexColumn.columnName = COLUMN_NAME;
        IndexType indexType;
        indexType = IndexType.createPrimaryKey(true, false);
        IndexColumn[] indexCols = new IndexColumn[]{indexColumn};
        index = table.addIndex(session, tableName, indexId, indexCols, indexType, Index.EMPTY_HEAD, null);
    }
    
    public int  removeRow(Value[] values) throws SQLException {
        ValueArray data = ValueArray.get(values);
        Row row = new Row(new Value[]{data}, data.getMemory());
        table.removeRow(session, row);
        return (int) table.getRowCount(session);
    }
    
    public boolean contains(Value[] values) throws SQLException {
        ValueArray data = ValueArray.get(values);
        Row row = new Row(new Value[]{data}, data.getMemory());
        Cursor cursor = index.find(session, row, row);
        return cursor.next();
    }
    
    public int addRow(Value[] values) throws SQLException {
        ValueArray data = ValueArray.get(values);
        Row row = new Row(new Value[]{data}, data.getMemory());
        table.addRow(session, row);
        return (int) table.getRowCount(session);
    }

    public void addRows(ObjectArray rows) throws SQLException {
        if (sort != null) {
            sort.sort(rows);
        }
        for (int i = 0; i < rows.size(); i++) {
            Value[] values = (Value[]) rows.get(i);
            addRow(values);
        }        
    }

    public void close() {
        int todo;
        try {
            index.remove(session);
            table.removeChildrenAndResources(session);
        } catch (SQLException e) {
            int test;
            e.printStackTrace();
        }
    }

    public void done() throws SQLException {
    }

    public Value[] next() throws SQLException {
        if (!cursor.next()) {
            return null;
        }
        Row row = cursor.get();
        ValueArray data = (ValueArray) row.getValue(0);
        return data.getList();
    }

    public void reset() throws SQLException {
        cursor = index.find(session, null, null);
    }
}

