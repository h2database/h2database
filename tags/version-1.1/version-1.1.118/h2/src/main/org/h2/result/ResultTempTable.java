/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.SQLException;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.index.BtreeIndex;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.PageBtreeIndex;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;
import org.h2.value.Value;
import org.h2.value.ValueArray;

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

    public ResultTempTable(Session session, SortOrder sort) throws SQLException {
        this.session = session;
        this.sort = sort;
        Schema schema = session.getDatabase().getSchema(Constants.SCHEMA_MAIN);
        Column column = new Column(COLUMN_NAME, Value.ARRAY);
        column.setNullable(false);
        ObjectArray<Column> columns = ObjectArray.newInstance();
        columns.add(column);
        int tableId = session.getDatabase().allocateObjectId(true, true);
        String tableName = "TEMP_RESULT_SET_" + tableId;
        table = schema.createTable(tableName, tableId, columns, true, false, true, false, Index.EMPTY_HEAD, session);
        session.addLocalTempTable(table);
        int indexId = session.getDatabase().allocateObjectId(true, false);
        IndexColumn indexColumn = new IndexColumn();
        indexColumn.column = column;
        indexColumn.columnName = COLUMN_NAME;
        IndexType indexType;
        indexType = IndexType.createPrimaryKey(true, false);
        IndexColumn[] indexCols = new IndexColumn[]{indexColumn};
        if (session.getDatabase().isPageStoreEnabled()) {
            index = new PageBtreeIndex(table, indexId, tableName, indexCols, indexType, Index.EMPTY_HEAD, session);
        } else {
            index = new BtreeIndex(session, table, indexId, tableName, indexCols, indexType, Index.EMPTY_HEAD);
        }
        index.setTemporary(true);
        session.addLocalTempTableIndex(index);
        table.getIndexes().add(index);
    }

    public int removeRow(Value[] values) throws SQLException {
        Row row = convertToRow(values);
        Cursor cursor = find(row);
        if (cursor != null) {
            row = cursor.get();
            table.removeRow(session, row);
        }
        return (int) table.getRowCount(session);
    }

    public boolean contains(Value[] values) throws SQLException {
        return find(convertToRow(values)) != null;
    }

    public int addRow(Value[] values) throws SQLException {
        Row row = convertToRow(values);
        Cursor cursor = find(row);
        if (cursor == null) {
            table.addRow(session, row);
        }
        return (int) table.getRowCount(session);
    }

    public void addRows(ObjectArray<Value[]> rows) throws SQLException {
        if (sort != null) {
            sort.sort(rows);
        }
        for (Value[] values : rows) {
            addRow(values);
        }
    }

    public void close() {
        try {
            if (table != null) {
                session.removeLocalTempTable(table);
            }
        } catch (SQLException e) {
            throw Message.convertToInternal(e);
        } finally {
            table = null;
        }
    }

    public void done() {
        // nothing to do
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

    private Row convertToRow(Value[] values) {
        ValueArray data = ValueArray.get(values);
        return new Row(new Value[]{data}, data.getMemory());
    }

    private Cursor find(Row row) throws SQLException {
        Cursor cursor = index.find(session, row, row);
        while (cursor.next()) {
            SearchRow found;
            found = cursor.getSearchRow();
            if (found.getValue(0).equals(row.getValue(0))) {
                return cursor;
            }
        }
        return null;
    }

}

