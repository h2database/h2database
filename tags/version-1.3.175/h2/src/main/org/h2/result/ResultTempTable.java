/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.util.ArrayList;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.PageBtreeIndex;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.table.Table;
import org.h2.value.Value;
import org.h2.value.ValueArray;

/**
 * This class implements the temp table buffer for the LocalResult class.
 */
public class ResultTempTable implements ResultExternal {

    private static final String COLUMN_NAME = "DATA";
    private final SortOrder sort;
    private final Index index;
    private Session session;
    private Table table;
    private Cursor resultCursor;
    private int rowCount;

    private final ResultTempTable parent;
    private boolean closed;
    private int childCount;

    ResultTempTable(Session session, SortOrder sort) {
        this.session = session;
        this.sort = sort;
        Schema schema = session.getDatabase().getSchema(Constants.SCHEMA_MAIN);
        Column column = new Column(COLUMN_NAME, Value.ARRAY);
        column.setNullable(false);
        CreateTableData data = new CreateTableData();
        data.columns.add(column);
        data.id = session.getDatabase().allocateObjectId();
        data.tableName = "TEMP_RESULT_SET_" + data.id;
        data.temporary = true;
        data.persistIndexes = false;
        data.persistData = true;
        data.create = true;
        data.session = session;
        table = schema.createTable(data);
        int indexId = session.getDatabase().allocateObjectId();
        IndexColumn indexColumn = new IndexColumn();
        indexColumn.column = column;
        indexColumn.columnName = COLUMN_NAME;
        IndexType indexType;
        indexType = IndexType.createPrimaryKey(true, false);
        IndexColumn[] indexCols = { indexColumn };
        if (session.getDatabase().getMvStore() != null) {
            index = table.addIndex(session, data.tableName, indexId, indexCols, indexType, true, null);
            index.setTemporary(true);
        } else {
            index = new PageBtreeIndex((RegularTable) table, indexId, data.tableName, indexCols, indexType, true, session);
            index.setTemporary(true);
            table.getIndexes().add(index);
        }
        parent = null;
    }

    private ResultTempTable(ResultTempTable parent) {
        this.parent = parent;
        this.session = parent.session;
        this.table = parent.table;
        this.index = parent.index;
        this.rowCount = parent.rowCount;
        // sort is only used when adding rows
        this.sort = null;
        reset();
    }

    @Override
    public synchronized ResultExternal createShallowCopy() {
        if (parent != null) {
            return parent.createShallowCopy();
        }
        if (closed) {
            return null;
        }
        childCount++;
        return new ResultTempTable(this);
    }

    @Override
    public int removeRow(Value[] values) {
        Row row = convertToRow(values);
        Cursor cursor = find(row);
        if (cursor != null) {
            row = cursor.get();
            table.removeRow(session, row);
            rowCount--;
        }
        return rowCount;
    }

    @Override
    public boolean contains(Value[] values) {
        return find(convertToRow(values)) != null;
    }

    @Override
    public int addRow(Value[] values) {
        Row row = convertToRow(values);
        Cursor cursor = find(row);
        if (cursor == null) {
            table.addRow(session, row);
            rowCount++;
        }
        return rowCount;
    }

    @Override
    public int addRows(ArrayList<Value[]> rows) {
        if (sort != null) {
            sort.sort(rows);
        }
        for (Value[] values : rows) {
            addRow(values);
        }
        return rowCount;
    }

    private synchronized void closeChild() {
        if (--childCount == 0 && closed) {
            dropTable();
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (parent != null) {
            parent.closeChild();
        } else {
            if (childCount == 0) {
                dropTable();
            }
        }
    }

    private void dropTable() {
        if (table == null) {
            return;
        }
        try {
            Database database = session.getDatabase();
            // Need to lock because not all of the code-paths
            // that reach here have already taken this lock,
            // notably via the close() paths.
            synchronized (session) {
                synchronized (database) {
                    table.truncate(session);
                }
            }
            // This session may not lock the sys table (except if it already has locked it)
            // because it must be committed immediately,
            // otherwise other threads can not access the sys table.
            // If the table is not removed now, it will be when the database
            // is opened the next time.
            // (the table is truncated, so this is just one record)
            if (!database.isSysTableLocked()) {
                Session sysSession = database.getSystemSession();
                index.removeChildrenAndResources(sysSession);
                table.removeChildrenAndResources(sysSession);
                // the transaction must be committed immediately
                sysSession.commit(false);
            }
        } finally {
            table = null;
        }
    }

    @Override
    public void done() {
        // nothing to do
    }

    @Override
    public Value[] next() {
        if (resultCursor == null) {
            if (session.getDatabase().getMvStore() != null) {
                // sometimes the transaction is already committed,
                // in which case we can't use the session
                if (index.getRowCount(session) == 0 && rowCount > 0) {
                    // this means querying is not transactional
                    resultCursor = index.find((Session) null, null, null);
                } else {
                    // the transaction is still open
                    resultCursor = index.find(session, null, null);
                }
            } else {
                resultCursor = index.find(session, null, null);
            }
        }
        if (!resultCursor.next()) {
            return null;
        }
        Row row = resultCursor.get();
        ValueArray data = (ValueArray) row.getValue(0);
        return data.getList();
    }

    @Override
    public void reset() {
        resultCursor = null;
    }

    private static Row convertToRow(Value[] values) {
        ValueArray data = ValueArray.get(values);
        return new Row(new Value[]{data}, Row.MEMORY_CALCULATE);
    }

    private Cursor find(Row row) {
        Cursor cursor = index.find(session, row, row);
        Value a = row.getValue(0);
        while (cursor.next()) {
            SearchRow found;
            found = cursor.getSearchRow();
            Value b = found.getValue(0);
            if (session.getDatabase().areEqual(a, b)) {
                return cursor;
            }
        }
        return null;
    }

}

