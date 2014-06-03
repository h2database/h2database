/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.util.ArrayList;
import java.util.Arrays;

import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
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
import org.h2.value.ValueNull;

/**
 * This class implements the temp table buffer for the LocalResult class.
 */
public class ResultTempTable implements ResultExternal {
    
    private static final String COLUMN_NAME = "DATA";
    private final boolean distinct;
    private final SortOrder sort;
    private Index index;
    private Session session;
    private Table table;
    private Cursor resultCursor;
    private int rowCount;
    private int columnCount;

    private final ResultTempTable parent;
    private boolean closed;
    private int childCount;
    private boolean containsLob;

    ResultTempTable(Session session, Expression[] expressions, boolean distinct, SortOrder sort) {
        this.session = session;
        this.distinct = distinct;
        this.sort = sort;
        this.columnCount = expressions.length;
        Schema schema = session.getDatabase().getSchema(Constants.SCHEMA_MAIN);
        CreateTableData data = new CreateTableData();
        for (int i = 0; i < expressions.length; i++) {
            int type = expressions[i].getType();
            Column col = new Column(COLUMN_NAME + i,
                    type);
            if (type == Value.CLOB || type == Value.BLOB) {
                containsLob = true;
            }
            data.columns.add(col);
        }
        data.id = session.getDatabase().allocateObjectId();
        data.tableName = "TEMP_RESULT_SET_" + data.id;
        data.temporary = true;
        data.persistIndexes = false;
        data.persistData = true;
        data.create = true;
        data.session = session;
        table = schema.createTable(data);
        if (sort != null || distinct) {
            createIndex();
        }
        parent = null;
    }
    
    private ResultTempTable(ResultTempTable parent) {
        this.parent = parent;
        this.columnCount = parent.columnCount;
        this.distinct = parent.distinct;
        this.session = parent.session;
        this.table = parent.table;
        this.index = parent.index;
        this.rowCount = parent.rowCount;
        this.sort = parent.sort;
        this.containsLob = parent.containsLob;
        reset();
    }
    
    private void createIndex() {
        IndexColumn[] indexCols = null;
        if (sort != null) {
            int[] colInd = sort.getQueryColumnIndexes();
            indexCols = new IndexColumn[colInd.length];
            for (int i = 0; i < colInd.length; i++) {
                IndexColumn indexColumn = new IndexColumn();
                indexColumn.column = table.getColumn(colInd[i]);
                indexColumn.sortType = sort.getSortTypes()[i];
                indexColumn.columnName = COLUMN_NAME + i;
                indexCols[i] = indexColumn;
            }
        } else {
            indexCols = new IndexColumn[columnCount];
            for (int i = 0; i < columnCount; i++) {
                IndexColumn indexColumn = new IndexColumn();
                indexColumn.column = table.getColumn(i);
                indexColumn.columnName = COLUMN_NAME + i;
                indexCols[i] = indexColumn;
            }
        }
        String indexName = table.getSchema().getUniqueIndexName(session,
                table, Constants.PREFIX_INDEX);
        int indexId = session.getDatabase().allocateObjectId();
        IndexType indexType = IndexType.createNonUnique(true);
        if (session.getDatabase().getMvStore() != null) {
            index = table.addIndex(session, indexName, indexId, indexCols,
                    indexType, true, null);
            index.setTemporary(true);
        } else {
            index = new PageBtreeIndex((RegularTable) table, indexId,
                    indexName, indexCols, indexType, true, session);
            index.setTemporary(true);
            table.getIndexes().add(index);
        }
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
        if (distinct) {
            Cursor cursor = find(row);
            if (cursor == null) {
                table.addRow(session, row);
                rowCount++;
            }
        } else {
            table.addRow(session, row);
            rowCount++;
        }
        return rowCount;
    }

    @Override
    public int addRows(ArrayList<Value[]> rows) {
        // speeds up inserting, but not really needed:
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
        if (containsLob) {
            // contains BLOB or CLOB: can not truncate now,
            // otherwise the BLOB and CLOB entries are removed
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
            // This session may not lock the sys table (except if it already has
            // locked it) because it must be committed immediately, otherwise
            // other threads can not access the sys table. If the table is not
            // removed now, it will be when the database is opened the next
            // time. (the table is truncated, so this is just one record)
            if (!database.isSysTableLocked()) {
                Session sysSession = database.getSystemSession();
                if (index != null) {
                    index.removeChildrenAndResources(sysSession);
                }
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
            Index idx;
            if (distinct || sort != null) {
                idx = index;
            } else {
                idx = table.getScanIndex(session);
            }
            if (session.getDatabase().getMvStore() != null) {
                // sometimes the transaction is already committed,
                // in which case we can't use the session
                if (idx.getRowCount(session) == 0 && rowCount > 0) {
                    // this means querying is not transactional
                    resultCursor = idx.find((Session) null, null, null);
                } else {
                    // the transaction is still open
                    resultCursor = idx.find(session, null, null);
                }
            } else {
                resultCursor = idx.find(session, null, null);
            }
        }
        if (!resultCursor.next()) {
            return null;
        }
        Row row = resultCursor.get();
        return row.getValueList();
    }

    @Override
    public void reset() {
        resultCursor = null;
    }

    private Row convertToRow(Value[] values) {
        if (values.length < columnCount) {
            Value[] v2 = Arrays.copyOf(values, columnCount);
            for (int i = values.length; i < columnCount; i++) {
                v2[i] = ValueNull.INSTANCE;
            }
            values = v2;
        }
        return new Row(values, Row.MEMORY_CALCULATE);
    }

    private Cursor find(Row row) {
        if (index == null) {
            // for the case "in(select ...)", the query might
            // use an optimization and not create the index
            // up front
            createIndex();
        }
        Cursor cursor = index.find(session, row, row);
        Database db = session.getDatabase();
        while (cursor.next()) {
            SearchRow found = cursor.getSearchRow();
            boolean ok = true;
            for (int i = 0; i < row.getColumnCount(); i++) {
                if (!db.areEqual(row.getValue(i), found.getValue(i))) {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                return cursor;
            }
        }
        return null;
    }

}

