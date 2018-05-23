/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.lang.ref.Reference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;

import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.util.TempFileDeleter;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This class implements the temp table buffer for the LocalResult class.
 */
public class ResultTempTable implements ResultExternal {

    private static final class CloseImpl implements AutoCloseable {
        private final Session session;
        private final Table table;
        Index index;

        CloseImpl(Session session, Table table) {
            this.session = session;
            this.table = table;
        }

        @Override
        public void close() throws Exception {
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
                table.removeChildrenAndResources(sysSession);
                if (index != null) {
                    // need to explicitly do this,
                    // as it's not registered in the system session
                    session.removeLocalTempTableIndex(index);
                }
                // the transaction must be committed immediately
                // TODO this synchronization cascade is very ugly
                synchronized (session) {
                    synchronized (sysSession) {
                        synchronized (database) {
                            sysSession.commit(false);
                        }
                    }
                }
            }
        }
    }

    private static final String COLUMN_NAME = "DATA";
    private final boolean distinct;
    private final SortOrder sort;
    private Index index;
    private final Session session;
    private Table table;
    private Cursor resultCursor;
    private int rowCount;
    private final int columnCount;

    private final ResultTempTable parent;
    private boolean closed;
    private int childCount;

    /**
     * Temporary file deleter.
     */
    private final TempFileDeleter tempFileDeleter;

    /**
     * Closeable to close the storage.
     */
    private final CloseImpl closeable;

    /**
     * Reference to the record in the temporary file deleter.
     */
    private final Reference<?> fileRef;

    ResultTempTable(Session session, Expression[] expressions, boolean distinct, SortOrder sort) {
        this.session = session;
        this.distinct = distinct;
        this.sort = sort;
        this.columnCount = expressions.length;
        Schema schema = session.getDatabase().getSchema(Constants.SCHEMA_MAIN);
        CreateTableData data = new CreateTableData();
        boolean containsLob = false;
        for (int i = 0; i < expressions.length; i++) {
            int type = expressions[i].getType();
            Column col = new Column(COLUMN_NAME + i, type);
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
        parent = null;
        if (containsLob) {
            // contains BLOB or CLOB: can not truncate now,
            // otherwise the BLOB and CLOB entries are removed
            tempFileDeleter = null;
            closeable = null;
            fileRef = null;
        } else {
            tempFileDeleter = session.getDatabase().getTempFileDeleter();
            closeable = new CloseImpl(session, table);
            fileRef = tempFileDeleter.addFile(closeable, this);
        }
        /*
         * If ORDER BY or DISTINCT is specified create the index immediately. If
         * they are not specified index still may be created later if required
         * for IN (SELECT ...) etc.
         */
        if (sort != null || distinct) {
            getIndex();
        }
    }

    private ResultTempTable(ResultTempTable parent) {
        this.parent = parent;
        this.columnCount = parent.columnCount;
        this.distinct = parent.distinct;
        this.session = parent.session;
        this.table = parent.table;
        this.rowCount = parent.rowCount;
        this.sort = parent.sort;
        this.tempFileDeleter = null;
        this.closeable = null;
        this.fileRef = null;
    }

    private Index getIndex() {
        if (parent != null) {
            return parent.getIndex();
        }
        if (index != null) {
            return index;
        }
        IndexColumn[] indexCols;
        if (sort != null) {
            int[] colIndex = sort.getQueryColumnIndexes();
            int len = colIndex.length;
            if (distinct) {
                BitSet used = new BitSet();
                indexCols = new IndexColumn[columnCount];
                for (int i = 0; i < len; i++) {
                    int idx = colIndex[i];
                    used.set(idx);
                    IndexColumn indexColumn = createIndexColumn(idx);
                    indexColumn.sortType = sort.getSortTypes()[i];
                    indexCols[i] = indexColumn;
                }
                int idx = 0;
                for (int i = len; i < columnCount; i++) {
                    idx = used.nextClearBit(idx);
                    indexCols[i] = createIndexColumn(idx);
                    idx++;
                }
            } else {
                indexCols = new IndexColumn[len];
                for (int i = 0; i < len; i++) {
                    IndexColumn indexColumn = createIndexColumn(colIndex[i]);
                    indexColumn.sortType = sort.getSortTypes()[i];
                    indexCols[i] = indexColumn;
                }
            }
        } else {
            indexCols = new IndexColumn[columnCount];
            for (int i = 0; i < columnCount; i++) {
                indexCols[i] = createIndexColumn(i);
            }
        }
        String indexName = table.getSchema().getUniqueIndexName(session, table, Constants.PREFIX_INDEX);
        int indexId = session.getDatabase().allocateObjectId();
        IndexType indexType = IndexType.createNonUnique(true);
        index = table.addIndex(session, indexName, indexId, indexCols, indexType, true, null);
        if (closeable != null) {
            closeable.index = index;
        }
        return index;
    }

    private IndexColumn createIndexColumn(int index) {
        IndexColumn indexColumn = new IndexColumn();
        indexColumn.column = table.getColumn(index);
        indexColumn.columnName = COLUMN_NAME + index;
        return indexColumn;
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
            delete();
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
                delete();
            }
        }
    }

    private void delete() {
        if (tempFileDeleter != null) {
            tempFileDeleter.deleteFile(fileRef, closeable);
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
                idx = getIndex();
            } else {
                idx = table.getScanIndex(session);
            }
            resultCursor = idx.find(session, null, null);
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
        return session.createRow(values, Row.MEMORY_CALCULATE);
    }

    private Cursor find(Row row) {
        Index index = getIndex();
        Cursor cursor = index.find(session, row, row);
        while (cursor.next()) {
            SearchRow found = cursor.getSearchRow();
            boolean ok = true;
            Database db = session.getDatabase();
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
