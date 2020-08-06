/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import org.h2.api.ErrorCode;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.index.SingleRowCursor;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStoreException;
import org.h2.mvstore.tx.Transaction;
import org.h2.mvstore.tx.TransactionMap;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueNull;
import org.h2.value.VersionedValue;

/**
 * A table stored in a MVStore.
 */
public class MVPrimaryIndex extends MVIndex<Long, SearchRow> {

    private final MVTable mvTable;
    private final String mapName;
    private final TransactionMap<Long, SearchRow> dataMap;
    private final AtomicLong lastKey = new AtomicLong();
    private int mainIndexColumn = SearchRow.ROWID_INDEX;

    public MVPrimaryIndex(Database db, MVTable table, int id,
            IndexColumn[] columns, IndexType indexType) {
        super(table, id, table.getName() + "_DATA", columns, indexType);
        this.mvTable = table;
        LongDBDataType keyType = new LongDBDataType();
        RowDataType valueType = table.getRowFactory().getRowDataType();
        mapName = "table." + getId();
        Transaction t = mvTable.getTransactionBegin();
        dataMap = t.openMap(mapName, keyType, valueType);
        dataMap.map.setVolatile(!table.isPersistData() || !indexType.isPersistent());
        if (!db.isStarting()) {
            dataMap.clear();
        }
        t.commit();
        Long k = dataMap.map.lastKey();    // include uncommitted keys as well
        lastKey.set(k == null ? 0 : k);
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public String getPlanSQL() {
        return table.getSQL(new StringBuilder(), TRACE_SQL_FLAGS).append(".tableScan").toString();
    }

    public void setMainIndexColumn(int mainIndexColumn) {
        this.mainIndexColumn = mainIndexColumn;
    }

    public int getMainIndexColumn() {
        return mainIndexColumn;
    }

    @Override
    public void close(SessionLocal session) {
        // ok
    }

    @Override
    public void add(SessionLocal session, Row row) {
        if (mainIndexColumn == SearchRow.ROWID_INDEX) {
            if (row.getKey() == 0) {
                row.setKey(lastKey.incrementAndGet());
            }
        } else {
            long c = row.getValue(mainIndexColumn).getLong();
            row.setKey(c);
        }

        if (mvTable.getContainsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                if (v instanceof ValueLob) {
                    ValueLob lob = ((ValueLob) v).copy(database, getId());
                    session.removeAtCommitStop(lob);
                    if (v != lob) {
                        row.setValue(i, lob);
                    }
                }
            }
        }

        TransactionMap<Long,SearchRow> map = getMap(session);
        long rowKey = row.getKey();
        try {
            Row old = (Row)map.putIfAbsent(rowKey, row);
            if (old != null) {
                int errorCode = ErrorCode.CONCURRENT_UPDATE_1;
                if (map.getImmediate(rowKey) != null || map.getFromSnapshot(rowKey) != null) {
                    // committed
                    errorCode = ErrorCode.DUPLICATE_KEY_1;
                }
                DbException e = DbException.get(errorCode,
                        getDuplicatePrimaryKeyMessage(mainIndexColumn).append(' ').append(old).toString());
                e.setSource(this);
                throw e;
            }
        } catch (MVStoreException e) {
            throw mvTable.convertException(e);
        }
        // because it's possible to directly update the key using the _rowid_
        // syntax
        long last;
        while (rowKey > (last = lastKey.get())) {
            if(lastKey.compareAndSet(last, rowKey)) break;
        }
    }

    @Override
    public void remove(SessionLocal session, Row row) {
        if (mvTable.getContainsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                if (v instanceof ValueLob) {
                    session.removeAtCommit((ValueLob) v);
                }
            }
        }
        TransactionMap<Long,SearchRow> map = getMap(session);
        try {
            Row existing = (Row)map.remove(row.getKey());
            if (existing == null) {
                StringBuilder builder = new StringBuilder();
                getSQL(builder, TRACE_SQL_FLAGS).append(": ").append(row.getKey());
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, builder.toString());
            }
        } catch (MVStoreException e) {
            throw mvTable.convertException(e);
        }
    }

    @Override
    public void update(SessionLocal session, Row oldRow, Row newRow) {
        if (mainIndexColumn != SearchRow.ROWID_INDEX) {
            long c = newRow.getValue(mainIndexColumn).getLong();
            newRow.setKey(c);
        }
        long key = oldRow.getKey();
        assert mainIndexColumn != SearchRow.ROWID_INDEX || key != 0;
        assert key == newRow.getKey() : key + " != " + newRow.getKey();
        if (mvTable.getContainsLargeObject()) {
            for (int i = 0, len = oldRow.getColumnCount(); i < len; i++) {
                Value oldValue = oldRow.getValue(i);
                Value newValue = newRow.getValue(i);
                if (oldValue != newValue) {
                    if (oldValue instanceof ValueLob) {
                        session.removeAtCommit((ValueLob) oldValue);
                    }
                    if (newValue instanceof ValueLob) {
                        ValueLob lob = ((ValueLob) newValue).copy(database, getId());
                        session.removeAtCommitStop(lob);
                        if (newValue != lob) {
                            newRow.setValue(i, lob);
                        }
                    }
                }
            }
        }

        TransactionMap<Long,SearchRow> map = getMap(session);
        try {
            Row existing = (Row)map.put(key, newRow);
            if (existing == null) {
                StringBuilder builder = new StringBuilder();
                getSQL(builder, TRACE_SQL_FLAGS).append(": ").append(key);
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, builder.toString());
            }
        } catch (MVStoreException e) {
            throw mvTable.convertException(e);
        }


        // because it's possible to directly update the key using the _rowid_
        // syntax
        if (newRow.getKey() > lastKey.get()) {
            lastKey.set(newRow.getKey());
        }
    }

    /**
     * Lock a single row.
     *
     * @param session database session
     * @param row to lock
     * @return row object if it exists
     */
    Row lockRow(SessionLocal session, Row row) {
        TransactionMap<Long,SearchRow> map = getMap(session);
        long key = row.getKey();
        return lockRow(map, key);
    }

    private Row lockRow(TransactionMap<Long,SearchRow> map, long key) {
        try {
            Row row = (Row) map.lock(key);
            ensureRowKey(row, key);
            return row;
        } catch (MVStoreException ex) {
            throw mvTable.convertException(ex);
        }
    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last) {
        long min = extractPKFromRow(first, Long.MIN_VALUE);
        long max = extractPKFromRow(last, Long.MAX_VALUE);
        return find(session, min, max);
    }

    private long extractPKFromRow(SearchRow row, long defaultValue) {
        long result;
        if (row == null) {
            result = defaultValue;
        } else if (mainIndexColumn == SearchRow.ROWID_INDEX) {
            result = row.getKey();
        } else {
            Value v = row.getValue(mainIndexColumn);
            if (v == null) {
                result = row.getKey();
            } else {
                result = v.getLong();
            }
        }
        return result;
    }


    @Override
    public MVTable getTable() {
        return mvTable;
    }

    @Override
    public Row getRow(SessionLocal session, long key) {
        TransactionMap<Long,SearchRow> map = getMap(session);
        Row row = (Row)map.getFromSnapshot(key);
        if (row == null) {
            throw DbException.get(ErrorCode.ROW_NOT_FOUND_IN_PRIMARY_INDEX, getTraceSQL(), String.valueOf(key));
        }
        ensureRowKey(row, key);
        return row;
    }

    @Override
    public double getCost(SessionLocal session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        try {
            return 10 * getCostRangeIndex(masks, dataMap.sizeAsLongMax(),
                    filters, filter, sortOrder, true, allColumnsSet);
        } catch (MVStoreException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public int getColumnIndex(Column col) {
        // can not use this index - use the delegate index instead
        return SearchRow.ROWID_INDEX;
    }

    @Override
    public boolean isFirstColumn(Column column) {
        return false;
    }

    @Override
    public void remove(SessionLocal session) {
        TransactionMap<Long,SearchRow> map = getMap(session);
        if (!map.isClosed()) {
            Transaction t = session.getTransaction();
            t.removeMap(map);
        }
    }

    @Override
    public void truncate(SessionLocal session) {
        if (mvTable.getContainsLargeObject()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
        getMap(session).clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(SessionLocal session, boolean first) {
        TransactionMap<Long,SearchRow> map = getMap(session);
        Long rowId = first ? map.firstKey() : map.lastKey();
        Row row = null;
        if (rowId != null) {
            row = (Row) map.getFromSnapshot(rowId);
            ensureRowKey(row, rowId);
        }
        return new SingleRowCursor(row);
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public long getRowCount(SessionLocal session) {
        return getMap(session).sizeAsLong();
    }

    /**
     * The maximum number of rows, including uncommitted rows of any session.
     *
     * @return the maximum number of rows
     */
    public long getRowCountMax() {
        return dataMap.sizeAsLongMax();
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return getRowCountMax();
    }

    @Override
    public long getDiskSpaceUsed() {
        return dataMap.map.getRootPage().getDiskSpaceUsed();
    }

    public String getMapName() {
        return mapName;
    }

    @Override
    public void addRowsToBuffer(List<Row> rows, String bufferName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addBufferedRows(List<String> bufferNames) {
        throw new UnsupportedOperationException();
    }

    /**
     * Search for a specific row or a set of rows.
     *
     * @param session the session
     * @param first the key of the first row
     * @param last the key of the last row
     * @return the cursor
     */
    Cursor find(SessionLocal session, Value first, Value last) {
        long from = first == null || first == ValueNull.INSTANCE ? Long.MIN_VALUE : first.getLong();
        long to = last == null || last == ValueNull.INSTANCE ? Long.MAX_VALUE : last.getLong();
        return find(session, from, to);
    }

    private Cursor find(SessionLocal session, Long first, Long last) {
        TransactionMap<Long,SearchRow> map = getMap(session);
        if (first != null && last != null && first.longValue() == last.longValue()) {
            Row row = (Row)map.getFromSnapshot(first);
            ensureRowKey(row, first);
            return new SingleRowCursor(row);
        }
        return new MVStoreCursor(map.entryIterator(first, last));
    }

    @Override
    public boolean isRowIdIndex() {
        return true;
    }

    /**
     * Get the map to store the data.
     *
     * @param session the session
     * @return the map
     */
    TransactionMap<Long,SearchRow> getMap(SessionLocal session) {
        if (session == null) {
            return dataMap;
        }
        Transaction t = session.getTransaction();
        return dataMap.getInstance(t);
    }

    @Override
    public MVMap<Long, VersionedValue<SearchRow>> getMVMap() {
        return dataMap.map;
    }

    private void ensureRowKey(Row row, long key) {
        if (mainIndexColumn != SearchRow.ROWID_INDEX && row != null && row.getKey() == 0) {
            long c = row.getValue(mainIndexColumn).getLong();
            assert c == key : c + " <> " + key;
            row.setKey(key);
        }
    }

    /**
     * A cursor.
     */
    static final class MVStoreCursor implements Cursor {

        private final Iterator<Entry<Long,SearchRow>> it;
        private Entry<Long,SearchRow> current;
        private Row row;

        public MVStoreCursor(Iterator<Entry<Long,SearchRow>> it) {
            this.it = it;
        }

        @Override
        public Row get() {
            if (row == null) {
                if (current != null) {
                    row = (Row)current.getValue();
                    if (row.getKey() == 0) {
                        row.setKey(current.getKey());
                    }
                }
            }
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return get();
        }

        @Override
        public boolean next() {
            current = it.hasNext() ? it.next() : null;
            row = null;
            return current != null;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }
    }
}
