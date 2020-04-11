/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import org.h2.api.ErrorCode;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.index.SingleRowCursor;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.MVStoreException;
import org.h2.mvstore.tx.Transaction;
import org.h2.mvstore.tx.TransactionMap;
import org.h2.mvstore.type.DataType;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.VersionedValue;

/**
 * An index stored in a MVStore.
 */
public final class MVSecondaryIndex extends BaseIndex implements MVIndex<SearchRow,Value> {

    /**
     * The multi-value table.
     */
    private final MVTable                         mvTable;
    private final TransactionMap<SearchRow,Value> dataMap;

    public MVSecondaryIndex(Database db, MVTable table, int id, String indexName,
                IndexColumn[] columns, IndexType indexType) {
        super(table, id, indexName, columns, indexType);
        this.mvTable = table;
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }
        String mapName = "index." + getId();
        assert db.isStarting() || !db.getStore().getMvStore().getMetaMap().containsKey(DataUtils.META_NAME + mapName);
        RowDataType keyType = getRowFactory().getRowDataType();
        ValueDataType valueType = new ValueDataType();
        Transaction t = mvTable.getTransactionBegin();
        dataMap = t.openMap(mapName, keyType, valueType);
        dataMap.map.setVolatile(!table.isPersistData() || !indexType.isPersistent());
        t.commit();
        if (!keyType.equals(dataMap.getKeyType())) {
            throw DbException.throwInternalError(
                    "Incompatible key type, expected " + keyType + " but got "
                            + dataMap.getKeyType() + " for index " + indexName);
        }
    }

    @Override
    public void addRowsToBuffer(List<Row> rows, String bufferName) {
        MVMap<SearchRow,Value> map = openMap(bufferName);
        for (Row row : rows) {
            SearchRow r = getRowFactory().createRow();
            r.copyFrom(row);
            map.append(r, ValueNull.INSTANCE);
        }
    }

    private static final class Source {

        private final Iterator<SearchRow> iterator;

        SearchRow currentRowData;

        public Source(Iterator<SearchRow> iterator) {
            assert iterator.hasNext();
            this.iterator = iterator;
            this.currentRowData = iterator.next();
        }

        public boolean hasNext() {
            boolean result = iterator.hasNext();
            if(result) {
                currentRowData = iterator.next();
            }
            return result;
        }

        public SearchRow next() {
            return currentRowData;
        }

        static final class Comparator implements java.util.Comparator<Source> {

            private final DataType<SearchRow> type;

            public Comparator(DataType<SearchRow> type) {
                this.type = type;
            }

            @Override
            public int compare(Source one, Source two) {
                return type.compare(one.currentRowData, two.currentRowData);
            }
        }
    }

    @Override
    public void addBufferedRows(List<String> bufferNames) {
        int buffersCount = bufferNames.size();
        Queue<Source> queue = new PriorityQueue<>(buffersCount,
                                new Source.Comparator(getRowFactory().getRowDataType()));
        for (String bufferName : bufferNames) {
            Iterator<SearchRow> iter = openMap(bufferName).keyIterator(null);
            if (iter.hasNext()) {
                queue.offer(new Source(iter));
            }
        }

        try {
            while (!queue.isEmpty()) {
                Source s = queue.poll();
                SearchRow row = s.next();

                if (indexType.isUnique() && !mayHaveNullDuplicates(row)) {
                    checkUnique(true, dataMap, row, Long.MIN_VALUE);
                }

                dataMap.putCommitted(row, ValueNull.INSTANCE);

                if (s.hasNext()) {
                    queue.offer(s);
                }
            }
        } finally {
            MVStore mvStore = database.getStore().getMvStore();
            for (String tempMapName : bufferNames) {
                mvStore.removeMap(tempMapName);
            }
        }
    }

    private MVMap<SearchRow,Value> openMap(String mapName) {
        RowDataType keyType = getRowFactory().getRowDataType();
        ValueDataType valueType = new ValueDataType();
        MVMap.Builder<SearchRow,Value> builder = new MVMap.Builder<SearchRow,Value>()
                                                .singleWriter()
                                                .keyType(keyType)
                                                .valueType(valueType);
        MVMap<SearchRow, Value> map = database.getStore().getMvStore()
                .openMap(mapName, builder);
        if (!keyType.equals(map.getKeyType())) {
            throw DbException.throwInternalError(
                    "Incompatible key type, expected " + keyType + " but got "
                            + map.getKeyType() + " for map " + mapName);
        }
        return map;
    }

    @Override
    public void close(Session session) {
        // ok
    }

    @Override
    public void add(Session session, Row row) {
        TransactionMap<SearchRow,Value> map = getMap(session);
        SearchRow key = convertToKey(row, null);
        boolean checkRequired, allowNonRepeatableRead;
        if (indexType.isUnique() && !mayHaveNullDuplicates(row)) {
            checkRequired = true;
            allowNonRepeatableRead = session.getTransaction().allowNonRepeatableRead();
        } else {
            checkRequired = false;
            allowNonRepeatableRead = false;
        }
        if (checkRequired) {
            checkUnique(allowNonRepeatableRead, map, row, Long.MIN_VALUE);
        }

        try {
            map.put(key, ValueNull.INSTANCE);
        } catch (MVStoreException e) {
            throw mvTable.convertException(e);
        }

        if (checkRequired) {
            checkUnique(allowNonRepeatableRead, map, row, row.getKey());
        }
    }

    private void checkUnique(boolean allowNonRepeatableRead, TransactionMap<SearchRow,Value> map, SearchRow row,
            long newKey) {
        SearchRow from = convertToKey(row, Boolean.FALSE);
        SearchRow to = convertToKey(row, Boolean.TRUE);
        if (!allowNonRepeatableRead) {
            Iterator<SearchRow> it = map.keyIterator(from, to);
            while (it.hasNext()) {
                SearchRow k = it.next();
                if (newKey != k.getKey() && !map.isDeletedByCurrentTransaction(k)) {
                    throw getDuplicateKeyException(k.toString());
                }
            }
        }
        Iterator<SearchRow> it = map.keyIteratorUncommitted(from, to);
        while (it.hasNext()) {
            SearchRow k = it.next();
            if (newKey != k.getKey()) {
                if (map.getImmediate(k) != null) {
                    // committed
                    throw getDuplicateKeyException(k.toString());
                }
                throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, table.getName());
            }
        }
    }

    @Override
    public void remove(Session session, Row row) {
        SearchRow searchRow = convertToKey(row, null);
        TransactionMap<SearchRow,Value> map = getMap(session);
        try {
            if (map.remove(searchRow) == null) {
                StringBuilder builder = new StringBuilder();
                getSQL(builder, TRACE_SQL_FLAGS).append(": ").append(row.getKey());
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, builder.toString());
            }
        } catch (MVStoreException e) {
            throw mvTable.convertException(e);
        }
    }

    @Override
    public void update(Session session, Row oldRow, Row newRow) {
        SearchRow searchRowOld = convertToKey(oldRow, null);
        SearchRow searchRowNew = convertToKey(newRow, null);
        if (!rowsAreEqual(searchRowOld, searchRowNew)) {
            super.update(session, oldRow, newRow);
        }
    }

    private boolean rowsAreEqual(SearchRow rowOne, SearchRow rowTwo) {
        if (rowOne == rowTwo) {
            return true;
        }
        for (int index : columnIds) {
            Value v1 = rowOne.getValue(index);
            Value v2 = rowTwo.getValue(index);
            if (!Objects.equals(v1, v2)) {
                return false;
            }
        }
        return rowOne.getKey() == rowTwo.getKey();
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return find(session, first, false, last);
    }

    private Cursor find(Session session, SearchRow first, boolean bigger, SearchRow last) {
        SearchRow min = convertToKey(first, bigger);
        TransactionMap<SearchRow,Value> map = getMap(session);
        SearchRow max = convertToKey(last, Boolean.TRUE);
        return new MVStoreCursor(session, map.keyIterator(min, max), mvTable);
    }

    private SearchRow convertToKey(SearchRow r, Boolean minMax) {
        if (r == null) {
            return null;
        }

        SearchRow row = getRowFactory().createRow();
        row.copyFrom(r);
        if (minMax != null) {
            row.setKey(minMax ? Long.MAX_VALUE : Long.MIN_VALUE);
        }
        return row;
    }

    @Override
    public MVTable getTable() {
        return mvTable;
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        try {
            return 10 * getCostRangeIndex(masks, dataMap.sizeAsLongMax(),
                    filters, filter, sortOrder, false, allColumnsSet);
        } catch (MVStoreException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public void remove(Session session) {
        TransactionMap<SearchRow,Value> map = getMap(session);
        if (!map.isClosed()) {
            Transaction t = session.getTransaction();
            t.removeMap(map);
        }
    }

    @Override
    public void truncate(Session session) {
        TransactionMap<SearchRow,Value> map = getMap(session);
        map.clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        TransactionMap<SearchRow,Value> map = getMap(session);
        SearchRow key = first ? map.firstKey() : map.lastKey();
        while (true) {
            if (key == null) {
                return new SingleRowCursor(null);
            }
            if (key.getValue(columnIds[0]) != ValueNull.INSTANCE) {
                return new SingleRowCursor(mvTable.getRow(session, key.getKey()));
            }
            key = first ? map.higherKey(key) : map.lowerKey(key);
        }
    }

    @Override
    public boolean needRebuild() {
        try {
            return dataMap.sizeAsLongMax() == 0;
        } catch (MVStoreException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getRowCount(Session session) {
        TransactionMap<SearchRow,Value> map = getMap(session);
        return map.sizeAsLong();
    }

    @Override
    public long getRowCountApproximation() {
        try {
            return dataMap.sizeAsLongMax();
        } catch (MVStoreException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getDiskSpaceUsed() {
        // TODO estimate disk space usage
        return 0;
    }

    @Override
    public boolean canFindNext() {
        return true;
    }

    @Override
    public Cursor findNext(Session session, SearchRow higherThan, SearchRow last) {
        return find(session, higherThan, true, last);
    }

    /**
     * Get the map to store the data.
     *
     * @param session the session
     * @return the map
     */
    private TransactionMap<SearchRow,Value> getMap(Session session) {
        if (session == null) {
            return dataMap;
        }
        Transaction t = session.getTransaction();
        return dataMap.getInstance(t);
    }

    @Override
    public MVMap<SearchRow,VersionedValue<Value>> getMVMap() {
        return dataMap.map;
    }

    /**
     * A cursor.
     */
    static final class MVStoreCursor implements Cursor {

        private final Session             session;
        private final Iterator<SearchRow> it;
        private final MVTable             mvTable;
        private       SearchRow           current;
        private       Row                 row;

        MVStoreCursor(Session session, Iterator<SearchRow> it, MVTable mvTable) {
            this.session = session;
            this.it = it;
            this.mvTable = mvTable;
        }

        @Override
        public Row get() {
            if (row == null) {
                SearchRow r = getSearchRow();
                if (r != null) {
                    row = mvTable.getRow(session, r.getKey());
                }
            }
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            return current;
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
