/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import org.h2.api.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.db.TransactionStore.Transaction;
import org.h2.mvstore.db.TransactionStore.TransactionMap;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.util.New;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;

/**
 * A table stored in a MVStore.
 */
public class MVSecondaryIndex extends BaseIndex implements MVIndex {

    /**
     * The multi-value table.
     */
    final MVTable mvTable;

    private final int keyColumns;
    private final String mapName;
    private TransactionMap<Value, Value> dataMap;

    public MVSecondaryIndex(Database db, MVTable table, int id, String indexName,
                IndexColumn[] columns, IndexType indexType) {
        this.mvTable = table;
        initBaseIndex(table, id, indexName, columns, indexType);
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }
        // always store the row key in the map key,
        // even for unique indexes, as some of the index columns could be null
        keyColumns = columns.length + 1;
        mapName = "index." + getId();
        int[] sortTypes = new int[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            sortTypes[i] = columns[i].sortType;
        }
        sortTypes[keyColumns - 1] = SortOrder.ASCENDING;
        ValueDataType keyType = new ValueDataType(
                db.getCompareMode(), db, sortTypes);
        ValueDataType valueType = new ValueDataType(null, null, null);
        Transaction t = mvTable.getTransaction(null);
        dataMap = t.openMap(mapName, keyType, valueType);
        t.commit();
        if (!keyType.equals(dataMap.getKeyType())) {
            throw DbException.throwInternalError("Incompatible key type");
        }
    }

    @Override
    public void addRowsToBuffer(List<Row> rows, String bufferName) {
        MVMap<Value, Value> map = openMap(bufferName);
        for (Row row : rows) {
            ValueArray key = convertToKey(row);
            map.put(key, ValueNull.INSTANCE);
        }
    }

    @Override
    public void addBufferedRows(List<String> bufferNames) {
        ArrayList<String> mapNames = New.arrayList(bufferNames);
        final CompareMode compareMode = database.getCompareMode();
        /**
         * A source of values.
         */
        class Source implements Comparable<Source> {
            Value value;
            Iterator<Value> next;
            int sourceId;
            @Override
            public int compareTo(Source o) {
                int comp = value.compareTo(o.value, compareMode);
                if (comp == 0) {
                    comp = sourceId - o.sourceId;
                }
                return comp;
            }
        }
        TreeSet<Source> sources = new TreeSet<Source>();
        for (int i = 0; i < bufferNames.size(); i++) {
            MVMap<Value, Value> map = openMap(bufferNames.get(i));
            Iterator<Value> it = map.keyIterator(null);
            if (it.hasNext()) {
                Source s = new Source();
                s.value = it.next();
                s.next = it;
                s.sourceId = i;
                sources.add(s);
            }
        }
        try {
            while (true) {
                Source s = sources.first();
                Value v = s.value;

                if (indexType.isUnique()) {
                    Value[] array = ((ValueArray) v).getList();
                    // don't change the original value
                    array = array.clone();
                    array[keyColumns - 1] = ValueLong.get(Long.MIN_VALUE);
                    ValueArray unique = ValueArray.get(array);
                    SearchRow row = convertToSearchRow((ValueArray) v);
                    checkUnique(row, dataMap, unique);
                }

                dataMap.putCommitted(v, ValueNull.INSTANCE);

                Iterator<Value> it = s.next;
                if (!it.hasNext()) {
                    sources.remove(s);
                    if (sources.size() == 0) {
                        break;
                    }
                } else {
                    Value nextValue = it.next();
                    sources.remove(s);
                    s.value = nextValue;
                    sources.add(s);
                }
            }
        } finally {
            for (String tempMapName : mapNames) {
                MVMap<Value, Value> map = openMap(tempMapName);
                map.getStore().removeMap(map);
            }
        }
    }

    private MVMap<Value, Value> openMap(String mapName) {
        int[] sortTypes = new int[keyColumns];
        for (int i = 0; i < indexColumns.length; i++) {
            sortTypes[i] = indexColumns[i].sortType;
        }
        sortTypes[keyColumns - 1] = SortOrder.ASCENDING;
        ValueDataType keyType = new ValueDataType(
                database.getCompareMode(), database, sortTypes);
        ValueDataType valueType = new ValueDataType(null, null, null);
        MVMap.Builder<Value, Value> builder =
                new MVMap.Builder<Value, Value>().keyType(keyType).valueType(valueType);
        MVMap<Value, Value> map = database.getMvStore().
                getStore().openMap(mapName, builder);
        if (!keyType.equals(map.getKeyType())) {
            throw DbException.throwInternalError("Incompatible key type");
        }
        return map;
    }

    @Override
    public void close(Session session) {
        // ok
    }

    @Override
    public void add(Session session, Row row) {
        TransactionMap<Value, Value> map = getMap(session);
        ValueArray array = convertToKey(row);
        ValueArray unique = null;
        if (indexType.isUnique()) {
            // this will detect committed entries only
            unique = convertToKey(row);
            unique.getList()[keyColumns - 1] = ValueLong.get(Long.MIN_VALUE);
            checkUnique(row, map, unique);
        }
        try {
            map.put(array, ValueNull.INSTANCE);
        } catch (IllegalStateException e) {
            throw mvTable.convertException(e);
        }
        if (indexType.isUnique()) {
            Iterator<Value> it = map.keyIterator(unique, true);
            while (it.hasNext()) {
                ValueArray k = (ValueArray) it.next();
                SearchRow r2 = convertToSearchRow(k);
                if (compareRows(row, r2) != 0) {
                    break;
                }
                if (containsNullAndAllowMultipleNull(r2)) {
                    // this is allowed
                    continue;
                }
                if (map.isSameTransaction(k)) {
                    continue;
                }
                if (map.get(k) != null) {
                    // committed
                    throw getDuplicateKeyException(k.toString());
                }
                throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, table.getName());
            }
        }
    }

    private void checkUnique(SearchRow row, TransactionMap<Value, Value> map, ValueArray unique) {
        Iterator<Value> it = map.keyIterator(unique, true);
        while (it.hasNext()) {
            ValueArray k = (ValueArray) it.next();
            SearchRow r2 = convertToSearchRow(k);
            if (compareRows(row, r2) != 0) {
                break;
            }
            if (map.get(k) != null) {
                if (!containsNullAndAllowMultipleNull(r2)) {
                    throw getDuplicateKeyException(k.toString());
                }
            }
        }
    }

    @Override
    public void remove(Session session, Row row) {
        ValueArray array = convertToKey(row);
        TransactionMap<Value, Value> map = getMap(session);
        try {
            Value old = map.remove(array);
            if (old == null) {
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1,
                        getSQL() + ": " + row.getKey());
            }
        } catch (IllegalStateException e) {
            throw mvTable.convertException(e);
        }
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return find(session, first, false, last);
    }

    private Cursor find(Session session, SearchRow first, boolean bigger, SearchRow last) {
        ValueArray min = convertToKey(first);
        if (min != null) {
            min.getList()[keyColumns - 1] = ValueLong.get(Long.MIN_VALUE);
        }
        TransactionMap<Value, Value> map = getMap(session);
        if (bigger && min != null) {
            // search for the next: first skip 1, then 2, 4, 8, until
            // we have a higher key; then skip 4, 2,...
            // (binary search), until 1
            int offset = 1;
            while (true) {
                ValueArray v = (ValueArray) map.relativeKey(min, offset);
                if (v != null) {
                    boolean foundHigher = false;
                    for (int i = 0; i < keyColumns - 1; i++) {
                        int idx = columnIds[i];
                        Value b = first.getValue(idx);
                        if (b == null) {
                            break;
                        }
                        Value a = v.getList()[i];
                        if (database.compare(a, b) > 0) {
                            foundHigher = true;
                            break;
                        }
                    }
                    if (!foundHigher) {
                        offset += offset;
                        min = v;
                        continue;
                    }
                }
                if (offset > 1) {
                    offset /= 2;
                    continue;
                }
                if (map.get(v) == null) {
                    min = (ValueArray) map.higherKey(min);
                    if (min == null) {
                        break;
                    }
                    continue;
                }
                min = v;
                break;
            }
            if (min == null) {
                return new MVStoreCursor(session,
                        Collections.<Value>emptyList().iterator(), null);
            }
        }
        return new MVStoreCursor(session, map.keyIterator(min), last);
    }

    private ValueArray convertToKey(SearchRow r) {
        if (r == null) {
            return null;
        }
        Value[] array = new Value[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            Column c = columns[i];
            int idx = c.getColumnId();
            Value v = r.getValue(idx);
            if (v != null) {
                array[i] = v.convertTo(c.getType());
            }
        }
        array[keyColumns - 1] = ValueLong.get(r.getKey());
        return ValueArray.get(array);
    }

    /**
     * Convert array of values to a SearchRow.
     *
     * @param key the index key
     * @return the row
     */
    SearchRow convertToSearchRow(ValueArray key) {
        Value[] array = key.getList();
        SearchRow searchRow = mvTable.getTemplateRow();
        searchRow.setKey((array[array.length - 1]).getLong());
        Column[] cols = getColumns();
        for (int i = 0; i < array.length - 1; i++) {
            Column c = cols[i];
            int idx = c.getColumnId();
            Value v = array[i];
            searchRow.setValue(idx, v);
        }
        return searchRow;
    }

    @Override
    public MVTable getTable() {
        return mvTable;
    }

    @Override
    public double getCost(Session session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            HashSet<Column> allColumnsSet) {
        try {
            return 10 * getCostRangeIndex(masks, dataMap.sizeAsLongMax(),
                    filters, filter, sortOrder, false, allColumnsSet);
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public void remove(Session session) {
        TransactionMap<Value, Value> map = getMap(session);
        if (!map.isClosed()) {
            Transaction t = mvTable.getTransaction(session);
            t.removeMap(map);
        }
    }

    @Override
    public void truncate(Session session) {
        TransactionMap<Value, Value> map = getMap(session);
        map.clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        TransactionMap<Value, Value> map = getMap(session);
        Value key = first ? map.firstKey() : map.lastKey();
        while (true) {
            if (key == null) {
                return new MVStoreCursor(session,
                        Collections.<Value>emptyList().iterator(), null);
            }
            if (((ValueArray) key).getList()[0] != ValueNull.INSTANCE) {
                break;
            }
            key = first ? map.higherKey(key) : map.lowerKey(key);
        }
        ArrayList<Value> list = New.arrayList();
        list.add(key);
        MVStoreCursor cursor = new MVStoreCursor(session, list.iterator(), null);
        cursor.next();
        return cursor;
    }

    @Override
    public boolean needRebuild() {
        try {
            return dataMap.sizeAsLongMax() == 0;
        } catch (IllegalStateException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    @Override
    public long getRowCount(Session session) {
        TransactionMap<Value, Value> map = getMap(session);
        return map.sizeAsLong();
    }

    @Override
    public long getRowCountApproximation() {
        try {
            return dataMap.sizeAsLongMax();
        } catch (IllegalStateException e) {
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

    @Override
    public void checkRename() {
        // ok
    }

    /**
     * Get the map to store the data.
     *
     * @param session the session
     * @return the map
     */
    TransactionMap<Value, Value> getMap(Session session) {
        if (session == null) {
            return dataMap;
        }
        Transaction t = mvTable.getTransaction(session);
        return dataMap.getInstance(t, Long.MAX_VALUE);
    }

    /**
     * A cursor.
     */
    class MVStoreCursor implements Cursor {

        private final Session session;
        private final Iterator<Value> it;
        private final SearchRow last;
        private Value current;
        private SearchRow searchRow;
        private Row row;

        public MVStoreCursor(Session session, Iterator<Value> it, SearchRow last) {
            this.session = session;
            this.it = it;
            this.last = last;
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
            if (searchRow == null) {
                if (current != null) {
                    searchRow = convertToSearchRow((ValueArray) current);
                }
            }
            return searchRow;
        }

        @Override
        public boolean next() {
            current = it.hasNext() ? it.next() : null;
            searchRow = null;
            if (current != null) {
                if (last != null && compareRows(getSearchRow(), last) > 0) {
                    searchRow = null;
                    current = null;
                }
            }
            row = null;
            return current != null;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }

    }

}
