/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
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
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;

/**
 * A table stored in a MVStore.
 */
public class MVPrimaryIndex extends BaseIndex {

    private final MVTable mvTable;
    private String mapName;
    private MVMap.Builder<Value, Value> mapBuilder;
    private long lastKey;
    private int mainIndexColumn = -1;

    public MVPrimaryIndex(Database db, MVTable table, int id, IndexColumn[] columns,
                IndexType indexType) {
        this.mvTable = table;
        initBaseIndex(table, id, table.getName() + "_DATA", columns, indexType);
        int[] sortTypes = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            sortTypes[i] = SortOrder.ASCENDING;
        }
        ValueDataType keyType = new ValueDataType(
                null, null, null);
        ValueDataType valueType = new ValueDataType(
                db.getCompareMode(), db, sortTypes);
        mapName = getName() + "_" + getId();
        mapBuilder = new MVMap.Builder<Value, Value>().
                keyType(keyType).
                valueType(valueType);
        TransactionMap<Value, Value> map = getMap(null);
        Value k = map.lastKey();
        map.getTransaction().commit();
        lastKey = k == null ? 0 : k.getLong();
    }

    /**
     * Rename the index.
     *
     * @param newName the new name
     */
    public void renameTable(String newName) {
        TransactionMap<Value, Value> map = getMap(null);
        rename(newName + "_DATA");
        String newMapName = newName + "_DATA_" + getId();
        map.renameMap(newMapName);
        map.getTransaction().commit();
        mapName = newMapName;
    }

    public String getCreateSQL() {
        return null;
    }

    public String getPlanSQL() {
        return table.getSQL() + ".tableScan";
    }

    public void setMainIndexColumn(int mainIndexColumn) {
        this.mainIndexColumn = mainIndexColumn;
    }

    public int getMainIndexColumn() {
        return mainIndexColumn;
    }

    @Override
    public void close(Session session) {
        // ok
    }

    @Override
    public void add(Session session, Row row) {
        if (mainIndexColumn == -1) {
            if (row.getKey() == 0) {
                row.setKey(++lastKey);
            }
        } else {
            Long c = row.getValue(mainIndexColumn).getLong();
            row.setKey(c);
        }
        Value[] array = new Value[columns.length];
        for (int i = 0; i < array.length; i++) {
            array[i] = row.getValue(i);
        }
        TransactionMap<Value, Value> map = getMap(session);
        if (map.containsKey(ValueLong.get(row.getKey()))) {
            String sql = "PRIMARY KEY ON " + table.getSQL();
            if (mainIndexColumn >= 0 && mainIndexColumn < indexColumns.length) {
                sql +=  "(" + indexColumns[mainIndexColumn].getSQL() + ")";
            }
            DbException e = DbException.get(ErrorCode.DUPLICATE_KEY_1, sql);
            e.setSource(this);
            throw e;
        }
        map.put(ValueLong.get(row.getKey()), ValueArray.get(array));
        lastKey = Math.max(lastKey, row.getKey());
    }

    @Override
    public void remove(Session session, Row row) {
        TransactionMap<Value, Value> map = getMap(session);
        Value old = map.remove(ValueLong.get(row.getKey()));
        if (old == null) {
            throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1,
                    getSQL() + ": " + row.getKey());
        }
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        long min, max;
        if (first == null || mainIndexColumn < 0) {
            min = Long.MIN_VALUE;
        } else {
            Value v = first.getValue(mainIndexColumn);
            if (v == null) {
                min = 0;
            } else {
                min = v.getLong();
            }
        }
        if (last == null || mainIndexColumn < 0) {
            max = Long.MAX_VALUE;
        } else {
            Value v = last.getValue(mainIndexColumn);
            if (v == null) {
                max = Long.MAX_VALUE;
            } else {
                max = v.getLong();
            }
        }
        TransactionMap<Value, Value> map = getMap(session);
        return new MVStoreCursor(session, map.keyIterator(
                ValueLong.get(min)), max);
    }

    public MVTable getTable() {
        return mvTable;
    }

    public Row getRow(Session session, long key) {
        TransactionMap<Value, Value> map = getMap(session);
        ValueArray array = (ValueArray) map.get(ValueLong.get(key));
        Row row = new Row(array.getList(), 0);
        row.setKey(key);
        return row;
    }

    @Override
    public double getCost(Session session, int[] masks, SortOrder sortOrder) {
        TransactionMap<Value, Value> map = getMap(session);
        long cost = 10 * (map.getSize() + Constants.COST_ROW_OFFSET);
        return cost;
    }

    public int getColumnIndex(Column col) {
        // can not use this index - use the delegate index instead
        return -1;
    }


    @Override
    public void remove(Session session) {
        TransactionMap<Value, Value> map = getMap(session);
        if (!map.isClosed()) {
            map.removeMap();
        }
    }

    @Override
    public void truncate(Session session) {
        TransactionMap<Value, Value> map = getMap(session);
        if (mvTable.getContainsLargeObject()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
        map.clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        TransactionMap<Value, Value> map = getMap(session);
        if (map.getSize() == 0) {
            return new MVStoreCursor(session, Collections.<Value>emptyList().iterator(), 0);
        }
        long key = first ? map.firstKey().getLong() : map.lastKey().getLong();
        MVStoreCursor cursor = new MVStoreCursor(session, 
                Arrays.asList((Value) ValueLong.get(key)).iterator(), key);
        cursor.next();
        return cursor;
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        TransactionMap<Value, Value> map = getMap(session);
        return map.getSize();
    }

    @Override
    public long getRowCountApproximation() {
        TransactionMap<Value, Value> map = getMap(null);
        long size = map.getSize();
        map.getTransaction().commit();
        return size;
    }

    public long getDiskSpaceUsed() {
        // TODO estimate disk space usage
        return 0;
    }

    @Override
    public void checkRename() {
        // ok
    }

    /**
     * Get the key from the row.
     *
     * @param row the row
     * @param ifEmpty the value to use if the row is empty
     * @param ifNull the value to use if the column is NULL
     * @return the key
     */
    long getKey(SearchRow row, long ifEmpty, long ifNull) {
        if (row == null) {
            return ifEmpty;
        }
        Value v = row.getValue(mainIndexColumn);
        if (v == null) {
            throw DbException.throwInternalError(row.toString());
        } else if (v == ValueNull.INSTANCE) {
            return ifNull;
        }
        return v.getLong();
    }

    /**
     * Search for a specific row or a set of rows.
     *
     * @param session the session
     * @param first the key of the first row
     * @param last the key of the last row
     * @return the cursor
     */
    Cursor find(Session session, long first, long last) {
        TransactionMap<Value, Value> map = getMap(session);
        return new MVStoreCursor(session, map.keyIterator(ValueLong.get(first)), last);
    }

    /**
     * A cursor.
     */
    class MVStoreCursor implements Cursor {

        private final Session session;
        private final Iterator<Value> it;
        private final long last;
        private ValueLong current;
        private Row row;

        public MVStoreCursor(Session session, Iterator<Value> it, long last) {
            this.session = session;
            this.it = it;
            this.last = last;
        }

        @Override
        public Row get() {
            if (row == null) {
                if (current != null) {
                    row = getRow(session, current.getLong());
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
            current = (ValueLong) it.next();
            if (current != null && current.getLong() > last) {
                current = null;
            }
            row = null;
            return current != null;
        }

        @Override
        public boolean previous() {
            // TODO previous
            return false;
        }

    }

    public boolean isRowIdIndex() {
        return true;
    }

    /**
     * Get the map to store the data.
     *
     * @param session the session
     * @return the map
     */
    TransactionMap<Value, Value> getMap(Session session) {
        if (session == null) {
            return mvTable.getTransaction(null).openMap(mapName, -1, mapBuilder);
        }
        Transaction t = mvTable.getTransaction(session);
        long version = session.getStatementVersion();
        return t.openMap(mapName, version, mapBuilder);
    }

}
