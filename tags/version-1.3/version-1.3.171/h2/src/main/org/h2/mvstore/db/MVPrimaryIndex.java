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
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A table stored in a MVStore.
 */
public class MVPrimaryIndex extends BaseIndex {

    private final MVTable mvTable;
    private MVMap<Long, Value[]> map;
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
        ValueArrayDataType t = new ValueArrayDataType(
                db.getCompareMode(), db, sortTypes);
        map = table.getStore().openMap(getName() + "_" + getId(),
                new MVMap.Builder<Long, Value[]>().valueType(t));
        Long k = map.lastKey();
        lastKey = k == null ? 0 : k;
    }

    /**
     * Rename the index.
     *
     * @param newName the new name
     */
    public void renameTable(String newName) {
        MVMap<Long, Value[]> map = getMap(null);
        rename(newName + "_DATA");
        map.renameMap(newName + "_DATA_" + getId());
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
        MVMap<Long, Value[]> map = getMap(session);
        if (map.containsKey(row.getKey())) {
            String sql = "PRIMARY KEY ON " + table.getSQL();
            if (mainIndexColumn >= 0 && mainIndexColumn < indexColumns.length) {
                sql +=  "(" + indexColumns[mainIndexColumn].getSQL() + ")";
            }
            DbException e = DbException.get(ErrorCode.DUPLICATE_KEY_1, sql);
            e.setSource(this);
            throw e;
        }
        map.put(row.getKey(), array);
        lastKey = Math.max(lastKey, row.getKey());
    }

    @Override
    public void remove(Session session, Row row) {
        MVMap<Long, Value[]> map = getMap(session);
        Value[] old = map.remove(row.getKey());
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
        MVMap<Long, Value[]> map = getMap(session);
        return new MVStoreCursor(session, map.keyIterator(min), max);
    }

    public MVTable getTable() {
        return mvTable;
    }

    public Row getRow(Session session, long key) {
        MVMap<Long, Value[]> map = getMap(session);
        Value[] array = map.get(key);
        Row row = new Row(array, 0);
        row.setKey(key);
        return row;
    }

    @Override
    public double getCost(Session session, int[] masks) {
        MVMap<Long, Value[]> map = getMap(session);
        long cost = 10 * (map.getSize() + Constants.COST_ROW_OFFSET);
        return cost;
    }

    public int getColumnIndex(Column col) {
        // can not use this index - use the delegate index instead
        return -1;
    }


    @Override
    public void remove(Session session) {
        MVMap<Long, Value[]> map = getMap(session);
        if (!map.isClosed()) {
            map.removeMap();
        }
    }

    @Override
    public void truncate(Session session) {
        MVMap<Long, Value[]> map = getMap(session);
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
        MVMap<Long, Value[]> map = getMap(session);
        if (map.getSize() == 0) {
            return new MVStoreCursor(session, Collections.<Long>emptyList().iterator(), 0);
        }
        long key = first ? map.firstKey() : map.lastKey();
        MVStoreCursor cursor = new MVStoreCursor(session, Arrays.asList(key).iterator(), key);
        cursor.next();
        return cursor;
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        MVMap<Long, Value[]> map = getMap(session);
        return map.getSize();
    }

    @Override
    public long getRowCountApproximation() {
        MVMap<Long, Value[]> map = getMap(null);
        return map.getSize();
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
        MVMap<Long, Value[]> map = getMap(session);
        return new MVStoreCursor(session, map.keyIterator(first), last);
    }

    /**
     * A cursor.
     */
    class MVStoreCursor implements Cursor {

        private final Session session;
        private final Iterator<Long> it;
        private final long last;
        private Long current;
        private Row row;

        public MVStoreCursor(Session session, Iterator<Long> it, long last) {
            this.session = session;
            this.it = it;
            this.last = last;
        }

        @Override
        public Row get() {
            if (row == null) {
                if (current != null) {
                    row = getRow(session, current);
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
            current = it.next();
            if (current != null && current > last) {
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
    MVMap<Long, Value[]> getMap(Session session) {
        // return mvTable.getTransaction(session).openMap(name)
        return map;
    }

}
