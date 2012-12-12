/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.Iterator;
import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.type.ObjectDataType;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.value.Value;
import org.h2.value.ValueLong;

/**
 * A table stored in a MVStore.
 */
public class MVSecondaryIndex extends BaseIndex {

    protected final MVTable mvTable;
    protected final int keyColumns;
    protected MVMap<Value[], Long> map;

    public MVSecondaryIndex(Database db, MVTable table, int id, String indexName,
                IndexColumn[] columns, IndexType indexType) {
        this.mvTable = table;
        initBaseIndex(table, id, indexName, columns, indexType);
        // always store the row key in the map key,
        // even for unique indexes, as some of the index columns could be null
        keyColumns = columns.length + 1;
        int[] sortTypes = new int[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            sortTypes[i] = columns[i].sortType;
        }
        sortTypes[keyColumns - 1] = SortOrder.ASCENDING;
        ValueArrayDataType t = new ValueArrayDataType(
                db.getCompareMode(), db, sortTypes);
        map = new MVMap<Value[], Long>(t, new ObjectDataType());
        map = table.getStore().openMap(getName(), map);
    }

    @Override
    public void close(Session session) {
        // ok
    }

    @Override
    public void add(Session session, Row row) {
        Value[] array = getKey(row);
        if (indexType.isUnique()) {
            array[keyColumns - 1] = ValueLong.get(0);
            if (map.containsKey(array)) {
                throw getDuplicateKeyException();
            }
        }
        array[keyColumns - 1] = ValueLong.get(row.getKey());
        map.put(array, Long.valueOf(0));
    }

    @Override
    public void remove(Session session, Row row) {
        Value[] array = getKey(row);
        Long old = map.remove(array);
        if (old == null) {
            if (old == null) {
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1,
                        getSQL() + ": " + row.getKey());
            }
        }
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        Value[] min = getKey(first);
        return new MVStoreCursor(session, map.keyIterator(min), last);
    }

    private Value[] getKey(SearchRow r) {
        if (r == null) {
            return null;
        }
        Value[] array = new Value[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            Column c = columns[i];
            int idx = c.getColumnId();
            if (r != null) {
                array[i] = r.getValue(idx);
            }
        }
        array[keyColumns - 1] = ValueLong.get(r.getKey());
        return array;
    }

    public MVTable getTable() {
        return mvTable;
    }

    @Override
    public double getCost(Session session, int[] masks) {
        return 10 * getCostRangeIndex(masks, map.getSize());
    }

    @Override
    public void remove(Session session) {
        if (!map.isClosed()) {
            map.removeMap();
        }
    }

    @Override
    public void truncate(Session session) {
        map.clear();
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        return null;
    }

    @Override
    public boolean needRebuild() {
        // TODO there should be a better way
        return map.getSize() == 0;
    }

    @Override
    public long getRowCount(Session session) {
        return map.getSize();
    }

    @Override
    public long getRowCountApproximation() {
        return map.getSize();
    }

    @Override
    public void checkRename() {
        // ok
    }

    /**
     * A cursor.
     */
    class MVStoreCursor implements Cursor {

        private final Session session;
        private final Iterator<Value[]> it;
        private final SearchRow last;
        private Value[] current;
        private SearchRow searchRow;
        private Row row;

        public MVStoreCursor(Session session, Iterator<Value[]> it, SearchRow last) {
            this.session = session;
            this.it = it;
            this.last = last;
        }

        @Override
        public Row get() {
            if (row == null) {
                row = mvTable.getRow(session, getSearchRow().getKey());
            }
            return row;
        }

        @Override
        public SearchRow getSearchRow() {
            if (searchRow == null) {
                Value[] array = current;
                Column[] cols = getColumns();
                searchRow = mvTable.getTemplateRow();
                searchRow.setKey((array[array.length - 1]).getLong());
                for (int i = 0; i < array.length - 1; i++) {
                    Column c = cols[i];
                    int idx = c.getColumnId();
                    Value v = array[i];
                    searchRow.setValue(idx, v);
                }
            }
            return searchRow;
        }

        @Override
        public boolean next() {
            current = it.next();
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
            // TODO previous
            return false;
        }

    }

}
