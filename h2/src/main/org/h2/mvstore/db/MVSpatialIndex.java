/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import static org.h2.util.geometry.GeometryUtils.MAX_X;
import static org.h2.util.geometry.GeometryUtils.MAX_Y;
import static org.h2.util.geometry.GeometryUtils.MIN_X;
import static org.h2.util.geometry.GeometryUtils.MIN_Y;

import java.util.Iterator;
import java.util.List;
import org.h2.api.ErrorCode;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.index.Cursor;
import org.h2.index.IndexCondition;
import org.h2.index.IndexType;
import org.h2.index.SpatialIndex;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStoreException;
import org.h2.mvstore.Page;
import org.h2.mvstore.rtree.MVRTreeMap;
import org.h2.mvstore.rtree.MVRTreeMap.RTreeCursor;
import org.h2.mvstore.rtree.Spatial;
import org.h2.mvstore.tx.Transaction;
import org.h2.mvstore.tx.TransactionMap;
import org.h2.mvstore.tx.VersionedValueType;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueGeometry;
import org.h2.value.ValueNull;
import org.h2.value.VersionedValue;

/**
 * This is an index based on a MVRTreeMap.
 *
 * @author Thomas Mueller
 * @author Noel Grandin
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class MVSpatialIndex extends MVIndex<Spatial, Value> implements SpatialIndex {

    /**
     * The multi-value table.
     */
    final MVTable mvTable;

    private final TransactionMap<Spatial, Value> dataMap;
    private final MVRTreeMap<VersionedValue<Value>> spatialMap;

    /**
     * Constructor.
     *
     * @param db the database
     * @param table the table instance
     * @param id the index id
     * @param indexName the index name
     * @param columns the indexed columns (only one geometry column allowed)
     * @param uniqueColumnCount count of unique columns (0 or 1)
     * @param indexType the index type (only spatial index)
     */
    public MVSpatialIndex(Database db, MVTable table, int id, String indexName, IndexColumn[] columns,
            int uniqueColumnCount, IndexType indexType) {
        super(table, id, indexName, columns, uniqueColumnCount, indexType);
        if (columns.length != 1) {
            throw DbException.getUnsupportedException(
                    "Can only index one column");
        }
        IndexColumn col = columns[0];
        if ((col.sortType & SortOrder.DESCENDING) != 0) {
            throw DbException.getUnsupportedException(
                    "Cannot index in descending order");
        }
        if ((col.sortType & SortOrder.NULLS_FIRST) != 0) {
            throw DbException.getUnsupportedException(
                    "Nulls first is not supported");
        }
        if ((col.sortType & SortOrder.NULLS_LAST) != 0) {
            throw DbException.getUnsupportedException(
                    "Nulls last is not supported");
        }
        if (col.column.getType().getValueType() != Value.GEOMETRY) {
            throw DbException.getUnsupportedException(
                    "Spatial index on non-geometry column, "
                    + col.column.getCreateSQL());
        }
        this.mvTable = table;
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }
        String mapName = "index." + getId();
        VersionedValueType<Value, Database> valueType = new VersionedValueType<>(NullValueDataType.INSTANCE);
        MVRTreeMap.Builder<VersionedValue<Value>> mapBuilder =
                new MVRTreeMap.Builder<VersionedValue<Value>>().
                valueType(valueType);
        spatialMap = db.getStore().getMvStore().openMap(mapName, mapBuilder);
        Transaction t = mvTable.getTransactionBegin();
        dataMap = t.openMapX(spatialMap);
        dataMap.map.setVolatile(!table.isPersistData() || !indexType.isPersistent());
        t.commit();
    }

    @Override
    public void addRowsToBuffer(List<Row> rows, String bufferName) {
        throw DbException.getInternalError();
    }

    @Override
    public void addBufferedRows(List<String> bufferNames) {
        throw DbException.getInternalError();
    }

    @Override
    public void close(SessionLocal session) {
        // ok
    }

    @Override
    public void add(SessionLocal session, Row row) {
        TransactionMap<Spatial, Value> map = getMap(session);
        SpatialKey key = getKey(row);

        if (key.isNull()) {
            return;
        }

        if (uniqueColumnColumn > 0) {
            // this will detect committed entries only
            RTreeCursor<VersionedValue<Value>> cursor = spatialMap.findContainedKeys(key);
            Iterator<Spatial> it = new SpatialKeyIterator(map, cursor, false);
            while (it.hasNext()) {
                Spatial k = it.next();
                if (k.equalsIgnoringId(key)) {
                    throw getDuplicateKeyException(key.toString());
                }
            }
        }
        try {
            map.put(key, ValueNull.INSTANCE);
        } catch (MVStoreException e) {
            throw mvTable.convertException(e);
        }
        if (uniqueColumnColumn > 0) {
            // check if there is another (uncommitted) entry
            RTreeCursor<VersionedValue<Value>> cursor = spatialMap.findContainedKeys(key);
            Iterator<Spatial> it = new SpatialKeyIterator(map, cursor, true);
            while (it.hasNext()) {
                Spatial k = it.next();
                if (k.equalsIgnoringId(key)) {
                    if (map.isSameTransaction(k)) {
                        continue;
                    }
                    map.remove(key);
                    if (map.getImmediate(k) != null) {
                        // committed
                        throw getDuplicateKeyException(k.toString());
                    }
                    throw DbException.get(ErrorCode.CONCURRENT_UPDATE_1, table.getName());
                }
            }
        }
    }

    @Override
    public void remove(SessionLocal session, Row row) {
        SpatialKey key = getKey(row);

        if (key.isNull()) {
            return;
        }

        TransactionMap<Spatial, Value> map = getMap(session);
        try {
            Value old = map.remove(key);
            if (old == null) {
                StringBuilder builder = new StringBuilder();
                getSQL(builder, TRACE_SQL_FLAGS).append(": ").append(row.getKey());
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1, builder.toString());
            }
        } catch (MVStoreException e) {
            throw mvTable.convertException(e);
        }
    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last, boolean reverse) {
        Iterator<Spatial> cursor = reverse ? spatialMap.keyIteratorReverse(null) : spatialMap.keyIterator(null);
        TransactionMap<Spatial, Value> map = getMap(session);
        Iterator<Spatial> it = new SpatialKeyIterator(map, cursor, false);
        return new MVStoreCursor(session, it, mvTable);
    }

    @Override
    public Cursor findByGeometry(SessionLocal session, SearchRow first, SearchRow last, boolean reverse,
            SearchRow intersection) {
        if (intersection == null) {
            return find(session, first, last, reverse);
        }
        Iterator<Spatial> cursor =
                spatialMap.findIntersectingKeys(getKey(intersection));
        TransactionMap<Spatial, Value> map = getMap(session);
        Iterator<Spatial> it = new SpatialKeyIterator(map, cursor, false);
        return new MVStoreCursor(session, it, mvTable);
    }

    /**
     * Returns the minimum bounding box that encloses all keys.
     *
     * @param session the session
     * @return the minimum bounding box that encloses all keys, or null
     */
    public Value getBounds(SessionLocal session) {
        FindBoundsCursor cursor = new FindBoundsCursor(spatialMap.getRootPage(), new SpatialKey(0), session,
                getMap(session), columnIds[0]);
        while (cursor.hasNext()) {
            cursor.next();
        }
        return cursor.getBounds();
    }

    /**
     * Returns the estimated minimum bounding box that encloses all keys.
     *
     * The returned value may be incorrect.
     *
     * @param session the session
     * @return the estimated minimum bounding box that encloses all keys, or null
     */
    public Value getEstimatedBounds(SessionLocal session) {
        Page<Spatial,VersionedValue<Value>> p = spatialMap.getRootPage();
        int count = p.getKeyCount();
        if (count > 0) {
            Spatial key = p.getKey(0);
            float bminxf = key.min(0), bmaxxf = key.max(0), bminyf = key.min(1), bmaxyf = key.max(1);
            for (int i = 1; i < count; i++) {
                key = p.getKey(i);
                float minxf = key.min(0), maxxf = key.max(0), minyf = key.min(1), maxyf = key.max(1);
                if (minxf < bminxf) {
                    bminxf = minxf;
                }
                if (maxxf > bmaxxf) {
                    bmaxxf = maxxf;
                }
                if (minyf < bminyf) {
                    bminyf = minyf;
                }
                if (maxyf > bmaxyf) {
                    bmaxyf = maxyf;
                }
            }
            return ValueGeometry.fromEnvelope(new double[] {bminxf, bmaxxf, bminyf, bmaxyf});
        }
        return ValueNull.INSTANCE;
    }

    private SpatialKey getKey(SearchRow row) {
        Value v = row.getValue(columnIds[0]);
        double[] env;
        if (v == ValueNull.INSTANCE || (env = v.convertToGeometry(null).getEnvelopeNoCopy()) == null) {
            return new SpatialKey(row.getKey());
        }
        return new SpatialKey(row.getKey(),
                (float) env[MIN_X], (float) env[MAX_X],
                (float) env[MIN_Y], (float) env[MAX_Y]);
    }

    @Override
    public MVTable getTable() {
        return mvTable;
    }

    @Override
    public double getCost(SessionLocal session, int[] masks, TableFilter[] filters,
            int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        // Never use spatial tree index without spatial filter
        if (columns.length == 0) {
            return Long.MAX_VALUE;
        }
        for (Column column : columns) {
            int index = column.getColumnId();
            int mask = masks[index];
            if ((mask & IndexCondition.SPATIAL_INTERSECTS) != IndexCondition.SPATIAL_INTERSECTS) {
                return Long.MAX_VALUE;
            }
        }
        return 10 * getCostRangeIndex(masks, dataMap.sizeAsLongMax(), filters, filter, sortOrder, true, allColumnsSet);
    }

    @Override
    public void remove(SessionLocal session) {
        TransactionMap<Spatial, Value> map = getMap(session);
        if (!map.isClosed()) {
            Transaction t = session.getTransaction();
            t.removeMap(map);
        }
    }

    @Override
    public void truncate(SessionLocal session) {
        TransactionMap<Spatial, Value> map = getMap(session);
        map.clear();
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
    public long getRowCount(SessionLocal session) {
        TransactionMap<Spatial, Value> map = getMap(session);
        return map.sizeAsLong();
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        try {
            return dataMap.sizeAsLongMax();
        } catch (MVStoreException e) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED, e);
        }
    }

    /**
     * Get the map to store the data.
     *
     * @param session the session
     * @return the map
     */
    private TransactionMap<Spatial, Value> getMap(SessionLocal session) {
        if (session == null) {
            return dataMap;
        }
        Transaction t = session.getTransaction();
        return dataMap.getInstance(t);
    }

    @Override
    public MVMap<Spatial, VersionedValue<Value>> getMVMap() {
        return dataMap.map;
    }

    /**
     * A cursor.
     */
    private static class MVStoreCursor implements Cursor {

        private final SessionLocal session;
        private final Iterator<Spatial> it;
        private final MVTable mvTable;
        private Spatial current;
        private SearchRow searchRow;
        private Row row;

        MVStoreCursor(SessionLocal session, Iterator<Spatial> it, MVTable mvTable) {
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
            if (searchRow == null) {
                if (current != null) {
                    searchRow = mvTable.getTemplateRow();
                    searchRow.setKey(current.getId());
                }
            }
            return searchRow;
        }

        @Override
        public boolean next() {
            current = it.hasNext() ? it.next() : null;
            searchRow = null;
            row = null;
            return current != null;
        }

        @Override
        public boolean previous() {
            throw DbException.getUnsupportedException("previous");
        }

    }

    private static class SpatialKeyIterator implements Iterator<Spatial> {

        private final TransactionMap<Spatial, Value> map;
        private final Iterator<Spatial> iterator;
        private final boolean includeUncommitted;
        private Spatial current;

        SpatialKeyIterator(TransactionMap<Spatial, Value> map,
                            Iterator<Spatial> iterator, boolean includeUncommitted) {
            this.map = map;
            this.iterator = iterator;
            this.includeUncommitted = includeUncommitted;
            fetchNext();
        }

        private void fetchNext() {
            while (iterator.hasNext()) {
                current = iterator.next();
                if (includeUncommitted || map.containsKey(current)) {
                    return;
                }
            }
            current = null;
        }

        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public Spatial next() {
            Spatial result = current;
            fetchNext();
            return result;
        }
    }

    /**
     * A cursor for getBounds() method.
     */
    private final class FindBoundsCursor extends RTreeCursor<VersionedValue<Value>> {

        private final SessionLocal session;

        private final TransactionMap<Spatial, Value> map;

        private final int columnId;

        private boolean hasBounds;

        private float bminxf, bmaxxf, bminyf, bmaxyf;

        private double bminxd, bmaxxd, bminyd, bmaxyd;

        FindBoundsCursor(Page<Spatial,VersionedValue<Value>> root, Spatial filter, SessionLocal session,
                TransactionMap<Spatial, Value> map, int columnId) {
            super(root, filter);
            this.session = session;
            this.map = map;
            this.columnId = columnId;
        }

        @Override
        protected boolean check(boolean leaf, Spatial key, Spatial test) {
            float minxf = key.min(0), maxxf = key.max(0), minyf = key.min(1), maxyf = key.max(1);
            if (leaf) {
                if (hasBounds) {
                    if ((minxf <= bminxf || maxxf >= bmaxxf || minyf <= bminyf || maxyf >= bmaxyf)
                            && map.containsKey(key)) {
                        double[] env = ((ValueGeometry) mvTable.getRow(session, key.getId()).getValue(columnId))
                                .getEnvelopeNoCopy();
                        double minxd = env[MIN_X], maxxd = env[MAX_X], minyd = env[MIN_Y], maxyd = env[MAX_Y];
                        if (minxd < bminxd) {
                            bminxf = minxf;
                            bminxd = minxd;
                        }
                        if (maxxd > bmaxxd) {
                            bmaxxf = maxxf;
                            bmaxxd = maxxd;
                        }
                        if (minyd < bminyd) {
                            bminyf = minyf;
                            bminyd = minyd;
                        }
                        if (maxyd > bmaxyd) {
                            bmaxyf = maxyf;
                            bmaxyd = maxyd;
                        }
                    }
                } else if (map.containsKey(key)) {
                    hasBounds = true;
                    double[] env = ((ValueGeometry) mvTable.getRow(session, key.getId()).getValue(columnId))
                            .getEnvelopeNoCopy();
                    bminxf = minxf;
                    bminxd = env[MIN_X];
                    bmaxxf = maxxf;
                    bmaxxd = env[MAX_X];
                    bminyf = minyf;
                    bminyd = env[MIN_Y];
                    bmaxyf = maxyf;
                    bmaxyd = env[MAX_Y];
                }
            } else if (hasBounds) {
                if (minxf <= bminxf || maxxf >= bmaxxf || minyf <= bminyf || maxyf >= bmaxyf) {
                    return true;
                }
            } else {
                return true;
            }
            return false;
        }

        Value getBounds() {
            return hasBounds ? ValueGeometry.fromEnvelope(new double[] {bminxd, bmaxxd, bminyd, bmaxyd})
                    : ValueNull.INSTANCE;
        }

    }

}

