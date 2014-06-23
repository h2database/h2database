/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License, Version
 * 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.List;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueGeometry;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.quadtree.Quadtree;

/**
 * This is an in-memory index based on a R-Tree.
 */
public class SpatialTreeIndex extends BaseIndex implements SpatialIndex {

    private Quadtree root;

    private final RegularTable tableData;
    private long rowCount;
    private boolean closed;

    public SpatialTreeIndex(RegularTable table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
        if (indexType.isUnique()) {
            throw DbException.getUnsupportedException("not unique");
        }
        if (columns.length > 1) {
            throw DbException.getUnsupportedException("can only do one column");
        }
        if ((columns[0].sortType & SortOrder.DESCENDING) != 0) {
            throw DbException.getUnsupportedException("cannot do descending");
        }
        if ((columns[0].sortType & SortOrder.NULLS_FIRST) != 0) {
            throw DbException.getUnsupportedException("cannot do nulls first");
        }
        if ((columns[0].sortType & SortOrder.NULLS_LAST) != 0) {
            throw DbException.getUnsupportedException("cannot do nulls last");
        }

        initBaseIndex(table, id, indexName, columns, indexType);
        tableData = table;
        if (!database.isStarting()) {
            if (columns[0].column.getType() != Value.GEOMETRY) {
                throw DbException.getUnsupportedException("spatial index on non-geometry column, "
                        + columns[0].column.getCreateSQL());
            }
        }

        root = new Quadtree();
    }

    @Override
    public void close(Session session) {
        root = null;
        closed = true;
    }

    @Override
    public void add(Session session, Row row) {
        if (closed) {
            throw DbException.throwInternalError();
        }
        root.insert(getEnvelope(row), row);
        rowCount++;
    }

    private Envelope getEnvelope(SearchRow row) {
        Value v = row.getValue(columnIds[0]);
        Geometry g = ((ValueGeometry) v).getGeometry();
        return g.getEnvelopeInternal();
    }

    @Override
    public void remove(Session session, Row row) {
        if (closed) {
            throw DbException.throwInternalError();
        }
        if (!root.remove(getEnvelope(row), row)) {
            throw DbException.throwInternalError("row not found");
        }
        rowCount--;
    }

    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        return find();
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return find();
    }

    @SuppressWarnings("unchecked")
    private Cursor find() {
        // TODO use an external iterator,
        // but let's see if we can get it working first
        // TODO in the context of a spatial index,
        // a query that uses ">" or "<" has no real meaning, so for now just ignore
        // it and return all rows
        List<Row> list = root.queryAll();
        return new ListCursor(list, true /*first*/);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Cursor findByGeometry(TableFilter filter, SearchRow intersection) {
        // TODO use an external iterator,
        // but let's see if we can get it working first
        List<Row> list;
        if (intersection != null) {
            list = root.query(getEnvelope(intersection));
        } else {
            list = root.queryAll();
        }
        return new ListCursor(list, true/*first*/);
    }

    @Override
    public double getCost(Session session, int[] masks, SortOrder sortOrder) {
        return getCostRangeIndex(masks, tableData.getRowCountApproximation(), sortOrder);
    }

    @Override
    public void remove(Session session) {
        truncate(session);
    }

    @Override
    public void truncate(Session session) {
        root = null;
        rowCount = 0;
    }

    @Override
    public void checkRename() {
        // nothing to do
    }

    @Override
    public boolean needRebuild() {
        return true;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        if (closed) {
            throw DbException.throwInternalError();
        }

        // TODO use an external iterator,
        // but let's see if we can get it working first
        @SuppressWarnings("unchecked")
        List<Row> list = root.queryAll();

        return new ListCursor(list, first);
    }

    @Override
    public long getRowCount(Session session) {
        return rowCount;
    }

    @Override
    public long getRowCountApproximation() {
        return rowCount;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    /**
     * A cursor of a fixed list of rows.
     */
    private static final class ListCursor implements Cursor {
        private final List<Row> rows;
        private int index;
        private Row current;

        public ListCursor(List<Row> rows, boolean first) {
            this.rows = rows;
            this.index = first ? 0 : rows.size();
        }

        @Override
        public Row get() {
            return current;
        }

        @Override
        public SearchRow getSearchRow() {
            return current;
        }

        @Override
        public boolean next() {
            current = index >= rows.size() ? null : rows.get(index++);
            return current != null;
        }

        @Override
        public boolean previous() {
            current = index < 0 ? null : rows.get(index--);
            return current != null;
        }

    }

}
