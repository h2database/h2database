/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import java.util.List;

import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.SessionLocal;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.result.Row;
import org.h2.result.RowFactory;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.VersionedValue;

/**
 * An index that delegates indexing to another index.
 */
public class MVDelegateIndex extends MVIndex<Long, SearchRow> {

    private final MVPrimaryIndex mainIndex;

    public MVDelegateIndex(MVTable table, int id, String name, MVPrimaryIndex mainIndex, IndexType indexType) {
        super(table, id, name, IndexColumn.wrap(new Column[] { table.getColumn(mainIndex.getMainIndexColumn()) }),
                1, indexType);
        this.mainIndex = mainIndex;
        if (id < 0) {
            throw DbException.getInternalError(name);
        }
    }

    @Override
    public RowFactory getRowFactory() {
        return mainIndex.getRowFactory();
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
    public MVMap<Long,VersionedValue<SearchRow>> getMVMap() {
        return mainIndex.getMVMap();
    }

    @Override
    public void add(SessionLocal session, Row row) {
        // nothing to do
    }

    @Override
    public Row getRow(SessionLocal session, long key) {
        return mainIndex.getRow(session, key);
    }

    @Override
    public boolean isRowIdIndex() {
        return true;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public void close(SessionLocal session) {
        // nothing to do
    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last, boolean reverse) {
        return mainIndex.find(session, first, last, reverse);
    }

    @Override
    public Cursor findFirstOrLast(SessionLocal session, boolean first) {
        return mainIndex.findFirstOrLast(session, first);
    }

    @Override
    public int getColumnIndex(Column col) {
        if (col.getColumnId() == mainIndex.getMainIndexColumn()) {
            return 0;
        }
        return -1;
    }

    @Override
    public boolean isFirstColumn(Column column) {
        return getColumnIndex(column) == 0;
    }

    @Override
    public double getCost(SessionLocal session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        return 10 * getCostRangeIndex(masks, mainIndex.getRowCountApproximation(session),
                filters, filter, sortOrder, true, allColumnsSet);
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public void remove(SessionLocal session, Row row) {
        // nothing to do
    }

    @Override
    public void update(SessionLocal session, Row oldRow, Row newRow) {
        // nothing to do
    }

    @Override
    public void remove(SessionLocal session) {
        mainIndex.setMainIndexColumn(SearchRow.ROWID_INDEX);
    }

    @Override
    public void truncate(SessionLocal session) {
        // nothing to do
    }

    @Override
    public long getRowCount(SessionLocal session) {
        return mainIndex.getRowCount(session);
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return mainIndex.getRowCountApproximation(session);
    }

}
