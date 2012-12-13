/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.IndexColumn;

/**
 * An index that delegates indexing to another index.
 */
public class MVDelegateIndex extends BaseIndex {

    private final MVPrimaryIndex mainIndex;

    public MVDelegateIndex(MVTable table, int id, String name,
            MVPrimaryIndex mainIndex,
            IndexType indexType) {
        IndexColumn[] cols = IndexColumn.wrap(new Column[] { table.getColumn(mainIndex.getMainIndexColumn())});
        this.initBaseIndex(table, id, name, cols, indexType);
        this.mainIndex = mainIndex;
        if (!database.isPersistent() || id < 0) {
            throw DbException.throwInternalError("" + name);
        }
    }

    public void add(Session session, Row row) {
        // nothing to do
    }

    public boolean canFindNext() {
        return false;
    }

    public boolean canGetFirstOrLast() {
        return false;
        // TODO
        // return true;
    }

    public void close(Session session) {
        // nothing to do
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) {
        long min = mainIndex.getKey(first, Long.MIN_VALUE, Long.MIN_VALUE);
        // ifNull is MIN_VALUE as well, because the column is never NULL
        // so avoid returning all rows (returning one row is OK)
        long max = mainIndex.getKey(last, Long.MAX_VALUE, Long.MIN_VALUE);
        return mainIndex.find(session, min, max);
    }

    public Cursor findFirstOrLast(Session session, boolean first) {
        return null;
//        Cursor cursor;
//        if (first) {
//            cursor = mainIndex.find(session, Long.MIN_VALUE, Long.MAX_VALUE, false);
//        } else  {
//            long x = mainIndex.getLastKey();
//            cursor = mainIndex.find(session, x, x, false);
//        }
//        cursor.next();
//        return cursor;
    }

    public Cursor findNext(Session session, SearchRow higherThan, SearchRow last) {
        throw DbException.throwInternalError();
    }

    public int getColumnIndex(Column col) {
        if (col.getColumnId() == mainIndex.getMainIndexColumn()) {
            return 0;
        }
        return -1;
    }

    public double getCost(Session session, int[] masks) {
        return 10 * getCostRangeIndex(masks, mainIndex.getRowCount(session));
    }

    public boolean needRebuild() {
        return false;
    }

    public void remove(Session session, Row row) {
        // nothing to do
    }

    public void remove(Session session) {
        mainIndex.setMainIndexColumn(-1);
        // TODO remove map?
        // session.getDatabase().getPageStore().removeMeta(this, session);
    }

    public void truncate(Session session) {
        // nothing to do
    }

    public void checkRename() {
        // ok
    }

    public long getRowCount(Session session) {
        return mainIndex.getRowCount(session);
    }

    public long getRowCountApproximation() {
        return mainIndex.getRowCountApproximation();
    }

    public long getDiskSpaceUsed() {
        return 0;
    }

}
