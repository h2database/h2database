/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.engine.UndoLogRecord;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.util.New;

/**
 * The scan index is not really an 'index' in the strict sense, because it can
 * not be used for direct lookup. It can only be used to iterate over all rows
 * of a table. Each regular table has one such object, even if no primary key or
 * indexes are defined.
 */
public class ScanIndex extends BaseIndex {
    private long firstFree = -1;
    private ArrayList<Row> rows = New.arrayList();
    private RegularTable tableData;
    private int rowCountDiff;
    private HashMap<Integer, Integer> sessionRowCount;
    private HashSet<Row> delta;
    private long rowCount;

    public ScanIndex(RegularTable table, int id, IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, table.getName() + "_DATA", columns, indexType);
        if (database.isMultiVersion()) {
            sessionRowCount = New.hashMap();
        }
        tableData = table;
    }

    public void remove(Session session) {
        truncate(session);
    }

    public void truncate(Session session) {
        rows = New.arrayList();
        firstFree = -1;
        if (tableData.getContainsLargeObject() && tableData.isPersistData()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
        tableData.setRowCount(0);
        rowCount = 0;
        rowCountDiff = 0;
        if (database.isMultiVersion()) {
            sessionRowCount.clear();
        }
    }

    public String getCreateSQL() {
        return null;
    }

    public void close(Session session) {
        // nothing to do
    }

    public Row getRow(Session session, long key) {
        return rows.get((int) key);
    }

    public void add(Session session, Row row) {
        // in-memory
        if (firstFree == -1) {
            int key = rows.size();
            row.setKey(key);
            rows.add(row);
        } else {
            long key = firstFree;
            Row free = rows.get((int) key);
            firstFree = free.getKey();
            row.setKey(key);
            rows.set((int) key, row);
        }
        row.setDeleted(false);
        if (database.isMultiVersion()) {
            if (delta == null) {
                delta = New.hashSet();
            }
            boolean wasDeleted = delta.remove(row);
            if (!wasDeleted) {
                delta.add(row);
            }
            incrementRowCount(session.getId(), 1);
        }
        rowCount++;
    }

    public void commit(int operation, Row row) {
        if (database.isMultiVersion()) {
            if (delta != null) {
                delta.remove(row);
            }
            incrementRowCount(row.getSessionId(), operation == UndoLogRecord.DELETE ? 1 : -1);
        }
    }

    private void incrementRowCount(int sessionId, int count) {
        if (database.isMultiVersion()) {
            Integer id = sessionId;
            Integer c = sessionRowCount.get(id);
            int current = c == null ? 0 : c.intValue();
            sessionRowCount.put(id, current + count);
            rowCountDiff += count;
        }
    }

    public void remove(Session session, Row row) {
        // in-memory
        if (!database.isMultiVersion() && rowCount == 1) {
            rows = New.arrayList();
            firstFree = -1;
        } else {
            Row free = new Row(null, 1);
            free.setKey(firstFree);
            long key = row.getKey();
            rows.set((int) key, free);
            firstFree = key;
        }
        if (database.isMultiVersion()) {
            // if storage is null, the delete flag is not yet set
            row.setDeleted(true);
            if (delta == null) {
                delta = New.hashSet();
            }
            boolean wasAdded = delta.remove(row);
            if (!wasAdded) {
                delta.add(row);
            }
            incrementRowCount(session.getId(), -1);
        }
        rowCount--;
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return new ScanCursor(session, this, database.isMultiVersion());
    }

    public double getCost(Session session, int[] masks) {
        return tableData.getRowCountApproximation() + Constants.COST_ROW_OFFSET;
    }

    public long getRowCount(Session session) {
        if (database.isMultiVersion()) {
            Integer i = sessionRowCount.get(session.getId());
            long count = i == null ? 0 : i.intValue();
            count += rowCount;
            count -= rowCountDiff;
            return count;
        }
        return rowCount;
    }

    /**
     * Get the next row that is stored after this row.
     *
     * @param session the session
     * @param row the current row or null to start the scan
     * @return the next row or null if there are no more rows
     */
    Row getNextRow(Session session, Row row) {
        long key;
        if (row == null) {
            key = -1;
        } else {
            key = row.getKey();
        }
        while (true) {
            key++;
            if (key >= rows.size()) {
                return null;
            }
            row = rows.get((int) key);
            if (!row.isEmpty()) {
                return row;
            }
        }
    }

    public int getColumnIndex(Column col) {
        // the scan index cannot use any columns
        return -1;
    }

    public void checkRename() {
        throw DbException.getUnsupportedException("SCAN");
    }

    public boolean needRebuild() {
        return false;
    }

    public boolean canGetFirstOrLast() {
        return false;
    }

    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("SCAN");
    }

    Iterator<Row> getDelta() {
        if (delta == null) {
            List<Row> e = Collections.emptyList();
            return e.iterator();
        }
        return delta.iterator();
    }

    public long getRowCountApproximation() {
        return rowCount;
    }

    public String getPlanSQL() {
        return table.getSQL() + ".tableScan";
    }

}
