/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.log.UndoLogRecord;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.Storage;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;
import org.h2.util.ObjectUtils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueLob;

/**
 * The scan index is not really an 'index' in the strict sense, because it can not be used for direct lookup.
 * It can only be used to iterate over all rows of a table.
 * Each regular table has one such object, even if no primary key or indexes are defined.
 */
public class ScanIndex extends BaseIndex {
    private int firstFree = -1;
    private ObjectArray rows = new ObjectArray();
    private Storage storage;
    private TableData tableData;
    private boolean containsLargeObject;
    private int rowCountDiff;
    private HashMap sessionRowCount;
    private HashSet delta;

    public ScanIndex(TableData table, int id, IndexColumn[] columns, IndexType indexType) throws SQLException {
        super(table, id, table.getName() + "_TABLE_SCAN", columns, indexType);
        if (database.isMultiVersion()) {
            sessionRowCount = new HashMap();
        }
        tableData = table;
        if (!database.isPersistent() || id < 0) {
            return;
        }
        this.storage = database.getStorage(table, id, true);
        int count = storage.getRecordCount();
        rowCount = count;
        table.setRowCount(count);
        trace.info("open existing " + table.getName() + " rows: " + count);
        for (int i = 0; i < columns.length; i++) {
            if (DataType.isLargeObject(columns[i].column.getType())) {
                containsLargeObject = true;
            }
        }
    }

    public void remove(Session session) throws SQLException {
        truncate(session);
        if (storage != null) {
            storage.delete(session);
        }
    }

    public void truncate(Session session) throws SQLException {
        if (storage == null) {
            rows = new ObjectArray();
            firstFree = -1;
        } else {
            storage.truncate(session);
        }
        if (containsLargeObject && tableData.isPersistent()) {
            ValueLob.removeAllForTable(database, table.getId());
        }
        tableData.setRowCount(0);
        rowCount = 0;
        if (database.isMultiVersion()) {
            sessionRowCount.clear();
        }
    }

    public String getCreateSQL() {
        return null;
    }

    public void close(Session session) throws SQLException {
        if (storage != null) {
            storage = null;
        }
    }

    public Row getRow(Session session, int key) throws SQLException {
        if (storage != null) {
            return (Row) storage.getRecord(session, key);
        }
        return (Row) rows.get(key);
    }

    public void add(Session session, Row row) throws SQLException {
        if (storage != null) {
            if (containsLargeObject) {
                for (int i = 0; i < row.getColumnCount(); i++) {
                    Value v = row.getValue(i);
                    Value v2 = v.link(database, getId());
                    session.unlinkAtCommitStop(v2);
                    if (v != v2) {
                        row.setValue(i, v2);
                    }
                }
            }
            storage.addRecord(session, row, Storage.ALLOCATE_POS);
        } else {
            if (firstFree == -1) {
                int key = rows.size();
                row.setPos(key);
                rows.add(row);
            } else {
                int key = firstFree;
                Row free = (Row) rows.get(key);
                firstFree = free.getPos();
                row.setPos(key);
                rows.set(key, row);
            }
            row.setDeleted(false);
        }
        if (database.isMultiVersion()) {
            if (delta == null) {
                delta = new HashSet();
            }
            boolean wasDeleted = delta.remove(row);
            if (!wasDeleted) {
                delta.add(row);
            }
            incrementRowCount(session.getId(), 1);
        }
        rowCount++;
    }

    public void commit(int operation, Row row) throws SQLException {
        if (database.isMultiVersion()) {
            if (delta != null && operation == UndoLogRecord.DELETE) {
                delta.remove(row);
            }
            incrementRowCount(row.getSessionId(), operation == UndoLogRecord.DELETE ? 1 : -1);
        }
    }

    private void incrementRowCount(int sessionId, int count) {
        if (database.isMultiVersion()) {
            Integer id = ObjectUtils.getInteger(sessionId);
            Integer c = (Integer) sessionRowCount.get(id);
            int current = c == null ? 0 : c.intValue();
            sessionRowCount.put(id, ObjectUtils.getInteger(current + count));
            rowCountDiff += count;
        }
    }

    public void remove(Session session, Row row) throws SQLException {
        if (storage != null) {
            storage.removeRecord(session, row.getPos());
            if (containsLargeObject) {
                for (int i = 0; i < row.getColumnCount(); i++) {
                    Value v = row.getValue(i);
                    if (v.isLinked()) {
                        session.unlinkAtCommit(v);
                    }
                }
            }
        } else {
            Row free = new Row(null, 0);
            free.setPos(firstFree);
            int key = row.getPos();
            rows.set(key, free);
            firstFree = key;
        }
        if (database.isMultiVersion()) {
            // if storage is null, the delete flag is not yet set
            row.setDeleted(true);
            if (delta == null) {
                delta = new HashSet();
            }
            boolean wasAdded = delta.remove(row);
            if (!wasAdded) {
                delta.add(row);
            }
            incrementRowCount(session.getId(), -1);
        }
        rowCount--;
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        return new ScanCursor(session, this, database.isMultiVersion());
    }

    public double getCost(Session session, int[] masks) throws SQLException {
        long cost = tableData.getRowCount(session) + Constants.COST_ROW_OFFSET;
        if (storage != null) {
            cost *= 10;
        }
        return cost;
    }

    public long getRowCount(Session session) {
        if (database.isMultiVersion()) {
            Integer i = (Integer) sessionRowCount.get(ObjectUtils.getInteger(session.getId()));
            long count = i == null ? 0 : i.intValue();
            count += super.getRowCount(session);
            count -= rowCountDiff;
            return count;
        }
        return super.getRowCount(session);
    }

    Row getNextRow(Session session, Row row) throws SQLException {
        if (storage == null) {
            int key;
            if (row == null) {
                key = -1;
            } else {
                key = row.getPos();
            }
            while (true) {
                key++;
                if (key >= rows.size()) {
                    return null;
                }
                row = (Row) rows.get(key);
                if (!row.isEmpty()) {
                    return row;
                }
            }
        }
        int pos = storage.getNext(row);
        if (pos < 0) {
            return null;
        }
        return (Row) storage.getRecord(session, pos);
    }

    public int getColumnIndex(Column col) {
        // the scan index cannot use any columns
        return -1;
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException();
    }

    public boolean needRebuild() {
        return false;
    }

    public boolean canGetFirstOrLast() {
        return false;
    }

    public SearchRow findFirstOrLast(Session session, boolean first) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public Iterator getDelta() {
        return delta == null ? Collections.EMPTY_LIST.iterator() : delta.iterator();
    }

}
