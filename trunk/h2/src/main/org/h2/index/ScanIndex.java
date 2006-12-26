/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.Storage;
import org.h2.table.Column;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueLob;

/**
 * @author Thomas
 */
public class ScanIndex extends Index {
    private int firstFree = -1;
    private ObjectArray rows = new ObjectArray();
    private Storage storage;
    private TableData tableData;
    private boolean containsLargeObject;

    public ScanIndex(TableData table, int id, Column[] columns, IndexType indexType)
            throws SQLException {
        super(table, id, table.getName() + "_TABLE_SCAN", columns, indexType);
        tableData = table;
        Database db = table.getDatabase();
        if(!db.isPersistent() || id < 0) {
            return;
        }
        this.storage = db.getStorage(table, id, true);
        int count = storage.getRecordCount();
        rowCount = count;
        table.setRowCount(count);
        trace.info("open existing " + table.getName() + " rows: " + count);
        for(int i=0; i<columns.length; i++) {
            if(DataType.isLargeObject(columns[i].getType())) {
                containsLargeObject = true;
            }
        }
    }

    public void remove(Session session) throws SQLException {
        truncate(session);
        if(storage != null) {
            storage.delete(session);
        }
    }

    public void truncate(Session session) throws SQLException {
        if(storage == null) {
            rows = new ObjectArray();
            firstFree = -1;
        } else {
            storage.truncate(session);
        }
        if(containsLargeObject && tableData.isPersistent()) {
            ValueLob.removeAllForTable(database, table.getId());
        }
        tableData.setRowCount(0);
        rowCount = 0;
    }

    public String getCreateSQL() {
        return null;
    }

    public void close(Session session) throws SQLException {
        if(storage != null) {
            storage = null;
        }
    }

    public Row getRow(int key) throws SQLException {
        if(storage != null) {
            return (Row) storage.getRecord(key);
        }
        return (Row) rows.get(key);
    }

    public void add(Session session, Row row) throws SQLException {
        if(storage != null) {
            if(containsLargeObject) {
                for(int i=0; i<row.getColumnCount(); i++) {
                    Value v = row.getValue(i);
                    Value v2 = v.link(database, getId());
                    session.unlinkAtCommitStop(v2);
                    if(v != v2) {
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
        }
        rowCount++;
    }

    public void remove(Session session, Row row) throws SQLException {
        if(storage != null) {
            storage.removeRecord(session, row.getPos());
            if(containsLargeObject) {
                for(int i=0; i<row.getColumnCount(); i++) {
                    Value v = row.getValue(i);
                    if(v.isLinked()) {
                        session.unlinkAtCommit(v);
                    }
                }
            }
        } else {
            Row free = new Row();
            free.setPos(firstFree);
            int key = row.getPos();
            rows.set(key, free);
            firstFree = key;
        }
        rowCount--;
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        return new ScanCursor(this);
    }

    public int getCost(int[] masks) throws SQLException {
        int cost = tableData.getRowCount() + Constants.COST_ROW_OFFSET;
        if(storage != null) {
            cost *= 10;
        }
        return cost;
    }

    Row getNextRow(Row row) throws SQLException {
        if(storage == null) {
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
        return (Row) storage.getRecord(pos);
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

    public boolean canGetFirstOrLast(boolean first) {
        return false;
    }

    public Value findFirstOrLast(Session session, boolean first) throws SQLException {
        throw Message.getUnsupportedException();
    }

}
