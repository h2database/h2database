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
import org.h2.store.DataPage;
import org.h2.store.Record;
import org.h2.store.RecordReader;
import org.h2.store.Storage;
import org.h2.table.Column;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * @author Thomas
 */
public class BtreeIndex extends Index implements RecordReader {

    // TODO index / btree: tune page size
    // final static int MAX_PAGE_SIZE = 256;

    private Storage storage;
    private BtreePage root;
    private TableData tableData;
    private BtreeHead head;
    private boolean needRebuild;
    private int headPos;
    private long lastChange;

    public BtreeIndex(Session session, TableData table, int id, String indexName, Column[] columns,
            IndexType indexType, int headPos) throws SQLException {
        // TODO we need to log index data
        super(table, id, indexName, columns, indexType);
        this.tableData = table;
        Database db = table.getDatabase();
        storage = db.getStorage(this, id, false);
        this.headPos = headPos;
        if (headPos == Index.EMPTY_HEAD || database.getRecovery()) {
            truncate(session);
            needRebuild = true;
        } else {
            Record rec = storage.getRecordIfStored(headPos);
            if(rec != null && (rec instanceof BtreeHead)) {
                head = (BtreeHead) rec;
            }
            if(head != null && head.getConsistent()) {
                setRoot((BtreePage) storage.getRecord(head.getRootPosition()));
                needRebuild = false;
                rowCount = table.getRowCount();
            } else {
                truncate(session);
                needRebuild = true;
            }
        }
    }

    private void setRoot(BtreePage newRoot) {
        if (root != null) {
            root.setRoot(false);
        }
        newRoot.setRoot(true);
        root = newRoot;
    }

    public int getHeadPos() {
        return headPos;
    }

    public void remove(Session session) throws SQLException {
        storage.delete(session);
        storage = null;
    }

    private void setChanged(Session session) throws SQLException {
        if(head != null && !database.getLogIndexChanges()) {
            if(head.getConsistent()) {
                deletePage(session, head);
                database.invalidateIndexSummary();
                head.setConsistent(false);
                flushHead(session);
            }
            lastChange = System.currentTimeMillis();
        }
    }

    void updatePage(Session session, Record p) throws SQLException {
        if(database.getLogIndexChanges()) {
            storage.addRecord(session, p, p.getPos());
        } else {
            storage.updateRecord(session, p);
        }
    }

int count;

    void deletePage(Session session, Record p) throws SQLException {
        if(database.getLogIndexChanges()) {
            storage.removeRecord(session, p.getPos());
        }
    }

    void addPage(Session session, Record p) throws SQLException {
        storage.addRecord(session, p, Storage.ALLOCATE_POS);
    }

    public BtreePage getPage(int i) throws SQLException {
        return (BtreePage) storage.getRecord(i);
    }

    public void flush(Session session) throws SQLException {
        lastChange = 0;
        if(storage != null) {
            storage.flushFile();
            deletePage(session, head);
            int todoCheckAndDocumentThisCondition;
            if(database.getLogIndexChanges() || !database.getLog().containsInDoubtTransactions()) {
                head.setConsistent(true);
            }
            flushHead(session);
        }
    }

    public void close(Session session) throws SQLException {
        flush(session);
        storage = null;
    }

    public void add(Session session, Row r) throws SQLException {
        // create a row that only contains the key values
        setChanged(session);
        Row row = table.getTemplateRow();
        row.setPos(r.getPos());
        for (int i = 0; i < columns.length; i++) {
            Column col = columns[i];
            int idx = col.getColumnId();
            Value v = r.getValue(idx);
            row.setValue(idx, v);
        }
        int splitPoint = root.add(row, session);
        if (splitPoint != 0) {
            SearchRow pivot = root.getData(splitPoint);
            int test;
            // deletePage(session, root);
            BtreePage page1 = root;
            BtreePage page2 = root.split(session, splitPoint);
            setRoot(new BtreeNode(this, page1, pivot, page2));
            addPage(session, root);
            deletePage(session, head);
            head.setRootPosition(root.getPos());
            flushHead(session);
        }
        rowCount++;
    }

    public void remove(Session session, Row row) throws SQLException {
        setChanged(session);
        if(rowCount == 1) {
            truncate(session);
        } else {
            root.remove(session, row, 0);
            rowCount--;
        }
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        if(Constants.CHECK && storage == null) {
            throw Message.getSQLException(Message.OBJECT_CLOSED);
        }
        if(first==null) {
            BtreeCursor cursor = new BtreeCursor(this, last);
            root.first(cursor);
            return cursor;
        } else {
            BtreeCursor cursor = new BtreeCursor(this, last);
            if (!root.findFirst(cursor, first)) {
                cursor.setCurrentRow(Cursor.POS_NO_ROW);
            }
            return cursor;
        }
    }

    public int getLookupCost(int rowCount) {
        for(int i=0, j = 1; ; i++) {
            j *= BtreePage.BLOCKS_PER_PAGE;
            if(j>rowCount) {
                return (i+1);
            }
        }
    }

    public int getCost(int[] masks) throws SQLException {
        return 10 * getCostRangeIndex(masks, tableData.getRowCount());
    }

    public Record read(DataPage s) throws SQLException {
        char c = (char) s.readByte();
        if (c == 'N') {
            return new BtreeNode(this, s);
        } else if (c == 'L') {
            return new BtreeLeaf(this, s);
        } else if (c == 'H') {
            return new BtreeHead(s);
        } else {
            throw Message.getSQLException(Message.FILE_CORRUPTED_1, getName());
        }
    }

    ObjectArray readRowArray(DataPage s) throws SQLException {
        int len = s.readInt();
        ObjectArray rows = new ObjectArray(len);
        for (int i = 0; i < len; i++) {
            int pos = s.readInt();
            SearchRow r;
            if(pos < 0) {
                r = null;
            } else {
                r = table.getTemplateSimpleRow(columns.length==1);
                r.setPos(pos);
                for (int j = 0; j < columns.length; j++) {
                    int idx = columns[j].getColumnId();
                    r.setValue(idx, s.readValue());
                }
            }
            rows.add(r);
        }
        return rows;
    }

    public Row getRow(int pos) throws SQLException {
        return tableData.getRow(pos);
    }

    private void flushHead(Session session) throws SQLException {
        updatePage(session, head);
        if(!database.getLogIndexChanges() && !database.getReadOnly()) {
            storage.flushRecord(head);
        }
        trace.debug("Index " + getSQL() + " head consistent="+head.getConsistent());
    }

    public void truncate(Session session) throws SQLException {
        setChanged(session);
        storage.truncate(session);
        head = new BtreeHead();
        addPage(session, head);
        setRoot(new BtreeLeaf(this, new ObjectArray()));
        addPage(session, root);
        deletePage(session, head);
        head.setRootPosition(root.getPos());
        head.setConsistent(database.getLogIndexChanges());
        lastChange = System.currentTimeMillis();
        flushHead(session);
        headPos = head.getPos();
        rowCount = 0;
    }

    public void checkRename() throws SQLException {
        // ok
    }

    public boolean needRebuild() {
        return needRebuild;
    }

    public int getRecordOverhead() {
        return storage.getRecordOverhead();
    }

    public long getLastChange() {
        return lastChange;
    }

    public boolean canGetFirstOrLast(boolean first) {
        return true;
    }

    public Value findFirstOrLast(Session session, boolean first) throws SQLException {
        if(first) {
            // TODO optimization: this loops through NULL values
            Cursor cursor = find(session, null, null);
            while(cursor.next()) {
                Value v = cursor.get().getValue(columnIndex[0]);
                if(v != ValueNull.INSTANCE) {
                    return v;
                }
            }
            return ValueNull.INSTANCE;
        } else {
            SearchRow row = root.getLast();
            if(row != null) {
                Value v = row.getValue(columnIndex[0]);
                return v;
            }
            return ValueNull.INSTANCE;
        }
    }

}
