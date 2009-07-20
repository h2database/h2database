/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
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
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * This is the most common type of index, a b-tree index.
 * The index structure is:
 * <ul>
 * <li>There is one {@link BtreeHead} that points to the root page.
 * The head always stays where it is.
 * </li><li>There is a number of {@link BtreePage}s. Each page is either
 * a {@link BtreeNode} or a {@link BtreeLeaf}.
 * </li><li>A node page links to other leaf pages or to node pages.
 * Leaf pages don't point to other pages (but may have a parent).
 * </li><li>The uppermost page is the root page. If pages
 * are added or deleted, the root page may change.
 * </li>
 * </ul>
 * Only the data of the indexed columns are stored in the index.
 */
public class BtreeIndex extends BaseIndex implements RecordReader {

    // TODO index / b-tree: tune page size
    // final static int MAX_PAGE_SIZE = 256;

    private Storage storage;
    private BtreePage rootPage;
    private TableData tableData;
    private BtreeHead head;
    private boolean needRebuild;
    private int headPos;
    private long lastChange;
    private long rowCount;
    private boolean headPosChanged;

    /**
     * Create a new b tree index with the given properties. If the index does
     * not yet exist, a new empty one is created.
     *
     * @param session the session
     * @param table the base table
     * @param id the object id
     * @param indexName the name of the index
     * @param columns the indexed columns
     * @param indexType the index type
     * @param headPos the position of the index header page, or Index.EMPTY_HEAD
     *            for a new index
     */
    public BtreeIndex(Session session, TableData table, int id, String indexName, IndexColumn[] columns,
            IndexType indexType, int headPos) throws SQLException {
        initBaseIndex(table, id, indexName, columns, indexType);
        this.tableData = table;
        Database db = table.getDatabase();
        storage = db.getStorage(this, id, false);
        this.headPos = headPos;
        if (headPos == Index.EMPTY_HEAD || database.getRecovery()) {
            truncate(session);
            needRebuild = true;
        } else {
            Record rec = storage.getRecordIfStored(session, headPos);
            if (rec != null && (rec instanceof BtreeHead)) {
                head = (BtreeHead) rec;
            }
            if (head != null && head.getConsistent()) {
                needRebuild = false;
                rowCount = table.getRowCount(session);
            } else {
                truncate(session);
                needRebuild = true;
            }
        }
    }

    private BtreePage getRoot(Session session) throws SQLException {
        if (rootPage == null) {
            setRoot((BtreePage) storage.getRecord(session, head.getRootPosition()));
        }
        return rootPage;
    }

    private BtreePage setRoot(BtreePage newRoot) {
        if (rootPage != null) {
            rootPage.setRoot(false);
        }
        newRoot.setRoot(true);
        rootPage = newRoot;
        return rootPage;
    }

    public int getHeadPos() {
        return headPos;
    }

    public void remove(Session session) throws SQLException {
        storage.truncate(session);
        database.removeStorage(storage.getId(), storage.getDiskFile());
        storage = null;
    }

    private void setChanged(Session session) throws SQLException {
        if (head != null && !database.getLogIndexChanges()) {
            // maybe there was a checkpoint, need to invalidate the summary in
            // this case too
            database.invalidateIndexSummary();
            if (head.getConsistent()) {
                deletePage(session, head);
                head.setConsistent(false);
                flushHead(session);
            }
            lastChange = System.currentTimeMillis();
        }
    }

    /**
     * Update a page in the storage.
     *
     * @param session the session
     * @param p the page to update
     */
    void updatePage(Session session, Record p) throws SQLException {
        if (SysProperties.REUSE_SPACE_BTREE_INDEX || database.getLogIndexChanges()) {
            storage.addRecord(session, p, p.getPos());
        } else {
            storage.updateRecord(session, p);
        }
    }

    /**
     * Delete a page from the storage.
     *
     * @param session the session
     * @param p the page to remove
     */
    void deletePage(Session session, Record p) throws SQLException {
        if (SysProperties.REUSE_SPACE_BTREE_INDEX || database.getLogIndexChanges()) {
            storage.removeRecord(session, p.getPos());
        }
    }

    /**
     * Add a page to the storage.
     *
     * @param session the session
     * @param p the page to add
     */
    void addPage(Session session, Record p) throws SQLException {
        storage.addRecord(session, p, Storage.ALLOCATE_POS);
    }

    /**
     * Get or read a page from the storage.
     *
     * @param session the session
     * @param i the page position
     * @return the page
     */
    BtreePage getPage(Session session, int i) throws SQLException {
        return (BtreePage) storage.getRecord(session, i);
    }

    /**
     * Write all changed paged to disk and mark the index as valid.
     *
     * @param session the session
     */
    public void flush(Session session) throws SQLException {
        lastChange = 0;
        if (storage != null) {
            storage.getDiskFile().flush();
            if (!database.isReadOnly()) {
                deletePage(session, head);
                // if we log index changes now, then the index is consistent
                // if we don't log index changes, then the index is only consistent
                // if there are no in doubt transactions
                if (database.getLogIndexChanges() || !database.getLog().containsInDoubtTransactions()) {
                    head.setConsistent(true);
                }
                flushHead(session);
            }
        }
    }

    public void close(Session session) throws SQLException {
        flush(session);
        if (headPosChanged) {
            database.update(session, this);
            headPosChanged = false;
        }
        storage = null;
    }

    public void add(Session session, Row r) throws SQLException {
        // create a row that only contains the key values
        setChanged(session);
        Row row = table.getTemplateRow();
        row.setPosAndVersion(r);
        for (int i = 0; i < columns.length; i++) {
            Column col = columns[i];
            int idx = col.getColumnId();
            Value v = r.getValue(idx);
            row.setValue(idx, v);
        }
        BtreePage root = getRoot(session);
        int splitPoint = root.add(row, session);
        if (splitPoint != 0) {
            SearchRow pivot = root.getData(splitPoint);
            BtreePage page1 = root;
            BtreePage page2 = root.split(session, splitPoint);
            root = setRoot(new BtreeNode(this, page1, pivot, page2));
            addPage(session, root);
            deletePage(session, head);
            head.setRootPosition(root.getPos());
            flushHead(session);
        }
        rowCount++;
    }

    /**
     * Create a search row for this row.
     *
     * @param row the row
     * @return the search row
     */
    SearchRow getSearchRow(Row row) {
        SearchRow r = table.getTemplateSimpleRow(columns.length == 1);
        r.setPosAndVersion(row);
        for (int j = 0; j < columns.length; j++) {
            int idx = columns[j].getColumnId();
            r.setValue(idx, row.getValue(idx));
        }
        return r;
    }

    public void remove(Session session, Row row) throws SQLException {
        setChanged(session);
        BtreePage root = getRoot(session);
        root.remove(session, row);
        rowCount--;
    }

    public boolean canFindNext() {
        return true;
    }

    public Cursor findNext(Session session, SearchRow first, SearchRow last) throws SQLException {
        return find(session, first, true, last);
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        return find(session, first, false, last);
    }

    private Cursor find(Session session, SearchRow first, boolean bigger, SearchRow last) throws SQLException {
        if (SysProperties.CHECK && storage == null) {
            throw Message.getSQLException(ErrorCode.OBJECT_CLOSED);
        }
        BtreePage root = getRoot(session);
        if (first == null) {
            BtreeCursor cursor = new BtreeCursor(session, this, last);
            root.first(cursor);
            return cursor;
        }
        BtreeCursor cursor = new BtreeCursor(session, this, last);
        if (getRowCount(session) == 0 || !root.findFirst(cursor, first, bigger)) {
            cursor.setCurrentRow(null);
        }
        return cursor;
    }

    public double getCost(Session session, int[] masks) {
        return 10 * getCostRangeIndex(masks, tableData.getRowCount(session));
    }

    public Record read(Session session, DataPage s) throws SQLException {
        char c = (char) s.readByte();
        if (c == 'N') {
            return new BtreeNode(this, s);
        } else if (c == 'L') {
            return new BtreeLeaf(this, session, s);
        } else if (c == 'H') {
            return new BtreeHead(s);
        } else {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, getName());
        }
    }

    /**
     * Read an array of rows from a data page.
     *
     * @param s the data page
     * @return the array of rows
     */
    ObjectArray<SearchRow> readRowArray(DataPage s) throws SQLException {
        int len = s.readInt();
        ObjectArray<SearchRow> rows = ObjectArray.newInstance(len);
        for (int i = 0; i < len; i++) {
            int pos = s.readInt();
            SearchRow r;
            if (pos < 0) {
                r = null;
            } else {
                r = table.getTemplateSimpleRow(columns.length == 1);
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

    /**
     * Get a row from the data file.
     *
     * @param session the session
     * @param pos the position in the data file
     * @return the row
     */
    Row getRow(Session session, int pos) throws SQLException {
        return tableData.getRow(session, pos);
    }

    private void flushHead(Session session) throws SQLException {
        updatePage(session, head);
        if (!database.getLogIndexChanges() && !database.isReadOnly()) {
            storage.flushRecord(head);
        }
        if (trace.isDebugEnabled()) {
            trace.debug("Index " + getSQL() + " head consistent=" + head.getConsistent());
        }
    }

    public void truncate(Session session) throws SQLException {
        setChanged(session);
        storage.truncate(session);
        head = new BtreeHead();
        addPage(session, head);
        ObjectArray<SearchRow> empty = ObjectArray.newInstance();
        BtreePage root = setRoot(new BtreeLeaf(this, empty));
        addPage(session, root);
        deletePage(session, head);
        head.setRootPosition(root.getPos());
        head.setConsistent(database.getLogIndexChanges());
        lastChange = System.currentTimeMillis();
        flushHead(session);
        int old = headPos;
        headPos = head.getPos();
        if (old != Index.EMPTY_HEAD) {
            // can not update the index entry now, because
            // updates are ignored at startup
            headPosChanged = true;
        }
        rowCount = 0;
    }

    public void checkRename() {
        // ok
    }

    public boolean needRebuild() {
        return needRebuild;
    }

    int getRecordOverhead() {
        return storage.getRecordOverhead();
    }

    /**
     * Get the last change time or 0 if the index has not been changed.
     *
     * @return the last change time or 0
     */
    public long getLastChange() {
        return lastChange;
    }

    public boolean canGetFirstOrLast() {
        return true;
    }

    public Cursor findFirstOrLast(Session session, boolean first) throws SQLException {
        if (first) {
            // TODO optimization: this loops through NULL elements
            Cursor cursor = find(session, null, false, null);
            while (cursor.next()) {
                SearchRow row = cursor.getSearchRow();
                Value v = row.getValue(columnIds[0]);
                if (v != ValueNull.INSTANCE) {
                    return cursor;
                }
            }
            return cursor;
        }
        BtreePage root = getRoot(session);
        BtreeCursor cursor = new BtreeCursor(session, this, null);
        root.last(cursor);
        // TODO optimization: this loops through NULL elements
        do {
            SearchRow row = cursor.getSearchRow();
            if (row == null) {
                break;
            }
            Value v = row.getValue(columnIds[0]);
            if (v != ValueNull.INSTANCE) {
                return cursor;
            }
        } while (cursor.previous());
        return cursor;
    }

    public long getRowCount(Session session) {
        return rowCount;
    }

    public long getRowCountApproximation() {
        return rowCount;
    }

}
