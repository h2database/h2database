/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.PageBtreeIndex;
import org.h2.index.PageScanIndex;
import org.h2.log.SessionState;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceSystem;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.util.Cache;
import org.h2.util.CacheLRU;
import org.h2.util.CacheObject;
import org.h2.util.CacheWriter;
import org.h2.util.FileUtils;
import org.h2.util.ObjectArray;
import org.h2.util.ObjectUtils;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueString;

/**
 * This class represents a file that is organized as a number of pages. The
 * first page (page 0) contains the file header, which is never modified once
 * the database is created. The format is:
 * <ul>
 * <li>0-47: file header (3 time "-- H2 0.5/B -- \n")</li>
 * <li>48-51: database page size in bytes
 *     (512 - 32768, must be a power of 2)</li>
 * <li>52: write version (0, otherwise the file is opened in read-only mode)</li>
 * <li>53: read version (0, otherwise opening the file fails)</li>
 * <li>54-57: meta table root page number (usually 1)</li>
 * <li>58-61: free list head page number (usually 2)</li>
 * <li>62-65: log[0] head page number (usually 3)</li>
 * <li>66-69: log[1] head page number (usually 4)</li>
 * </ul>
 */
public class PageStore implements CacheWriter {

    // TODO check if PageLog.reservePages is required
    // TODO PageDataLeaf and Node: support random delete/add
    // TODO PageStore.openMetaIndex (add collation for indexes,
    // desc columns support)
    // TODO btree index with fixed size values doesn't need offset and so on
    // TODO log block allocation
    // TODO block compression: maybe http://en.wikipedia.org/wiki/LZJB
    // with RLE, specially for 0s.
    // TODO test that setPageId updates parent, overflow parent
    // TODO order pages so that searching for a key
    // doesn't seek backwards in the file
    // TODO use an undo log and maybe redo log (for performance)
    // TODO checksum: 0 for empty; position hash + every 128th byte,
    // specially important for log; misdirected reads or writes
    // TODO type, sequence (start with random); checksum (start with block id)
    // TODO for lists: write sequence byte
    // TODO completely re-use keys of deleted rows; maybe
    // remember last page with deleted keys (in the root page?),
    // and chain such pages
    // TODO remove Database.objectIds
    // TODO detect circles in linked lists
    // (input stream, free list, extend pages...)
    // at runtime and recovery
    // synchronized correctly (on the index?)
    // TODO two phase commit: append (not patch) commit & rollback
    // TODO remove trace or use isDebugEnabled
    // TODO recover tool: don't re-do uncommitted operations
    // TODO no need to log old page if it was always empty
    // TODO don't store default values (store a special value)
    // TODO btree: maybe split at the insertion point
    // TODO split files (1 GB max size)
    // TODO add a setting (that can be changed at runtime) to call fsync
    // and delay on each commit
    // TODO var int: see google protocol buffers

    /**
     * The smallest possible page size.
     */
    public static final int PAGE_SIZE_MIN = 128;

    /**
     * The biggest possible page size.
     */
    public static final int PAGE_SIZE_MAX = 32768;

    /**
     * The default page size.
     */
    public static final int PAGE_SIZE_DEFAULT = 1024;

    /**
     * The number of log streams.
     */
    public static final int LOG_COUNT = 2;

    static final int INCREMENT_PAGES = 128;
    private static final int READ_VERSION = 0;
    private static final int WRITE_VERSION = 0;

    private static final int META_TYPE_SCAN_INDEX = 0;
    private static final int META_TYPE_BTREE_INDEX = 1;
    private static final int META_TABLE_ID = -1;

    private Database database;
    private final Trace trace;
    private String fileName;
    private FileStore file;
    private String accessMode;
    private int cacheSize;
    private Cache cache;

    private int pageSize;
    private int pageSizeShift;
    private int metaTableRootPageId;
    private int freeListRootPageId;
    private int lastUsedPage;

    private int activeLog;
    private int[] logRootPageIds = new int[LOG_COUNT];
    private boolean recoveryRunning;
    private HashMap sessionStates = new HashMap();

    /**
     * The file size in bytes.
     */
    private long fileLength;

    /**
     * Number of pages (including free pages).
     */
    private int pageCount;

    /**
     * The transaction logs.
     */
    private PageLog[] logs = new PageLog[LOG_COUNT];

    private Schema metaSchema;
    private TableData metaTable;
    private PageScanIndex metaIndex;
    private HashMap metaObjects;
    private int systemTableHeadPos;

    /**
     * Create a new page store object.
     *
     * @param database the database
     * @param fileName the file name
     * @param accessMode the access mode
     * @param cacheSizeDefault the default cache size
     */
    public PageStore(Database database, String fileName, String accessMode, int cacheSizeDefault) throws SQLException {
        this.fileName = fileName;
        this.accessMode = accessMode;
        this.database = database;
        trace = database.getTrace(Trace.PAGE_STORE);
        int test;
// trace.setLevel(TraceSystem.DEBUG);
        this.cacheSize = cacheSizeDefault;
        String cacheType = database.getCacheType();
        this.cache = CacheLRU.getCache(this, cacheType, cacheSize);
    }

    /**
     * Copy the next page to the output stream.
     *
     * @param pageId the page to copy
     * @param out the output stream
     * @return the new position, or -1 if there is no more data to copy
     */
    public int copyDirect(int pageId, OutputStream out) throws SQLException {
        synchronized (database) {
            byte[] buffer = new byte[pageSize];
            try {
                if (pageId >= pageCount) {
                    return -1;
                }
                file.seek(pageId * pageSize);
                file.readFullyDirect(buffer, 0, pageSize);
                out.write(buffer, 0, pageSize);
                return pageId + 1;
            } catch (IOException e) {
                throw Message.convertIOException(e, fileName);
            }
        }
    }

    /**
     * Open the file and read the header.
     */
    public void open() throws SQLException {
        try {
            if (FileUtils.exists(fileName)) {
                // existing
                file = database.openFile(fileName, accessMode, true);
                readHeader();
                fileLength = file.length();
                pageCount = (int) (fileLength / pageSize);
                initLogs();
                recover(true);
                recover(false);
                checkpoint();
            } else {
                // new
                setPageSize(PAGE_SIZE_DEFAULT);
                file = database.openFile(fileName, accessMode, false);
                metaTableRootPageId = 1;
                freeListRootPageId = 2;
                pageCount = 3 + LOG_COUNT;
                increaseFileSize(INCREMENT_PAGES - pageCount);
                getFreeList();
                for (int i = 0; i < LOG_COUNT; i++) {
                    logRootPageIds[i] = 3 + i;
                }
                writeHeader();
                initLogs();
                openMetaIndex();
                getLog().openForWriting(0);
                switchLogIfPossible();
                getLog().flush();
                systemTableHeadPos = Index.EMPTY_HEAD;
            }
            lastUsedPage = getFreeList().getLastUsed() + 1;
        } catch (SQLException e) {
            close();
            throw e;
        }
    }

    private void initLogs() {
        for (int i = 0; i < LOG_COUNT; i++) {
            logs[i] = new PageLog(this, logRootPageIds[i]);
        }
    }

    /**
     * Flush all pending changes to disk, and re-open the log file.
     */
    public void checkpoint() throws SQLException {
        trace.debug("checkpoint");
        if (getLog() == null || database.isReadOnly()) {
            // the file was never fully opened
            return;
        }
        synchronized (database) {
            database.checkPowerOff();
            ObjectArray list = cache.getAllChanged();
            CacheObject.sort(list);
            for (int i = 0; i < list.size(); i++) {
                Record rec = (Record) list.get(i);
                writeBack(rec);
            }
            int todoFlushBeforeReopen;
            // switch twice so there are no redo entries
            switchLogIfPossible();
            switchLogIfPossible();
            int todoWriteDeletedPages;
        }
        int pageCount = getFreeList().getLastUsed() + 1;
        trace.debug("pageCount:" + pageCount);
        file.setLength(pageSize * pageCount);
    }

    private void switchLogIfPossible() throws SQLException {
        trace.debug("switchLogIfPossible");
        if (database.isReadOnly()) {
            return;
        }
        int id = getLog().getId();
        getLog().close();
        activeLog = (activeLog + 1) % LOG_COUNT;
        int todoCanOnlyReuseAfterLoggedChangesAreWritten;
        getLog().openForWriting(id + 1);

//        Session[] sessions = database.getSessions(true);
//        int firstUncommittedLog = getLog().getId();
//        int firstUncommittedPos = getLog().getPos();
//        for (int i = 0; i < sessions.length; i++) {
//            Session session = sessions[i];
//            int log = session.getFirstUncommittedLog();
//            int pos = session.getFirstUncommittedPos();
//            if (pos != LOG_WRITTEN) {
//                if (log < firstUncommittedLog ||
//        (log == firstUncommittedLog && pos < firstUncommittedPos)) {
//                    firstUncommittedLog = log;
//                    firstUncommittedPos = pos;
//                }
//            }
//        }


//        if (nextLog.containsUncommitted())
//        activeLog = nextLogId;
//        getLog().reopen();
    }

    private void readHeader() throws SQLException {
        long length = file.length();
        if (length < PAGE_SIZE_MIN) {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, fileName);
        }
        database.notifyFileSize(length);
        file.seek(FileStore.HEADER_LENGTH);
        DataPage page = DataPage.create(database, new byte[PAGE_SIZE_MIN - FileStore.HEADER_LENGTH]);
        file.readFully(page.getBytes(), 0, PAGE_SIZE_MIN - FileStore.HEADER_LENGTH);
        setPageSize(page.readInt());
        int writeVersion = page.readByte();
        int readVersion = page.readByte();
        if (readVersion != 0) {
            throw Message.getSQLException(ErrorCode.FILE_VERSION_ERROR_1, fileName);
        }
        if (writeVersion != 0) {
            try {
                file.close();
            } catch (IOException e) {
                throw Message.convertIOException(e, "close");
            }
            accessMode = "r";
            file = database.openFile(fileName, accessMode, true);
        }
        metaTableRootPageId = page.readInt();
        freeListRootPageId = page.readInt();
        for (int i = 0; i < LOG_COUNT; i++) {
            logRootPageIds[i] = page.readInt();
        }
    }

    /**
     * Set the page size. The size must be a power of two. This method must be
     * called before opening.
     *
     * @param size the page size
     */
    public void setPageSize(int size) throws SQLException {
        if (size < PAGE_SIZE_MIN || size > PAGE_SIZE_MAX) {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, fileName);
        }
        boolean good = false;
        int shift = 0;
        for (int i = 1; i <= size;) {
            if (size == i) {
                good = true;
                break;
            }
            shift++;
            i += i;
        }
        if (!good) {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, fileName);
        }
        pageSize = size;
        pageSizeShift = shift;
    }

    private void writeHeader() throws SQLException {
        int todoChecksumForHeader;
        DataPage page = DataPage.create(database, new byte[pageSize - FileStore.HEADER_LENGTH]);
        page.writeInt(pageSize);
        page.writeByte((byte) WRITE_VERSION);
        page.writeByte((byte) READ_VERSION);
        page.writeInt(metaTableRootPageId);
        page.writeInt(freeListRootPageId);
        for (int i = 0; i < LOG_COUNT; i++) {
            page.writeInt(logRootPageIds[i]);
        }
        file.seek(FileStore.HEADER_LENGTH);
        file.write(page.getBytes(), 0, pageSize - FileStore.HEADER_LENGTH);
    }

    /**
     * Close the file without writing anything.
     */
    public void close() throws SQLException {
        try {
            trace.debug("close");
            if (file != null) {
                file.close();
            }
            file = null;
        } catch (IOException e) {
            throw Message.convertIOException(e, "close");
        }
    }

    public void flushLog() throws SQLException {
        getLog().flush();
    }

    public Trace getTrace() {
        return trace;
    }

    public void writeBack(CacheObject obj) throws SQLException {
        synchronized (database) {
            Record record = (Record) obj;
            if (trace.isDebugEnabled()) {
                trace.debug("writeBack " + record);
            }
            int todoRemoveParameter;
            record.write(null);
            record.setChanged(false);
        }
    }

    /**
     * Update a record.
     *
     * @param record the record
     * @param logUndo if an undo entry need to be logged
     * @param old the old data (if known)
     */
    public void updateRecord(Record record, boolean logUndo, DataPage old) throws SQLException {
        synchronized (database) {
            if (trace.isDebugEnabled()) {
                if (!record.isChanged()) {
                    trace.debug("updateRecord " + record.toString());
                }
            }
            record.setChanged(true);
            int pos = record.getPos();
            getFreeList().allocate(pos);
            cache.update(pos, record);
            if (logUndo && !recoveryRunning) {
                if (old == null) {
                    old = readPage(pos);
                }
                getLog().addUndo(record.getPos(), old);
            }
        }
    }

    /**
     * Allocate a page.
     *
     * @return the page id
     */
    public int allocatePage() throws SQLException {
        return allocatePage(false);
    }

    private PageFreeList getFreeList() throws SQLException {
        PageFreeList free = (PageFreeList) cache.find(freeListRootPageId);
        if (free == null) {
            free = new PageFreeList(this, freeListRootPageId, 3 + LOG_COUNT);
            free.read();
            cache.put(free);
        }
        return free;
    }

    /**
     * Allocate a page.
     *
     * @param atEnd if the allocated page must be at the end of the file
     * @return the page id
     */
    public int allocatePage(boolean atEnd) throws SQLException {
        PageFreeList list = getFreeList();
        int id;
        while (true) {
            id = atEnd ? list.allocateAtEnd(++lastUsedPage) : list.allocate();
            if (id < 0) {
                increaseFileSize(INCREMENT_PAGES);
            } else {
                break;
            }
        }
        if (trace.isDebugEnabled()) {
            trace.debug("allocated " + id + " atEnd:" + atEnd);
        }
        if (id >= pageCount) {
            increaseFileSize(INCREMENT_PAGES);
        }
        return id;
    }

    private void increaseFileSize(int increment) throws SQLException {
        pageCount += increment;
        long newLength = pageCount * pageSize;
        file.setLength(newLength);
        fileLength = newLength;
    }

    /**
     * Add a page to the free list.
     *
     * @param pageId the page id
     * @param logUndo if an undo entry need to be logged
     * @param old the old data (if known)
     */
    public void freePage(int pageId, boolean logUndo, DataPage old) throws SQLException {
        if (trace.isDebugEnabled()) {
            trace.debug("freePage " + pageId);
        }
        cache.remove(pageId);
        getFreeList().free(pageId);
        if (recoveryRunning) {
            writePage(pageId, createDataPage());
        } else if (logUndo) {
            if (old == null) {
                old = readPage(pageId);
            }
            getLog().addUndo(pageId, old);
        }

    }

    /**
     * Create a data page.
     *
     * @return the data page.
     */
    public DataPage createDataPage() {
        return DataPage.create(database, new byte[pageSize]);
    }

    /**
     * Get the record if it is stored in the file, or null if not.
     *
     * @param pos the page id
     * @return the record or null
     */
    public Record getRecord(int pos) {
        CacheObject obj = cache.find(pos);
        return (Record) obj;
    }

    /**
     * Read a page.
     *
     * @param pos the page id
     * @return the page
     */
    public DataPage readPage(int pos) throws SQLException {
        DataPage page = createDataPage();
        readPage(pos, page);
        return page;
    }

    /**
     * Read a page.
     *
     * @param pos the page id
     * @param page the page
     */
    public void readPage(int pos, DataPage page) throws SQLException {
        if (pos >= pageCount) {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, pos + " of " + pageCount);
        }
        file.seek(pos << pageSizeShift);
        file.readFully(page.getBytes(), 0, pageSize);
    }

    /**
     * Get the page size.
     *
     * @return the page size
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Get the number of pages (including free pages).
     *
     * @return the page count
     */
    public int getPageCount() {
        return pageCount;
    }

    /**
     * Write a page.
     *
     * @param pageId the page id
     * @param data the data
     */
    public void writePage(int pageId, DataPage data) throws SQLException {
        file.seek(pageId << pageSizeShift);
        file.write(data.getBytes(), 0, pageSize);
    }

    /**
     * Remove a page from the cache.
     *
     * @param pageId the page id
     */
    public void removeRecord(int pageId) {
        cache.remove(pageId);
    }

    /**
     * Set the root page of the free list.
     *
     * @param pageId the first free list page
     * @param existing if the page already exists
     * @param next the next page
     */
    void setFreeListRootPage(int pageId, boolean existing, int next) throws SQLException {
        this.freeListRootPageId = pageId;
        if (!existing) {
            PageFreeList free = new PageFreeList(this, pageId, next);
            updateRecord(free, false, null);
        }
    }

    PageLog getLog() {
        return logs[activeLog];
    }

    Database getDatabase() {
        return database;
    }

    /**
     * Run one recovery stage. There are two recovery stages: first (undo is
     * true) only the undo steps are run (restoring the state before the last
     * checkpoint). In the second stage (undo is false) the committed operations
     * are re-applied.
     *
     * @param undo true if the undo step should be run
     */
    private void recover(boolean undo) throws SQLException {
        trace.debug("log recover #" + undo);
        if (!undo) {
            openMetaIndex();
            readMetaData();
        }
        try {
            recoveryRunning = true;
            int maxId = 0;
            for (int i = 0; i < LOG_COUNT; i++) {
                int id = logs[i].openForReading();
                if (id > maxId) {
                    maxId = id;
                    activeLog = i;
                }
            }
            for (int i = 0; i < LOG_COUNT; i++) {
                int j;
                if (undo) {
                    // undo: start with the newest file and go backward
                    j = Math.abs(activeLog - i) % LOG_COUNT;
                } else {
                    // redo: start with the oldest log file
                    j = (activeLog + 1 + i) % LOG_COUNT;
                }
                logs[j].recover(undo);
            }
            if (!undo) {
                switchLogIfPossible();
                int todoProbablyStillRequiredForTwoPhaseCommit;
                sessionStates = new HashMap();
            }
        } catch (SQLException e) {
            int test;
            e.printStackTrace();
            throw e;
        } catch (RuntimeException e) {
            int test;
            e.printStackTrace();
            throw e;
        } finally {
            recoveryRunning = false;
        }
        if (!undo) {
            PageScanIndex index = (PageScanIndex) metaObjects.get(ObjectUtils.getInteger(0));
            if (index == null) {
                systemTableHeadPos = Index.EMPTY_HEAD;
            } else {
                systemTableHeadPos = index.getHeadPos();
            }
            for (Iterator it = metaObjects.values().iterator(); it.hasNext();) {
                Index openIndex = (Index) it.next();
                openIndex.close(database.getSystemSession());
            }
            metaObjects = null;
            trace.debug("log recover done");
        }
    }

    /**
     * A record is added to a table, or removed from a table.
     *
     * @param session the session
     * @param tableId the table id
     * @param row the row to add
     * @param add true if the row is added, false if it is removed
     */
    public void logAddOrRemoveRow(Session session, int tableId, Row row, boolean add) throws SQLException {
        if (!recoveryRunning) {
            getLog().logAddOrRemoveRow(session, tableId, row, add);
        }
    }

    /**
     * Mark a committed transaction.
     *
     * @param session the session
     */
    public void commit(Session session) throws SQLException {
        getLog().commit(session);
    }

    /**
     * Get the session state for this session. A new object is created if there
     * is no session state yet.
     *
     * @param sessionId the session id
     * @return the session state object
     */
    private SessionState getOrAddSessionState(int sessionId) {
        Integer key = ObjectUtils.getInteger(sessionId);
        SessionState state = (SessionState) sessionStates.get(key);
        if (state == null) {
            state = new SessionState();
            sessionStates.put(key, state);
            state.sessionId = sessionId;
        }
        return state;
    }

    /**
     * Set the last commit record for a session.
     *
     * @param sessionId the session id
     * @param logId the log file id
     * @param pos the position in the log file
     */
    void setLastCommitForSession(int sessionId, int logId, int pos) {
        SessionState state = getOrAddSessionState(sessionId);
        state.lastCommitLog = logId;
        state.lastCommitPos = pos;
        state.inDoubtTransaction = null;
    }

    /**
     * Check if the session contains uncommitted log entries at the given position.
     *
     * @param sessionId the session id
     * @param logId the log file id
     * @param pos the position in the log file
     * @return true if this session contains an uncommitted transaction
     */
    boolean isSessionCommitted(int sessionId, int logId, int pos) {
        Integer key = ObjectUtils.getInteger(sessionId);
        SessionState state = (SessionState) sessionStates.get(key);
        if (state == null) {
            return true;
        }
        return state.isCommitted(logId, pos);
    }

    /**
     * Get the position of the system table head.
     *
     * @return the system table head
     */
    public int getSystemTableHeadPos() throws SQLException {
        return systemTableHeadPos;
    }

    /**
     * Redo a change in a table.
     *
     * @param tableId the object id of the table
     * @param row the row
     * @param add true if the record is added, false if deleted
     */
    void redo(int tableId, Row row, boolean add) throws SQLException {
        if (tableId == META_TABLE_ID) {
            if (add) {
                addMeta(row, database.getSystemSession());
            } else {
                removeMeta(row);
            }
        }
        PageScanIndex index = (PageScanIndex) metaObjects.get(ObjectUtils.getInteger(tableId));
        if (index == null) {
            throw Message.throwInternalError("Table not found: " + tableId + " " + row + " " + add);
        }
        Table table = index.getTable();
        if (add) {
            table.addRow(database.getSystemSession(), row);
        } else {
            table.removeRow(database.getSystemSession(), row);
        }
    }

    private void openMetaIndex() throws SQLException {
        ObjectArray cols = ObjectArray.newInstance();
        cols.add(new Column("ID", Value.INT));
        cols.add(new Column("TYPE", Value.INT));
        cols.add(new Column("PARENT", Value.INT));
        cols.add(new Column("HEAD", Value.INT));
        cols.add(new Column("COLUMNS", Value.STRING));
//        new CompareMode()
        metaSchema = new Schema(database, 0, "", null, true);
        int headPos = metaTableRootPageId;
        metaTable = new TableData(metaSchema, "PAGE_INDEX",
                META_TABLE_ID, cols, true, true, false, headPos, database.getSystemSession());
        metaIndex = (PageScanIndex) metaTable.getScanIndex(
                database.getSystemSession());
        metaObjects = new HashMap();
        metaObjects.put(ObjectUtils.getInteger(-1), metaIndex);
    }

    private void readMetaData() throws SQLException {
        Cursor cursor = metaIndex.find(database.getSystemSession(), null, null);
        while (cursor.next()) {
            Row row = cursor.get();
            addMeta(row, database.getSystemSession());
        }
    }

    private void removeMeta(Row row) throws SQLException {
        int id = row.getValue(0).getInt();
        Index index = (Index) metaObjects.remove(ObjectUtils.getInteger(id));
        index.getTable().removeIndex(index);
        if (index instanceof PageBtreeIndex) {
            index.getSchema().remove(index);
        }
    }

    private void addMeta(Row row, Session session) throws SQLException {
        int id = row.getValue(0).getInt();
        int type = row.getValue(1).getInt();
        int parent = row.getValue(2).getInt();
        int headPos = row.getValue(3).getInt();
        String columnList = row.getValue(4).getString();
        String[] columns = StringUtils.arraySplit(columnList, ',', false);
        IndexType indexType = IndexType.createNonUnique(true);
        Index meta;
        if (trace.isDebugEnabled()) {
            trace.debug("addMeta id=" + id + " type=" + type + " parent=" + parent + " columns=" + columnList);
        }
        if (type == META_TYPE_SCAN_INDEX) {
            ObjectArray columnArray = ObjectArray.newInstance();
            for (int i = 0; i < columns.length; i++) {
                Column col = new Column("C" + i, Value.INT);
                columnArray.add(col);
            }
            TableData table = new TableData(metaSchema, "T" + id, id, columnArray, true, true, false, headPos, session);
            meta = table.getScanIndex(session);
        } else {
            PageScanIndex p = (PageScanIndex) metaObjects.get(ObjectUtils.getInteger(parent));
            if (p == null) {
                throw Message.throwInternalError("parent not found:" + parent);
            }
            TableData table = (TableData) p.getTable();
            Column[] tableCols = table.getColumns();
            Column[] cols = new Column[columns.length];
            for (int i = 0; i < columns.length; i++) {
                cols[i] = tableCols[Integer.parseInt(columns[i])];
            }
            IndexColumn[] indexColumns = IndexColumn.wrap(cols);
            meta = table.addIndex(session, "I" + id, id, indexColumns, indexType, headPos, null);
        }
        metaObjects.put(ObjectUtils.getInteger(id), meta);
    }

    /**
     * Add the meta data of an index.
     *
     * @param index the index to add
     * @param session the session
     */
    public void addMeta(Index index, Session session) throws SQLException {
        int type = index instanceof PageScanIndex ? META_TYPE_SCAN_INDEX : META_TYPE_BTREE_INDEX;
        Column[] columns = index.getColumns();
        String[] columnIndexes = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {
            columnIndexes[i] = String.valueOf(columns[i].getColumnId());
        }
        String columnList = StringUtils.arrayCombine(columnIndexes, ',');
        addMeta(index.getId(), type, index.getTable().getId(), index.getHeadPos(), columnList, session);
    }

    private void addMeta(int id, int type, int parent, int headPos, String columnList, Session session) throws SQLException {
        Row row = metaTable.getTemplateRow();
        row.setValue(0, ValueInt.get(id));
        row.setValue(1, ValueInt.get(type));
        row.setValue(2, ValueInt.get(parent));
        row.setValue(3, ValueInt.get(headPos));
        row.setValue(4, ValueString.get(columnList));
        row.setPos(id + 1);
        metaIndex.add(session, row);
    }

    /**
     * Remove the meta data of an index.
     *
     * @param index the index to remove
     * @param session the session
     */
    public void removeMeta(Index index, Session session) throws SQLException {
        Row row = metaIndex.getRow(session, index.getId() + 1);
        metaIndex.remove(session, row);
    }

}
