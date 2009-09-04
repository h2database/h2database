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
import java.util.zip.CRC32;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.PageBtreeIndex;
import org.h2.index.PageBtreeLeaf;
import org.h2.index.PageBtreeNode;
import org.h2.index.PageDataLeaf;
import org.h2.index.PageDataNode;
import org.h2.index.PageDataOverflow;
import org.h2.index.PageIndex;
import org.h2.index.PageScanIndex;
import org.h2.log.InDoubtTransaction;
import org.h2.log.LogSystem;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.util.BitField;
import org.h2.util.Cache;
import org.h2.util.CacheLRU;
import org.h2.util.CacheObject;
import org.h2.util.CacheWriter;
import org.h2.util.FileUtils;
import org.h2.util.IntArray;
import org.h2.util.IntIntHashMap;
import org.h2.util.New;
import org.h2.util.ObjectArray;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueString;

/**
 * This class represents a file that is organized as a number of pages. Page 0
 * contains a static file header, and pages 1 and 2 both contain the variable
 * file header (page 2 is a copy of page 1 and is only read if the checksum of
 * page 1 is invalid). The format of page 0 is:
 * <ul>
 * <li>0-47: file header (3 time "-- H2 0.5/B -- \n")</li>
 * <li>48-51: page size in bytes (512 - 32768, must be a power of 2)</li>
 * <li>52: write version (if not 0 the file is opened in read-only mode)</li>
 * <li>53: read version (if not 0 opening the file fails)</li>
 * </ul>
 * The format of page 1 and 2 is:
 * <ul>
 * <li>0-7: write counter (incremented each time the header changes)</li>
 * <li>8-11: log trunk page (0 for none)</li>
 * <li>12-15: log data page (0 for none)</li>
 * <li>16-23: checksum of bytes 0-15 (CRC32)</li>
 * </ul>
 * Page 3 contains the first free list page.
 * Page 4 contains the meta table root page.
 */
public class PageStore implements CacheWriter {

    // TODO a correctly closed database should not contain log pages
    // TODO shrinking: a way to load pages centrally
    // TODO shrinking: Page.moveTo(int pageId).

    // TODO utf-x: test if it's faster
    // TODO after opening the database, delay writing until required

    // TODO scan index: support long keys, and use var long
    // TODO don't save the direct parent (only root); remove setPageId
    // TODO implement checksum; 0 for empty pages
    // TODO remove parent, use tableId if required
    // TODO replace CRC32
    // TODO optimization: try to avoid allocating a byte array per page
    // TODO optimization: check if calling Data.getValueLen slows things down
    // TODO PageBtreeNode: 4 bytes offset - others use only 2
    // TODO undo pages: don't store the middle zeroes
    // TODO undo pages compression: try http://en.wikipedia.org/wiki/LZJB
    // TODO order pages so that searching for a key only seeks forward
    // TODO completely re-use keys of deleted rows; maybe
    // remember last page with deleted keys (in the root page?),
    // and chain such pages

    // TODO detect circles in linked lists
    // (input stream, free list, extend pages...)
    // at runtime and recovery
    // synchronized correctly (on the index?)
    // TODO remove trace or use isDebugEnabled
    // TODO recover tool: don't re-do uncommitted operations
    // TODO no need to log old page if it was always empty
    // TODO don't store default values (store a special value)
    // TODO maybe split at the last insertion point
    // TODO split files (1 GB max size)
    // TODO add a setting (that can be changed at runtime) to call fsync
    // and delay on each commit
    // TODO PageData and PageBtree addRowTry: try to simplify
    // TODO test running out of disk space (using a special file system)
    // TODO check for file size (exception if not exact size expected)
    // TODO implement missing code for STORE_BTREE_ROWCOUNT (maybe enable)
    // TODO delete: only log the key
    // TODO update: only log the key and changed values
    // TODO store dates differently in Data; test moving db to another timezone
    // TODO online backup using bsdiff
    // TODO trying to insert duplicate key can split a page: not in recovery

    // TODO when removing DiskFile:
    // remove CacheObject.blockCount
    // remove Record.getMemorySize
    // simplify InDoubtTransaction
    // remove parameter in Record.write(DataPage buff)
    // remove Record.getByteCount
    // remove Database.objectIds
    // remove TableData.checkRowCount

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
    public static final int PAGE_SIZE_DEFAULT = 2 * 1024;

    /**
     * Store the rowcount in b-tree indexes.
     */
    public static final boolean STORE_BTREE_ROWCOUNT = false;

    private static final int PAGE_ID_FREE_LIST_ROOT = 3;
    private static final int PAGE_ID_META_ROOT = 4;

    private static final int MIN_PAGE_COUNT = 6;

    private static final int INCREMENT_PAGES = 128;

    private static final int READ_VERSION = 0;
    private static final int WRITE_VERSION = 0;

    private static final int META_TYPE_SCAN_INDEX = 0;
    private static final int META_TYPE_BTREE_INDEX = 1;
    private static final int META_TABLE_ID = -1;

    private static final SearchRow[] EMPTY_SEARCH_ROW = new SearchRow[0];

    private Database database;
    private final Trace trace;
    private String fileName;
    private FileStore file;
    private String accessMode;
    private int pageSize;
    private int pageSizeShift;
    private long writeCount;
    private int logFirstTrunkPage, logFirstDataPage;

    private int cacheSize;
    private Cache cache;

    private int freeListPagesPerList;

    private boolean recoveryRunning;

    /**
     * The file size in bytes.
     */
    private long fileLength;

    /**
     * Number of pages (including free pages).
     */
    private int pageCount;

    private PageLog log;

    private Schema metaSchema;
    private TableData metaTable;
    private PageScanIndex metaIndex;
    private IntIntHashMap metaRootPageId = new IntIntHashMap();
    private HashMap<Integer, Index> metaObjects = New.hashMap();

    /**
     * The map of reserved pages, to ensure index head pages
     * are not used for regular data during recovery. The key is the page id,
     * and the value the latest transaction position where this page is used.
     */
    private HashMap<Integer, Integer> reservedPages;
    private int systemTableHeadPos;
    // TODO reduce DEFAULT_MAX_LOG_SIZE, and don't divide here
    private long maxLogSize = Constants.DEFAULT_MAX_LOG_SIZE / 10;
    private Session systemSession;

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
        // int test;
        // trace.setLevel(TraceSystem.DEBUG);
        this.cacheSize = cacheSizeDefault;
        String cacheType = database.getCacheType();
        this.cache = CacheLRU.getCache(this, cacheType, cacheSize);
        systemSession = new Session(database, null, 0);
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
                file.seek((long) pageId << pageSizeShift);
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
            metaRootPageId.put(META_TABLE_ID, PAGE_ID_META_ROOT);
            if (FileUtils.exists(fileName)) {
                if (FileUtils.length(fileName) < MIN_PAGE_COUNT * PAGE_SIZE_MIN) {
                    // the database was not fully created
                    openNew();
                } else {
                    openExisting();
                }
            } else {
                openNew();
            }
            // lastUsedPage = getFreeList().getLastUsed() + 1;
        } catch (SQLException e) {
            close();
            throw e;
        }
    }

    private void openNew() throws SQLException {
        setPageSize(PAGE_SIZE_DEFAULT);
        freeListPagesPerList = PageFreeList.getPagesAddressed(pageSize);
        file = database.openFile(fileName, accessMode, false);
        recoveryRunning = true;
        writeStaticHeader();
        writeVariableHeader();
        log = new PageLog(this);
        increaseFileSize(MIN_PAGE_COUNT);
        openMetaIndex();
        logFirstTrunkPage = allocatePage();
        log.openForWriting(logFirstTrunkPage, false);
        systemTableHeadPos = Index.EMPTY_HEAD;
        recoveryRunning = false;
        increaseFileSize(INCREMENT_PAGES);
    }

    private void openExisting() throws SQLException {
        file = database.openFile(fileName, accessMode, true);
        readStaticHeader();
        freeListPagesPerList = PageFreeList.getPagesAddressed(pageSize);
        fileLength = file.length();
        pageCount = (int) (fileLength / pageSize);
        if (pageCount < MIN_PAGE_COUNT) {
            close();
            openNew();
            return;
        }
        readVariableHeader();
        log = new PageLog(this);
        log.openForReading(logFirstTrunkPage, logFirstDataPage);
        recover();
        if (!database.isReadOnly()) {
            recoveryRunning = true;
            log.free();
            logFirstTrunkPage = allocatePage();
            log.openForWriting(logFirstTrunkPage, false);
            recoveryRunning = false;
            checkpoint();
        }
    }

    private void writeBack() throws SQLException {
        ObjectArray<CacheObject> list = cache.getAllChanged();
        CacheObject.sort(list);
        for (CacheObject rec : list) {
            writeBack(rec);
        }
    }

    /**
     * Flush all pending changes to disk, and re-open the log file.
     */
    public void checkpoint() throws SQLException {
        trace.debug("checkpoint");
        if (log == null || database.isReadOnly()) {
            // the file was never fully opened
            return;
        }
        synchronized (database) {
            database.checkPowerOff();
            writeBack();
            log.checkpoint();
            switchLog();
            // write back the free list
            writeBack();
            byte[] empty = new byte[pageSize];
            // TODO avoid to write empty pages more than once
            for (int i = PAGE_ID_FREE_LIST_ROOT; i < pageCount; i++) {
                if (!isUsed(i)) {
                    file.seek((long) i << pageSizeShift);
                    file.write(empty, 0, pageSize);
                    writeCount++;
                }
            }
        }
    }

    /**
     * Shrink the file so there are no empty pages at the end.
     */
    public void trim() throws SQLException {
        // find the last used page
        int lastUsed = -1;
        for (int i = getFreeListId(pageCount); i >= 0; i--) {
            lastUsed = getFreeList(i).getLastUsed();
            if (lastUsed != -1) {
                break;
            }
        }
        // open a new log at the very end
        // (to be truncated later)
        writeBack();
        recoveryRunning = true;
        try {
            log.free();
            logFirstTrunkPage = lastUsed + 1;
            allocatePage(logFirstTrunkPage);
            log.openForWriting(logFirstTrunkPage, true);
        } finally {
            recoveryRunning = false;
        }
        int maxMove = Integer.MAX_VALUE;
        for (int x = lastUsed, j = 0; x > MIN_PAGE_COUNT && j < maxMove; x--, j++) {
            compact(x);
        }
        writeBack();
        // truncate the log
        recoveryRunning = true;
        try {
            log.free();
            setLogFirstPage(0, 0);
        } finally {
            recoveryRunning = false;
        }
        writeBack();
        for (int i = getFreeListId(pageCount); i >= 0; i--) {
            lastUsed = getFreeList(i).getLastUsed();
            if (lastUsed != -1) {
                break;
            }
        }
        pageCount = lastUsed + 1;
        trace.debug("pageCount:" + pageCount);
        file.setLength((long) pageCount << pageSizeShift);
    }

    private void compact(int full) throws SQLException {
        int free = -1;
        for (int i = 0; i < pageCount; i++) {
            free = getFreeList(i).getFirstFree();
            if (free != -1) {
                break;
            }
        }
        if (free == -1 || free >= full) {
            return;
        }
        Page f = (Page) cache.get(free);
        if (f != null) {
            Message.throwInternalError("not free: " + f);
        }
        if (isUsed(full)) {
            Page p = getPage(full);
            if (p != null) {
                trace.debug("move " + p.getPos() + " to " + free);
                long logSection = log.getLogSectionId(), logPos = log.getLogPos();
                p.moveTo(systemSession, free);
                if (log.getLogSectionId() == logSection || log.getLogPos() != logPos) {
                    commit(systemSession);
                }
            } else {
                freePage(full);
            }
        }
    }

    /**
     * Read a page from the store.
     *
     * @param pageId the page id
     * @return the page
     */
    public Page getPage(int pageId) throws SQLException {
        Record rec = getRecord(pageId);
        if (rec != null) {
            return (Page) rec;
        }
        Data data = Data.create(database, pageSize);
        readPage(pageId, data);
        data.readInt();
        int type = data.readByte();
        Page p;
        switch (type & ~Page.FLAG_LAST) {
        case Page.TYPE_EMPTY:
            return null;
        case Page.TYPE_FREE_LIST:
            p = PageFreeList.read(this, data, pageId);
            break;
        case Page.TYPE_DATA_LEAF: {
            int indexId = data.readInt();
            PageScanIndex index = (PageScanIndex) metaObjects.get(indexId);
            if (index == null) {
                Message.throwInternalError("index not found " + indexId);
            }
            p = PageDataLeaf.read(index, data, pageId);
            break;
        }
        case Page.TYPE_DATA_NODE: {
            int indexId = data.readInt();
            PageScanIndex index = (PageScanIndex) metaObjects.get(indexId);
            if (index == null) {
                Message.throwInternalError("index not found " + indexId);
            }
            p = PageDataNode.read(index, data, pageId);
            break;
        }
        case Page.TYPE_DATA_OVERFLOW: {
            int indexId = data.readInt();
            PageScanIndex index = (PageScanIndex) metaObjects.get(indexId);
            if (index == null) {
                Message.throwInternalError("index not found " + indexId);
            }
            p = PageDataOverflow.read(index, data, pageId);
            break;
        }
        case Page.TYPE_BTREE_LEAF: {
            int indexId = data.readInt();
            PageBtreeIndex index = (PageBtreeIndex) metaObjects.get(indexId);
            if (index == null) {
                Message.throwInternalError("index not found " + indexId);
            }
            p = PageBtreeLeaf.read(index, data, pageId);
            break;
        }
        case Page.TYPE_BTREE_NODE: {
            int indexId = data.readInt();
            PageBtreeIndex index = (PageBtreeIndex) metaObjects.get(indexId);
            if (index == null) {
                Message.throwInternalError("index not found " + indexId);
            }
            p = PageBtreeNode.read(index, data, pageId);
            break;
        }
        case Page.TYPE_STREAM_TRUNK:
            p = PageStreamTrunk.read(this, data, pageId);
            break;
        case Page.TYPE_STREAM_DATA:
            p = PageStreamData.read(this, data, pageId);
            break;
        default:
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, "page=" + pageId + " type=" + type);
        }
        cache.put(p);
        return p;
    }

    private void switchLog() throws SQLException {
        trace.debug("switchLog");
        Session[] sessions = database.getSessions(true);
        int firstUncommittedLog = log.getLogSectionId();
        for (int i = 0; i < sessions.length; i++) {
            Session session = sessions[i];
            int log = session.getFirstUncommittedLog();
            if (log != LogSystem.LOG_WRITTEN) {
                if (log < firstUncommittedLog) {
                    firstUncommittedLog = log;
                }
            }
        }
        log.removeUntil(firstUncommittedLog);
    }

    private void readStaticHeader() throws SQLException {
        long length = file.length();
        database.notifyFileSize(length);
        file.seek(FileStore.HEADER_LENGTH);
        Data page = Data.create(database, new byte[PAGE_SIZE_MIN - FileStore.HEADER_LENGTH]);
        file.readFully(page.getBytes(), 0, PAGE_SIZE_MIN - FileStore.HEADER_LENGTH);
        setPageSize(page.readInt());
        int writeVersion = page.readByte();
        int readVersion = page.readByte();
        if (readVersion != 0) {
            throw Message.getSQLException(ErrorCode.FILE_VERSION_ERROR_1, fileName);
        }
        if (writeVersion != 0) {
            close();
            database.setReadOnly(true);
            accessMode = "r";
            file = database.openFile(fileName, accessMode, true);
        }
    }

    private void readVariableHeader() throws SQLException {
        Data page = Data.create(database, pageSize);
        for (int i = 1;; i++) {
            if (i == 3) {
                throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, fileName);
            }
            page.reset();
            readPage(i, page);
            writeCount = page.readLong();
            logFirstTrunkPage = page.readInt();
            logFirstDataPage = page.readInt();
            CRC32 crc = new CRC32();
            crc.update(page.getBytes(), 0, page.length());
            long expected = crc.getValue();
            long got = page.readLong();
            if (expected == got) {
                break;
            }
        }
    }

    /**
     * Set the page size. The size must be a power of two. This method must be
     * called before opening.
     *
     * @param size the page size
     */
    private void setPageSize(int size) throws SQLException {
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

    private void writeStaticHeader() throws SQLException {
        Data page = Data.create(database, new byte[pageSize - FileStore.HEADER_LENGTH]);
        page.writeInt(pageSize);
        page.writeByte((byte) WRITE_VERSION);
        page.writeByte((byte) READ_VERSION);
        file.seek(FileStore.HEADER_LENGTH);
        file.write(page.getBytes(), 0, pageSize - FileStore.HEADER_LENGTH);
    }

    /**
     * Set the trunk page and data page id of the log.
     *
     * @param trunkPageId the trunk page id
     * @param dataPageId the data page id
     */
    void setLogFirstPage(int trunkPageId, int dataPageId) throws SQLException {
        this.logFirstTrunkPage = trunkPageId;
        this.logFirstDataPage = dataPageId;
        writeVariableHeader();
    }

    private void writeVariableHeader() throws SQLException {
        Data page = Data.create(database, pageSize);
        page.writeLong(writeCount);
        page.writeInt(logFirstTrunkPage);
        page.writeInt(logFirstDataPage);
        CRC32 crc = new CRC32();
        crc.update(page.getBytes(), 0, page.length());
        page.writeLong(crc.getValue());
        file.seek(pageSize);
        file.write(page.getBytes(), 0, pageSize);
        file.seek(pageSize + pageSize);
        file.write(page.getBytes(), 0, pageSize);
        writeCount++;
    }

    /**
     * Close the file without further writing.
     */
    public void close() throws SQLException {
        trace.debug("close");
        if (log != null) {
            log.close();
            log = null;
        }
        if (file != null) {
            try {
                file.close();
            } catch (IOException e) {
                throw Message.convert(e);
            } finally {
                file = null;
            }
        }
    }

    public void flushLog() throws SQLException {
        if (file != null) {
            synchronized (database) {
                log.flush();
            }
        }
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
    public void updateRecord(Record record, boolean logUndo, Data old) throws SQLException {
        synchronized (database) {
            if (trace.isDebugEnabled()) {
                if (!record.isChanged()) {
                    trace.debug("updateRecord " + record.toString());
                }
            }
            checkOpen();
            database.checkWritingAllowed();
            record.setChanged(true);
            int pos = record.getPos();
            allocatePage(pos);
            cache.update(pos, record);
            if (logUndo && !recoveryRunning) {
                if (old == null) {
                    old = readPage(pos);
                }
                log.addUndo(pos, old);
            }
        }
    }

    private int getFreeListId(int pageId) {
        return (pageId - PAGE_ID_FREE_LIST_ROOT) / freeListPagesPerList;
    }

    private PageFreeList getFreeListForPage(int pageId) throws SQLException {
        return getFreeList(getFreeListId(pageId));
    }

    private PageFreeList getFreeList(int i) throws SQLException {
        int p = PAGE_ID_FREE_LIST_ROOT + i * freeListPagesPerList;
        while (p >= pageCount) {
            increaseFileSize(INCREMENT_PAGES);
        }
        PageFreeList list = null;
        if (p < pageCount) {
            list = (PageFreeList) getPage(p);
        }
        if (list == null) {
            list = PageFreeList.create(this, p);
            cache.put(list);
        }
        return list;
    }

    private void freePage(int pageId) throws SQLException {
        PageFreeList list = getFreeListForPage(pageId);
        list.free(pageId);
    }

    /**
     * Set the bit of an already allocated page.
     *
     * @param pageId the page to allocate
     */
    void allocatePage(int pageId) throws SQLException {
        PageFreeList list = getFreeListForPage(pageId);
        list.allocate(pageId);
    }

    private boolean isUsed(int pageId) throws SQLException {
        return getFreeListForPage(pageId).isUsed(pageId);
    }

    /**
     * Allocate a number of pages.
     *
     * @param list the list where to add the allocated pages
     * @param pagesToAllocate the number of pages to allocate
     * @param exclude the exclude list
     * @param after all allocated pages are higher than this page
     */
    void allocatePages(IntArray list, int pagesToAllocate, BitField exclude, int after) throws SQLException {
        for (int i = 0; i < pagesToAllocate; i++) {
            int page = allocatePage(exclude, after);
            after = page;
            list.add(page);
        }
    }

    /**
     * Allocate a page.
     *
     * @return the page id
     */
    public int allocatePage() throws SQLException {
        return allocatePage(null, 0);
    }

    private int allocatePage(BitField exclude, int first) throws SQLException {
        int pos;
        synchronized (database) {
            // TODO could remember the first possible free list page
            for (int i = 0;; i++) {
                PageFreeList list = getFreeList(i);
                pos = list.allocate(exclude, first);
                if (pos >= 0) {
                    break;
                }
            }
            if (pos >= pageCount) {
                increaseFileSize(INCREMENT_PAGES);
            }
            if (trace.isDebugEnabled()) {
                trace.debug("allocatePage " + pos);
            }
            return pos;
        }
    }

    private void increaseFileSize(int increment) throws SQLException {
        pageCount += increment;
        long newLength = (long) pageCount << pageSizeShift;
        file.setLength(newLength);
        writeCount++;
        fileLength = newLength;
    }

    /**
     * Add a page to the free list.
     *
     * @param pageId the page id
     * @param logUndo if an undo entry need to be logged
     * @param old the old data (if known)
     */
    public void freePage(int pageId, boolean logUndo, Data old) throws SQLException {
        if (trace.isDebugEnabled()) {
            trace.debug("freePage " + pageId);
        }
        synchronized (database) {
            cache.remove(pageId);
            freePage(pageId);
            if (recoveryRunning) {
                writePage(pageId, createData());
            } else if (logUndo) {
                if (old == null) {
                    old = readPage(pageId);
                }
                log.addUndo(pageId, old);
            }
        }
    }

    /**
     * Create a data object.
     *
     * @return the data page.
     */
    public Data createData() {
        return Data.create(database, new byte[pageSize]);
    }

    /**
     * Get the record if it is stored in the file, or null if not.
     *
     * @param pos the page id
     * @return the record or null
     */
    public Record getRecord(int pos) {
        synchronized (database) {
            CacheObject obj = cache.find(pos);
            return (Record) obj;
        }
    }

    /**
     * Read a page.
     *
     * @param pos the page id
     * @return the page
     */
    public Data readPage(int pos) throws SQLException {
        Data page = createData();
        readPage(pos, page);
        return page;
    }

    /**
     * Read a page.
     *
     * @param pos the page id
     * @param page the page
     */
    void readPage(int pos, Data page) throws SQLException {
        synchronized (database) {
            if (pos >= pageCount) {
                throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, pos + " of " + pageCount);
            } else if (pos < 0) {
                throw Message.throwInternalError("negative offset: " + pos);
            }
            file.seek((long) pos << pageSizeShift);
            file.readFully(page.getBytes(), 0, pageSize);
        }
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
    public void writePage(int pageId, Data data) throws SQLException {
        synchronized (database) {
            file.seek((long) pageId << pageSizeShift);
            file.write(data.getBytes(), 0, pageSize);
            writeCount++;
        }
    }

    /**
     * Remove a page from the cache.
     *
     * @param pageId the page id
     */
    public void removeRecord(int pageId) {
        synchronized (database) {
            cache.remove(pageId);
        }
    }

    Database getDatabase() {
        return database;
    }

    /**
     * Run recovery.
     */
    private void recover() throws SQLException {
        trace.debug("log recover");
        recoveryRunning = true;
        log.recover(PageLog.RECOVERY_STAGE_UNDO);
        if (reservedPages != null) {
            for (int r : reservedPages.keySet()) {
                allocatePage(r);
            }
        }
        log.recover(PageLog.RECOVERY_STAGE_ALLOCATE);
        openMetaIndex();
        readMetaData();
        log.recover(PageLog.RECOVERY_STAGE_REDO);
        boolean setReadOnly = false;
        if (!database.isReadOnly()) {
            if (log.getInDoubtTransactions().size() == 0) {
                log.recoverEnd();
                switchLog();
            } else {
                setReadOnly = true;
            }
        }
        PageScanIndex systemTable = (PageScanIndex) metaObjects.get(0);
        if (systemTable == null) {
            systemTableHeadPos = Index.EMPTY_HEAD;
        } else {
            systemTableHeadPos = systemTable.getHeadPos();
        }
        for (Index openIndex : metaObjects.values()) {
            if (openIndex.getTable().isTemporary()) {
                openIndex.remove(systemSession);
                this.removeMetaIndex(openIndex, systemSession);
            }
            openIndex.close(systemSession);
        }
        recoveryRunning = false;
        reservedPages = null;
        writeBack();
        // clear the cache because it contains pages with closed indexes
        cache.clear();
        if (setReadOnly) {
            database.setReadOnly(true);
        }
        trace.debug("log recover done");
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
        synchronized (database) {
            if (!recoveryRunning) {
                log.logAddOrRemoveRow(session, tableId, row, add);
            }
        }
    }

    /**
     * Mark a committed transaction.
     *
     * @param session the session
     */
    public void commit(Session session) throws SQLException {
        synchronized (database) {
            checkOpen();
            log.commit(session.getId());
            if (log.getSize() > maxLogSize) {
                checkpoint();
            }
        }
    }

    /**
     * Prepare a transaction.
     *
     * @param session the session
     * @param transaction the name of the transaction
     */
    public void prepareCommit(Session session, String transaction) throws SQLException {
        synchronized (database) {
            log.prepareCommit(session, transaction);
        }
    }

    /**
     * Get the position of the system table head.
     *
     * @return the system table head
     */
    public int getSystemTableHeadPos() {
        return systemTableHeadPos;
    }

    /**
     * Reserve the page if this is a index root page entry.
     *
     * @param logPos the redo log position
     * @param tableId the table id
     * @param row the row
     */
    void allocateIfIndexRoot(int logPos, int tableId, Row row) throws SQLException {
        if (tableId == META_TABLE_ID) {
            int rootPageId = row.getValue(3).getInt();
            if (reservedPages == null) {
                reservedPages = New.hashMap();
            }
            reservedPages.put(rootPageId, logPos);
        }
    }

    /**
     * Redo a change in a table.
     *
     * @param logPos the redo log position
     * @param tableId the object id of the table
     * @param row the row
     * @param add true if the record is added, false if deleted
     */
    void redo(int logPos, int tableId, Row row, boolean add) throws SQLException {
        if (tableId == META_TABLE_ID) {
            if (add) {
                addMeta(row, systemSession, true);
            } else {
                removeMeta(logPos, row);
            }
        }
        Index index = metaObjects.get(tableId);
        if (index == null) {
            throw Message.throwInternalError("Table not found: " + tableId + " " + row + " " + add);
        }
        Table table = index.getTable();
        if (add) {
            table.addRow(systemSession, row);
        } else {
            table.removeRow(systemSession, row);
        }
    }

    /**
     * Redo a truncate.
     *
     * @param tableId the object id of the table
     */
    void redoTruncate(int tableId) throws SQLException {
        Index index = metaObjects.get(tableId);
        Table table = index.getTable();
        table.truncate(systemSession);
    }

    private void openMetaIndex() throws SQLException {
        ObjectArray<Column> cols = ObjectArray.newInstance();
        cols.add(new Column("ID", Value.INT));
        cols.add(new Column("TYPE", Value.INT));
        cols.add(new Column("PARENT", Value.INT));
        cols.add(new Column("HEAD", Value.INT));
        cols.add(new Column("OPTIONS", Value.STRING));
        cols.add(new Column("COLUMNS", Value.STRING));
        metaSchema = new Schema(database, 0, "", null, true);
        metaTable = new TableData(metaSchema, "PAGE_INDEX",
                META_TABLE_ID, cols, false, true, true, false, 0, systemSession);
        metaIndex = (PageScanIndex) metaTable.getScanIndex(
                systemSession);
        metaObjects.clear();
        metaObjects.put(-1, metaIndex);
    }

    private void readMetaData() throws SQLException {
        Cursor cursor = metaIndex.find(systemSession, null, null);
        while (cursor.next()) {
            Row row = cursor.get();
            addMeta(row, systemSession, false);
        }
    }

    private void removeMeta(int logPos, Row row) throws SQLException {
        int id = row.getValue(0).getInt();
        Index index = metaObjects.remove(id);
        int headPos = index.getHeadPos();
        index.getTable().removeIndex(index);
        if (index instanceof PageBtreeIndex) {
            if (index.isTemporary()) {
                systemSession.removeLocalTempTableIndex(index);
            } else {
                index.getSchema().remove(index);
            }
        }
        index.remove(systemSession);
        if (reservedPages != null && reservedPages.containsKey(headPos)) {
            // re-allocate the page if it is used later on again
            int latestPos = reservedPages.get(headPos);
            if (latestPos > logPos) {
                allocatePage(headPos);
            }
        }
    }

    private void addMeta(Row row, Session session, boolean redo) throws SQLException {
        int id = row.getValue(0).getInt();
        int type = row.getValue(1).getInt();
        int parent = row.getValue(2).getInt();
        int rootPageId = row.getValue(3).getInt();
        String options = row.getValue(4).getString();
        String columnList = row.getValue(5).getString();
        String[] columns = StringUtils.arraySplit(columnList, ',', false);
        IndexType indexType = IndexType.createNonUnique(true);
        Index meta;
        if (trace.isDebugEnabled()) {
            trace.debug("addMeta id=" + id + " type=" + type + " parent=" + parent + " columns=" + columnList);
        }
        if (redo) {
            writePage(rootPageId, createData());
            allocatePage(rootPageId);
        }
        metaRootPageId.put(id, rootPageId);
        if (type == META_TYPE_SCAN_INDEX) {
            ObjectArray<Column> columnArray = ObjectArray.newInstance();
            for (int i = 0; i < columns.length; i++) {
                Column col = new Column("C" + i, Value.INT);
                columnArray.add(col);
            }
            String[] ops = StringUtils.arraySplit(options, ',', true);
            boolean temp = ops.length == 3 && ops[2].equals("temp");
            TableData table = new TableData(metaSchema, "T" + id, id, columnArray, temp, true, true, false, 0, session);
            CompareMode mode = CompareMode.getInstance(ops[0], Integer.parseInt(ops[1]));
            table.setCompareMode(mode);
            meta = table.getScanIndex(session);
        } else {
            Index p = metaObjects.get(parent);
            if (p == null) {
                throw Message.throwInternalError("parent not found:" + parent);
            }
            TableData table = (TableData) p.getTable();
            Column[] tableCols = table.getColumns();
            IndexColumn[] cols = new IndexColumn[columns.length];
            for (int i = 0; i < columns.length; i++) {
                String c = columns[i];
                IndexColumn ic = new IndexColumn();
                int idx = c.indexOf('/');
                if (idx >= 0) {
                    String s = c.substring(idx + 1);
                    ic.sortType = Integer.parseInt(s);
                    c = c.substring(0, idx);
                }
                Column column = tableCols[Integer.parseInt(c)];
                ic.column = column;
                cols[i] = ic;
            }
            meta = table.addIndex(session, "I" + id, id, cols, indexType, id, null);
        }
        metaObjects.put(id, meta);
    }

    /**
     * Add an index to the in-memory index map.
     *
     * @param index the index
     */
    public void addIndex(PageIndex index) {
        metaObjects.put(index.getId(), index);
    }

    /**
     * Add the meta data of an index.
     *
     * @param index the index to add
     * @param session the session
     */
    public void addMeta(PageIndex index, Session session) throws SQLException {
        int type = index instanceof PageScanIndex ? META_TYPE_SCAN_INDEX : META_TYPE_BTREE_INDEX;
        IndexColumn[] columns = index.getIndexColumns();
        StatementBuilder buff = new StatementBuilder();
        for (IndexColumn col : columns) {
            buff.appendExceptFirst(",");
            int id = col.column.getColumnId();
            buff.append(id);
            int sortType = col.sortType;
            if (sortType != 0) {
                buff.append('/');
                buff.append(sortType);
            }
        }
        String columnList = buff.toString();
        Table table = index.getTable();
        CompareMode mode = table.getCompareMode();
        String options = mode.getName()+ "," + mode.getStrength();
        if (table.isTemporary()) {
            options += ",temp";
        }
        Row row = metaTable.getTemplateRow();
        row.setValue(0, ValueInt.get(index.getId()));
        row.setValue(1, ValueInt.get(type));
        row.setValue(2, ValueInt.get(table.getId()));
        row.setValue(3, ValueInt.get(index.getRootPageId()));
        row.setValue(4, ValueString.get(options));
        row.setValue(5, ValueString.get(columnList));
        row.setPos(index.getId() + 1);
        metaIndex.add(session, row);
    }

    /**
     * Remove the meta data of an index.
     *
     * @param index the index to remove
     * @param session the session
     */
    public void removeMeta(Index index, Session session) throws SQLException {
        if (!recoveryRunning) {
            removeMetaIndex(index, session);
            metaObjects.remove(index.getId());
        }
    }

    private void removeMetaIndex(Index index, Session session) throws SQLException {
        Row row = metaIndex.getRow(session, index.getId() + 1);
        metaIndex.remove(session, row);
    }

    /**
     * Set the maximum log file size in megabytes.
     *
     * @param maxSize the new maximum log file size
     */
    public void setMaxLogSize(long maxSize) {
        this.maxLogSize = maxSize;
    }

    /**
     * Commit or rollback a prepared transaction after opening a database with
     * in-doubt transactions.
     *
     * @param sessionId the session id
     * @param pageId the page where the transaction was prepared
     * @param commit if the transaction should be committed
     */
    public void setInDoubtTransactionState(int sessionId, int pageId, boolean commit) throws SQLException {
        boolean old = database.isReadOnly();
        try {
            database.setReadOnly(false);
            log.setInDoubtTransactionState(sessionId, pageId, commit);
        } finally {
            database.setReadOnly(old);
        }
    }

    /**
     * Get the list of in-doubt transaction.
     *
     * @return the list
     */
    public ObjectArray<InDoubtTransaction> getInDoubtTransactions() {
        return log.getInDoubtTransactions();
    }

    /**
     * Check whether the recovery process is currently running.
     *
     * @return true if it is
     */
    public boolean isRecoveryRunning() {
        return this.recoveryRunning;
    }

    private void checkOpen() throws SQLException {
        if (file == null) {
            throw Message.getSQLException(ErrorCode.SIMULATED_POWER_OFF);
        }
    }

    /**
     * Create an array of SearchRow with the given size.
     *
     * @param entryCount the number of elements
     * @return the array
     */
    public static SearchRow[] newSearchRows(int entryCount) {
        if (entryCount == 0) {
            return EMPTY_SEARCH_ROW;
        }
        return new SearchRow[entryCount];
    }

    /**
     * Get the write count.
     *
     * @return the write count
     */
    public long getWriteCount() {
        return writeCount;
    }

    /**
     * A table is truncated.
     *
     * @param session the session
     * @param tableId the table id
     */
    public void logTruncate(Session session, int tableId) throws SQLException {
        synchronized (database) {
            if (!recoveryRunning) {
                log.logTruncate(session, tableId);
            }
        }
    }

    int getLogFirstTrunkPage() {
        return this.logFirstTrunkPage;
    }

    int getLogFirstDataPage() {
        return this.logFirstDataPage;
    }

    /**
     * Get the root page of an index.
     *
     * @param index the index
     * @return the root page
     */
    public int getRootPageId(PageIndex index) {
        return metaRootPageId.get(index.getId());
    }

    // TODO implement checksum
//    private void updateChecksum(byte[] d, int pos) {
//        int ps = pageSize;
//        int s1 = 255 + (d[0] & 255), s2 = 255 + s1;
//        s2 += s1 += d[1] & 255;
//        s2 += s1 += d[(ps >> 1) - 1] & 255;
//        s2 += s1 += d[ps >> 1] & 255;
//        s2 += s1 += d[ps - 2] & 255;
//        s2 += s1 += d[ps - 1] & 255;
//        d[5] = (byte) (((s1 & 255) + (s1 >> 8)) ^ pos);
//        d[6] = (byte) (((s2 & 255) + (s2 >> 8)) ^ (pos >> 8));
//    }
//
//    private void verifyChecksum(byte[] d, int pos) throws SQLException {
//        int ps = pageSize;
//        int s1 = 255 + (d[0] & 255), s2 = 255 + s1;
//        s2 += s1 += d[1] & 255;
//        s2 += s1 += d[(ps >> 1) - 1] & 255;
//        s2 += s1 += d[ps >> 1] & 255;
//        s2 += s1 += d[ps - 2] & 255;
//        s2 += s1 += d[ps - 1] & 255;
//        if (d[5] != (byte) (((s1 & 255) + (s1 >> 8)) ^ pos)
//                || d[6] != (byte) (((s2 & 255) + (s2 >> 8)) ^ (pos >> 8))) {
//            throw Message.getSQLException(
//                ErrorCode.FILE_CORRUPTED_1, "wrong checksum");
//        }
//    }

}
