/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.zip.CRC32;
import org.h2.command.ddl.CreateTableData;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.MultiVersionIndex;
import org.h2.index.PageBtreeIndex;
import org.h2.index.PageBtreeLeaf;
import org.h2.index.PageBtreeNode;
import org.h2.index.PageDataIndex;
import org.h2.index.PageDataLeaf;
import org.h2.index.PageDataNode;
import org.h2.index.PageDataOverflow;
import org.h2.index.PageDelegateIndex;
import org.h2.index.PageIndex;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.RegularTable;
import org.h2.table.Table;
import org.h2.util.Cache;
import org.h2.util.CacheLRU;
import org.h2.util.CacheObject;
import org.h2.util.CacheWriter;
import org.h2.util.IOUtils;
import org.h2.util.IntArray;
import org.h2.util.IntIntHashMap;
import org.h2.util.New;
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
 * <li>52: write version (read-only if larger than 1)</li>
 * <li>53: read version (opening fails if larger than 1)</li>
 * </ul>
 * The format of page 1 and 2 is:
 * <ul>
 * <li>CRC32 of the remaining data: int (0-3)</li>
 * <li>write counter (incremented on each write): long (4-11)</li>
 * <li>log trunk key: int (12-15)</li>
 * <li>log trunk page (0 for none): int (16-19)</li>
 * <li>log data page (0 for none): int (20-23)</li>
 * </ul>
 * Page 3 contains the first free list page.
 * Page 4 contains the meta table root page.
 */
public class PageStore implements CacheWriter {

    // TODO test running out of disk space (using a special file system)
    // TODO test with recovery being the default method
    // TODO test reservedPages does not grow unbound

    // TODO utf-x: test if it's faster
    // TODO corrupt pages should be freed once in a while
    // TODO node row counts are incorrect (not splitting row counts)
    // TODO after opening the database, delay writing until required
    // TODO optimization: try to avoid allocating a byte array per page
    // TODO optimization: check if calling Data.getValueLen slows things down
    // TODO order pages so that searching for a key only seeks forward
    // TODO optimization: update: only log the key and changed values
    // TODO index creation: use less space (ordered, split at insertion point)
    // TODO detect circles in linked lists
    // (input stream, free list, extend pages...)
    // at runtime and recovery
    // synchronized correctly (on the index?)
    // TODO remove trace or use isDebugEnabled
    // TODO recover tool: support syntax to delete a row with a key
    // TODO don't store default values (store a special value)
    // TODO split files (1 GB max size)
    // TODO add a setting (that can be changed at runtime) to call fsync
    // and delay on each commit
    // TODO check for file size (exception if not exact size expected)
    // TODO online backup using bsdiff

    // TODO when removing DiskFile:
    // remove CacheObject.blockCount
    // remove Record.getMemorySize
    // simplify InDoubtTransaction
    // remove parameter in Record.write(DataPage buff)
    // remove Record.getByteCount
    // remove Database.objectIds
    // remove TableData.checkRowCount
    // remove Row.setPos
    // remove database URL option RECOVER=1 option
    // remove old database URL options and documentation

    /**
     * The smallest possible page size.
     */
    public static final int PAGE_SIZE_MIN = 64;

    /**
     * The biggest possible page size.
     */
    public static final int PAGE_SIZE_MAX = 32768;

    private static final int PAGE_ID_FREE_LIST_ROOT = 3;
    private static final int PAGE_ID_META_ROOT = 4;

    private static final int MIN_PAGE_COUNT = 6;

    private static final int INCREMENT_PAGES = 128;

    private static final int READ_VERSION = 3;
    private static final int WRITE_VERSION = 3;

    private static final int META_TYPE_DATA_INDEX = 0;
    private static final int META_TYPE_BTREE_INDEX = 1;
    private static final int META_TABLE_ID = -1;

    private static final SearchRow[] EMPTY_SEARCH_ROW = { };

    private Database database;
    private final Trace trace;
    private String fileName;
    private FileStore file;
    private String accessMode;
    private int pageSize = SysProperties.PAGE_SIZE;
    private int pageSizeShift;
    private long writeCountBase, writeCount, readCount;
    private int logKey, logFirstTrunkPage, logFirstDataPage;

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
    private RegularTable metaTable;
    private PageDataIndex metaIndex;
    private IntIntHashMap metaRootPageId = new IntIntHashMap();
    private HashMap<Integer, PageIndex> metaObjects = New.hashMap();
    private HashMap<Integer, PageIndex> tempObjects;

    /**
     * The map of reserved pages, to ensure index head pages
     * are not used for regular data during recovery. The key is the page id,
     * and the value the latest transaction position where this page is used.
     */
    private HashMap<Integer, Integer> reservedPages;
    private boolean isNew;
    private long maxLogSize = Constants.DEFAULT_MAX_LOG_SIZE;
    private Session systemSession;
    private BitSet freed = new BitSet();

    private ArrayList<PageFreeList> freeLists = New.arrayList();

    /**
     * The change count is something like a "micro-transaction-id".
     * It is used to ensure that changed pages are not written to the file
     * before the the current operation is not finished. This is only a problem
     * when using a very small cache size. The value starts at 1 so that
     * pages with change count 0 can be evicted from the cache.
     */
    private int changeCount = 1;

    private Data emptyPage;

    private long logSizeBase;

    /**
     * Create a new page store object.
     *
     * @param database the database
     * @param fileName the file name
     * @param accessMode the access mode
     * @param cacheSizeDefault the default cache size
     */
    public PageStore(Database database, String fileName, String accessMode, int cacheSizeDefault) {
        this.fileName = fileName;
        this.accessMode = accessMode;
        this.database = database;
        trace = database.getTrace(Trace.PAGE_STORE);
        // if (!fileName.endsWith("reopen.h2.db"))
        // trace.setLevel(TraceSystem.DEBUG);
        String cacheType = database.getCacheType();
        this.cache = CacheLRU.getCache(this, cacheType, cacheSizeDefault);
        systemSession = new Session(database, null, 0);
    }

    /**
     * Copy the next page to the output stream.
     *
     * @param pageId the page to copy
     * @param out the output stream
     * @return the new position, or -1 if there is no more data to copy
     */
    public int copyDirect(int pageId, OutputStream out) throws IOException {
        synchronized (database) {
            byte[] buffer = new byte[pageSize];
            if (pageId >= pageCount) {
                return -1;
            }
            file.seek((long) pageId << pageSizeShift);
            file.readFullyDirect(buffer, 0, pageSize);
            readCount++;
            out.write(buffer, 0, pageSize);
            return pageId + 1;
        }
    }

    /**
     * Open the file and read the header.
     */
    public void open() {
        try {
            metaRootPageId.put(META_TABLE_ID, PAGE_ID_META_ROOT);
            if (IOUtils.exists(fileName)) {
                long length = IOUtils.length(fileName);
                if (length < MIN_PAGE_COUNT * PAGE_SIZE_MIN) {
                    if (database.isReadOnly()) {
                        throw DbException.get(ErrorCode.FILE_CORRUPTED_1, fileName + " length: " + length);
                    }
                    // the database was not fully created
                    openNew();
                } else {
                    openExisting();
                }
            } else {
                openNew();
            }
        } catch (DbException e) {
            close();
            throw e;
        }
    }

    private void openNew() {
        setPageSize(pageSize);
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
        isNew = true;
        recoveryRunning = false;
        increaseFileSize(INCREMENT_PAGES);
    }

    private void openExisting() {
        file = database.openFile(fileName, accessMode, true);
        readStaticHeader();
        freeListPagesPerList = PageFreeList.getPagesAddressed(pageSize);
        fileLength = file.length();
        pageCount = (int) (fileLength / pageSize);
        if (pageCount < MIN_PAGE_COUNT) {
            if (database.isReadOnly()) {
                throw DbException.get(ErrorCode.FILE_CORRUPTED_1, fileName + " pageCount: " + pageCount);
            }
            file.close();
            IOUtils.delete(fileName);
            openNew();
            return;
        }
        readVariableHeader();
        log = new PageLog(this);
        log.openForReading(logKey, logFirstTrunkPage, logFirstDataPage);
        recover();
        if (!database.isReadOnly()) {
            recoveryRunning = true;
            log.free();
            logFirstTrunkPage = allocatePage();
            log.openForWriting(logFirstTrunkPage, false);
            recoveryRunning = false;
            checkpoint();
            removeOldTempIndexes();
        }
    }

    private void removeOldTempIndexes() {
        if (tempObjects != null) {
            metaObjects.putAll(tempObjects);
            for (PageIndex index: tempObjects.values()) {
                if (index.getTable().isTemporary()) {
                    index.truncate(systemSession);
                    index.remove(systemSession);
                }
            }
            systemSession.commit(true);
            tempObjects = null;
        }
        metaObjects.clear();
        metaObjects.put(-1, metaIndex);
    }

    private void writeIndexRowCounts() {
        for (PageIndex index: metaObjects.values()) {
            index.writeRowCount();
        }
    }

    private void writeBack() {
        ArrayList<CacheObject> list = cache.getAllChanged();
        Collections.sort(list);
        for (CacheObject rec : list) {
            writeBack(rec);
        }
    }

    /**
     * Flush all pending changes to disk, and switch the new transaction log.
     */
    public void checkpoint() {
        trace.debug("checkpoint");
        if (log == null || database.isReadOnly()) {
            // the file was never fully opened
            return;
        }
        synchronized (database) {
            database.checkPowerOff();
            writeIndexRowCounts();

            log.checkpoint();
            writeBack();

            int firstUncommittedSection = getFirstUncommittedSection();

            log.removeUntil(firstUncommittedSection);

            // write back the free list
            writeBack();

            // ensure the free list is backed up again
            log.checkpoint();


            byte[] empty = new byte[pageSize];
            for (int i = PAGE_ID_FREE_LIST_ROOT; i < pageCount; i++) {
                if (isUsed(i)) {
                    freed.clear(i);
                } else if (!freed.get(i)) {
                    if (trace.isDebugEnabled()) {
                        trace.debug("free " + i);
                    }
                    freed.set(i);
                    file.seek((long) i << pageSizeShift);
                    file.write(empty, 0, pageSize);
                    writeCount++;
                }
            }
        }
    }

    /**
     * Shrink the file so there are no empty pages at the end.
     *
     * @param fully if the database should be fully compressed
     */
    public void compact(boolean fully) {
        if (!SysProperties.PAGE_STORE_TRIM) {
            return;
        }
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

            // ensure the free list is backed up again
            log.checkpoint();

        } finally {
            recoveryRunning = false;
        }
        long start = System.currentTimeMillis();
        int maxCompactTime = SysProperties.MAX_COMPACT_TIME;
        int maxMove = SysProperties.MAX_COMPACT_COUNT;
        if (fully) {
            maxCompactTime = Integer.MAX_VALUE;
            maxMove = Integer.MAX_VALUE;
        }
        for (int x = lastUsed, j = 0; x > MIN_PAGE_COUNT && j < maxMove; x--, j++) {
            compact(x);
            long now = System.currentTimeMillis();
            if (now > start + maxCompactTime) {
                break;
            }
        }

        // TODO can most likely be simplified
        checkpoint();
        log.checkpoint();
        writeIndexRowCounts();
        log.checkpoint();
        writeBack();
        commit(systemSession);
        writeBack();
        log.checkpoint();

        // truncate the log
        recoveryRunning = true;
        try {
            log.free();
            setLogFirstPage(0, 0, 0);
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
        int newPageCount = lastUsed + 1;
        if (newPageCount < pageCount) {
            freed.set(newPageCount, pageCount, false);
        }
        pageCount = newPageCount;
        // the easiest way to remove superfluous entries
        freeLists.clear();
        trace.debug("pageCount:" + pageCount);
        long newLength = (long) pageCount << pageSizeShift;
        if (file.length() != newLength) {
            file.setLength(newLength);
            writeCount++;
        }
    }

    private void compact(int full) {
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
            DbException.throwInternalError("not free: " + f);
        }
        if (isUsed(full)) {
            Page p = getPage(full);
            if (p != null) {
                trace.debug("move " + p.getPos() + " to " + free);
                try {
                    p.moveTo(systemSession, free);
                } finally {
                    changeCount++;
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
    public Page getPage(int pageId) {
        synchronized (database) {
            Page p = (Page) cache.find(pageId);
            if (p != null) {
                return p;
            }
        }
        Data data = createData();
        readPage(pageId, data);
        int type = data.readByte();
        if (type == Page.TYPE_EMPTY) {
            return null;
        }
        data.readShortInt();
        data.readInt();
        if (!checksumTest(data.getBytes(), pageId, pageSize)) {
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "wrong checksum");
        }
        Page p;
        switch (type & ~Page.FLAG_LAST) {
        case Page.TYPE_FREE_LIST:
            p = PageFreeList.read(this, data, pageId);
            break;
        case Page.TYPE_DATA_LEAF: {
            int indexId = data.readVarInt();
            PageDataIndex index = (PageDataIndex) metaObjects.get(indexId);
            if (index == null) {
                throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "index not found " + indexId);
            }
            p = PageDataLeaf.read(index, data, pageId);
            break;
        }
        case Page.TYPE_DATA_NODE: {
            int indexId = data.readVarInt();
            PageDataIndex index = (PageDataIndex) metaObjects.get(indexId);
            if (index == null) {
                throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "index not found " + indexId);
            }
            p = PageDataNode.read(index, data, pageId);
            break;
        }
        case Page.TYPE_DATA_OVERFLOW: {
            p = PageDataOverflow.read(this, data, pageId);
            break;
        }
        case Page.TYPE_BTREE_LEAF: {
            int indexId = data.readVarInt();
            PageBtreeIndex index = (PageBtreeIndex) metaObjects.get(indexId);
            if (index == null) {
                throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "index not found " + indexId);
            }
            p = PageBtreeLeaf.read(index, data, pageId);
            break;
        }
        case Page.TYPE_BTREE_NODE: {
            int indexId = data.readVarInt();
            PageBtreeIndex index = (PageBtreeIndex) metaObjects.get(indexId);
            if (index == null) {
                throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "index not found " + indexId);
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
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "page=" + pageId + " type=" + type);
        }
        cache.put(p);
        return p;
    }

    private int getFirstUncommittedSection() {
        trace.debug("getFirstUncommittedSection");
        Session[] sessions = database.getSessions(true);
        int firstUncommittedSection = log.getLogSectionId();
        for (int i = 0; i < sessions.length; i++) {
            Session session = sessions[i];
            int firstUncommitted = session.getFirstUncommittedLog();
            if (firstUncommitted != Session.LOG_WRITTEN) {
                if (firstUncommitted < firstUncommittedSection) {
                    firstUncommittedSection = firstUncommitted;
                }
            }
        }
        return firstUncommittedSection;
    }

    private void readStaticHeader() {
        file.seek(FileStore.HEADER_LENGTH);
        Data page = Data.create(database, new byte[PAGE_SIZE_MIN - FileStore.HEADER_LENGTH]);
        file.readFully(page.getBytes(), 0, PAGE_SIZE_MIN - FileStore.HEADER_LENGTH);
        readCount++;
        setPageSize(page.readInt());
        int writeVersion = page.readByte();
        int readVersion = page.readByte();
        if (readVersion > READ_VERSION) {
            throw DbException.get(ErrorCode.FILE_VERSION_ERROR_1, fileName);
        }
        if (writeVersion > WRITE_VERSION) {
            close();
            database.setReadOnly(true);
            accessMode = "r";
            file = database.openFile(fileName, accessMode, true);
        }
    }

    private void readVariableHeader() {
        Data page = createData();
        for (int i = 1;; i++) {
            if (i == 3) {
                throw DbException.get(ErrorCode.FILE_CORRUPTED_1, fileName);
            }
            page.reset();
            readPage(i, page);
            CRC32 crc = new CRC32();
            crc.update(page.getBytes(), 4, pageSize - 4);
            int expected = (int) crc.getValue();
            int got = page.readInt();
            if (expected == got) {
                writeCountBase = page.readLong();
                logKey = page.readInt();
                logFirstTrunkPage = page.readInt();
                logFirstDataPage = page.readInt();
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
    public void setPageSize(int size) {
        if (size < PAGE_SIZE_MIN || size > PAGE_SIZE_MAX) {
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, fileName + " pageSize: " + size);
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
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, fileName);
        }
        pageSize = size;
        emptyPage = createData();
        pageSizeShift = shift;
    }

    private void writeStaticHeader() {
        Data page = Data.create(database, new byte[pageSize - FileStore.HEADER_LENGTH]);
        page.writeInt(pageSize);
        page.writeByte((byte) WRITE_VERSION);
        page.writeByte((byte) READ_VERSION);
        file.seek(FileStore.HEADER_LENGTH);
        file.write(page.getBytes(), 0, pageSize - FileStore.HEADER_LENGTH);
        writeCount++;
    }

    /**
     * Set the trunk page and data page id of the log.
     *
     * @param logKey the log key of the trunk page
     * @param trunkPageId the trunk page id
     * @param dataPageId the data page id
     */
    void setLogFirstPage(int logKey, int trunkPageId, int dataPageId) {
        if (trace.isDebugEnabled()) {
            trace.debug("setLogFirstPage key: " + logKey + " trunk: " + trunkPageId + " data: " + dataPageId);
        }
        this.logKey = logKey;
        this.logFirstTrunkPage = trunkPageId;
        this.logFirstDataPage = dataPageId;
        writeVariableHeader();
    }

    private void writeVariableHeader() {
        trace.debug("writeVariableHeader");
        file.sync();
        Data page = createData();
        page.writeInt(0);
        page.writeLong(getWriteCountTotal());
        page.writeInt(logKey);
        page.writeInt(logFirstTrunkPage);
        page.writeInt(logFirstDataPage);
        CRC32 crc = new CRC32();
        crc.update(page.getBytes(), 4, pageSize - 4);
        page.setInt(0, (int) crc.getValue());
        file.seek(pageSize);
        file.write(page.getBytes(), 0, pageSize);
        file.seek(pageSize + pageSize);
        file.write(page.getBytes(), 0, pageSize);
        // don't increment the write counter, because it was just written
    }

    /**
     * Close the file without further writing.
     */
    public void close() {
        trace.debug("close");
        if (log != null) {
            log.close();
            log = null;
        }
        if (file != null) {
            try {
                file.close();
            } finally {
                file = null;
            }
        }
    }

    public void flushLog() {
        if (file != null) {
            synchronized (database) {
                log.flush();
            }
        }
    }

    /**
     * Flush the transaction log and sync the file.
     */
    public void sync() {
        if (file != null) {
            synchronized (database) {
                log.flush();
                file.sync();
            }
        }
    }

    public Trace getTrace() {
        return trace;
    }

    public void writeBack(CacheObject obj) {
        synchronized (database) {
            Page record = (Page) obj;
            if (trace.isDebugEnabled()) {
                trace.debug("writeBack " + record);
            }
            record.write();
            record.setChanged(false);
        }
    }

    /**
     * Write an undo log entry if required.
     *
     * @param page the page
     * @param old the old data (if known) or null
     */
    public void logUndo(Page page, Data old) {
        synchronized (database) {
            if (trace.isDebugEnabled()) {
                // if (!record.isChanged()) {
                //     trace.debug("logUndo " + record.toString());
                // }
            }
            checkOpen();
            database.checkWritingAllowed();
            if (!recoveryRunning) {
                int pos = page.getPos();
                if (!log.getUndo(pos)) {
                    if (old == null) {
                        old = readPage(pos);
                    }
                    log.addUndo(pos, old);
                }
            }
        }
    }

    /**
     * Update a page.
     *
     * @param page the page
     */
    public void update(Page page) {
        synchronized (database) {
            if (trace.isDebugEnabled()) {
                if (!page.isChanged()) {
                    trace.debug("updateRecord " + page.toString());
                }
            }
            checkOpen();
            database.checkWritingAllowed();
            page.setChanged(true);
            int pos = page.getPos();
            if (SysProperties.CHECK && !recoveryRunning) {
                // ensure the undo entry is already written
                log.addUndo(pos, null);
            }
            allocatePage(pos);
            cache.update(pos, page);
        }
    }

    private int getFreeListId(int pageId) {
        return (pageId - PAGE_ID_FREE_LIST_ROOT) / freeListPagesPerList;
    }

    private PageFreeList getFreeListForPage(int pageId) {
        return getFreeList(getFreeListId(pageId));
    }

    private PageFreeList getFreeList(int i) {
        PageFreeList list = null;
        if (i < freeLists.size()) {
            list = freeLists.get(i);
            if (list != null) {
                return list;
            }
        }
        int p = PAGE_ID_FREE_LIST_ROOT + i * freeListPagesPerList;
        while (p >= pageCount) {
            increaseFileSize(INCREMENT_PAGES);
        }
        if (p < pageCount) {
            list = (PageFreeList) getPage(p);
        }
        if (list == null) {
            list = PageFreeList.create(this, p);
            cache.put(list);
        }
        while (freeLists.size() <= i) {
            freeLists.add(null);
        }
        freeLists.set(i, list);
        return list;
    }

    private void freePage(int pageId) {
        PageFreeList list = getFreeListForPage(pageId);
        list.free(pageId);
    }

    /**
     * Set the bit of an already allocated page.
     *
     * @param pageId the page to allocate
     */
    void allocatePage(int pageId) {
        PageFreeList list = getFreeListForPage(pageId);
        list.allocate(pageId);
    }

    private boolean isUsed(int pageId) {
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
    void allocatePages(IntArray list, int pagesToAllocate, BitSet exclude, int after) {
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
    public int allocatePage() {
        int pos = allocatePage(null, 0);
        if (!recoveryRunning) {
            log.addUndo(pos, emptyPage);
        }
        return pos;
    }

    private int allocatePage(BitSet exclude, int first) {
        int page;
        synchronized (database) {
            // TODO could remember the first possible free list page
            for (int i = 0;; i++) {
                PageFreeList list = getFreeList(i);
                page = list.allocate(exclude, first);
                if (page >= 0) {
                    break;
                }
            }
            if (page >= pageCount) {
                increaseFileSize(INCREMENT_PAGES);
            }
            if (trace.isDebugEnabled()) {
                // trace.debug("allocatePage " + pos);
            }
            return page;
        }
    }

    private void increaseFileSize(int increment) {
        for (int i = pageCount; i < pageCount + increment; i++) {
            freed.set(i);
        }
        pageCount += increment;
        long newLength = (long) pageCount << pageSizeShift;
        file.setLength(newLength);
        writeCount++;
        fileLength = newLength;
    }

    /**
     * Add a page to the free list. The undo log entry must have been written.
     *
     * @param pageId the page id
     */
    public void free(int pageId) {
        free(pageId, true);
    }

    /**
     * Add a page to the free list.
     *
     * @param pageId the page id
     * @param undo if the undo record must have been written
     */
    void free(int pageId, boolean undo) {
        if (trace.isDebugEnabled()) {
            // trace.debug("freePage " + pageId);
        }
        synchronized (database) {
            cache.remove(pageId);
            if (SysProperties.CHECK && !recoveryRunning && undo) {
                // ensure the undo entry is already written
                log.addUndo(pageId, null);
            }
            freePage(pageId);
            if (recoveryRunning) {
                writePage(pageId, createData());
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
     * Read a page.
     *
     * @param pos the page id
     * @return the page
     */
    public Data readPage(int pos) {
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
    void readPage(int pos, Data page) {
        synchronized (database) {
            if (pos < 0 || pos >= pageCount) {
                throw DbException.get(ErrorCode.FILE_CORRUPTED_1, pos + " of " + pageCount);
            }
            file.seek((long) pos << pageSizeShift);
            file.readFully(page.getBytes(), 0, pageSize);
            readCount++;
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
    public void writePage(int pageId, Data data) {
        if (pageId <= 0) {
            DbException.throwInternalError("write to page " + pageId);
        }
        byte[] bytes = data.getBytes();
        if (SysProperties.CHECK) {
            boolean shouldBeFreeList = (pageId - PAGE_ID_FREE_LIST_ROOT) % freeListPagesPerList == 0;
            boolean isFreeList = bytes[0] == Page.TYPE_FREE_LIST;
            if (bytes[0] != 0 && shouldBeFreeList != isFreeList) {
                throw DbException.throwInternalError();
            }
        }
        checksumSet(bytes, pageId);
        synchronized (database) {
            file.seek((long) pageId << pageSizeShift);
            file.write(bytes, 0, pageSize);
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
    private void recover() {
        trace.debug("log recover");
        recoveryRunning = true;
        log.recover(PageLog.RECOVERY_STAGE_UNDO);
        if (reservedPages != null) {
            for (int r : reservedPages.keySet()) {
                if (trace.isDebugEnabled()) {
                    trace.debug("reserve " + r);
                }
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
                int firstUncommittedSection = getFirstUncommittedSection();
                log.removeUntil(firstUncommittedSection);
            } else {
                setReadOnly = true;
            }
        }
        PageDataIndex systemTable = (PageDataIndex) metaObjects.get(0);
        isNew = systemTable == null;
        for (PageIndex index : metaObjects.values()) {
            if (index.getTable().isTemporary()) {
                // temporary indexes are removed after opening
                if (tempObjects == null) {
                    tempObjects = New.hashMap();
                }
                tempObjects.put(index.getId(), index);
            } else {
                index.close(systemSession);
            }
        }

        allocatePage(PAGE_ID_META_ROOT);
        writeIndexRowCounts();
        recoveryRunning = false;
        reservedPages = null;

        writeBack();
        // clear the cache because it contains pages with closed indexes
        cache.clear();
        freeLists.clear();

        metaObjects.clear();
        metaObjects.put(-1, metaIndex);

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
    public void logAddOrRemoveRow(Session session, int tableId, Row row, boolean add) {
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
    public void commit(Session session) {
        synchronized (database) {
            checkOpen();
            log.commit(session.getId());
            if (log.getSize() - logSizeBase > maxLogSize) {
                checkpoint();
                logSizeBase = log.getSize();
            }
        }
    }

    /**
     * Prepare a transaction.
     *
     * @param session the session
     * @param transaction the name of the transaction
     */
    public void prepareCommit(Session session, String transaction) {
        synchronized (database) {
            log.prepareCommit(session, transaction);
        }
    }

    /**
     * Check whether this is a new database.
     *
     * @return true if it is
     */
    public boolean isNew() {
        return isNew;
    }

    /**
     * Reserve the page if this is a index root page entry.
     *
     * @param logPos the redo log position
     * @param tableId the table id
     * @param row the row
     */
    void allocateIfIndexRoot(int logPos, int tableId, Row row) {
        if (tableId == META_TABLE_ID) {
            int rootPageId = row.getValue(3).getInt();
            if (reservedPages == null) {
                reservedPages = New.hashMap();
            }
            reservedPages.put(rootPageId, logPos);
        }
    }

    /**
     * Redo a delete in a table.
     *
     * @param logPos the redo log position
     * @param tableId the object id of the table
     * @param key the key of the row to delete
     */
    void redoDelete(int logPos, int tableId, long key) {
        Index index = metaObjects.get(tableId);
        PageDataIndex scan = (PageDataIndex) index;
        Row row = scan.getRow(key);
        redo(logPos, tableId, row, false);
    }

    /**
     * Redo a change in a table.
     *
     * @param logPos the redo log position
     * @param tableId the object id of the table
     * @param row the row
     * @param add true if the record is added, false if deleted
     */
    void redo(int logPos, int tableId, Row row, boolean add) {
        if (tableId == META_TABLE_ID) {
            if (add) {
                addMeta(row, systemSession, true);
            } else {
                removeMeta(logPos, row);
            }
        }
        Index index = metaObjects.get(tableId);
        if (index == null) {
            throw DbException.throwInternalError("Table not found: " + tableId + " " + row + " " + add);
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
    void redoTruncate(int tableId) {
        Index index = metaObjects.get(tableId);
        Table table = index.getTable();
        table.truncate(systemSession);
    }

    private void openMetaIndex() {
        CreateTableData data = new CreateTableData();
        ArrayList<Column> cols = data.columns;
        cols.add(new Column("ID", Value.INT));
        cols.add(new Column("TYPE", Value.INT));
        cols.add(new Column("PARENT", Value.INT));
        cols.add(new Column("HEAD", Value.INT));
        cols.add(new Column("OPTIONS", Value.STRING));
        cols.add(new Column("COLUMNS", Value.STRING));
        metaSchema = new Schema(database, 0, "", null, true);
        data.schema = metaSchema;
        data.tableName = "PAGE_INDEX";
        data.id = META_TABLE_ID;
        data.temporary = false;
        data.persistData = true;
        data.persistIndexes = true;
        data.create = false;
        data.session = systemSession;
        metaTable = new RegularTable(data);
        metaIndex = (PageDataIndex) metaTable.getScanIndex(
                systemSession);
        metaObjects.clear();
        metaObjects.put(-1, metaIndex);
    }

    private void readMetaData() {
        Cursor cursor = metaIndex.find(systemSession, null, null);
        // first, create all tables
        while (cursor.next()) {
            Row row = cursor.get();
            int type = row.getValue(1).getInt();
            if (type == META_TYPE_DATA_INDEX) {
                addMeta(row, systemSession, false);
            }
        }
        // now create all secondary indexes
        // otherwise the table might not be created yet
        cursor = metaIndex.find(systemSession, null, null);
        while (cursor.next()) {
            Row row = cursor.get();
            int type = row.getValue(1).getInt();
            if (type != META_TYPE_DATA_INDEX) {
                addMeta(row, systemSession, false);
            }
        }
    }

    private void removeMeta(int logPos, Row row) {
        int id = row.getValue(0).getInt();
        PageIndex index = metaObjects.get(id);
        int rootPageId = index.getRootPageId();
        index.getTable().removeIndex(index);
        if (index instanceof PageBtreeIndex || index instanceof PageDelegateIndex) {
            if (index.isTemporary()) {
                systemSession.removeLocalTempTableIndex(index);
            } else {
                index.getSchema().remove(index);
            }
        }
        index.remove(systemSession);
        metaObjects.remove(id);
        if (reservedPages != null && reservedPages.containsKey(rootPageId)) {
            // re-allocate the page if it is used later on again
            int latestPos = reservedPages.get(rootPageId);
            if (latestPos > logPos) {
                allocatePage(rootPageId);
            }
        }
    }

    private void addMeta(Row row, Session session, boolean redo) {
        int id = row.getValue(0).getInt();
        int type = row.getValue(1).getInt();
        int parent = row.getValue(2).getInt();
        int rootPageId = row.getValue(3).getInt();
        String options = row.getValue(4).getString();
        String columnList = row.getValue(5).getString();
        String[] columns = StringUtils.arraySplit(columnList, ',', false);
        String[] ops = StringUtils.arraySplit(options, ',', false);
        Index meta;
        if (trace.isDebugEnabled()) {
            trace.debug("addMeta id=" + id + " type=" + type + " root=" + rootPageId + " parent=" + parent + " columns=" + columnList);
        }
        if (redo && rootPageId != 0) {
            // ensure the page is empty, but not used by regular data
            writePage(rootPageId, createData());
            allocatePage(rootPageId);
        }
        metaRootPageId.put(id, rootPageId);
        if (type == META_TYPE_DATA_INDEX) {
            CreateTableData data = new CreateTableData();
            for (int i = 0; i < columns.length; i++) {
                Column col = new Column("C" + i, Value.INT);
                data.columns.add(col);
            }
            data.schema = metaSchema;
            data.tableName = "T" + id;
            data.id = id;
            data.temporary = ops[2].equals("temp");
            data.persistData = true;
            data.persistIndexes = true;
            data.create = false;
            data.session = session;
            RegularTable table = new RegularTable(data);
            CompareMode mode = CompareMode.getInstance(ops[0], Integer.parseInt(ops[1]));
            table.setCompareMode(mode);
            meta = table.getScanIndex(session);
        } else {
            Index p = metaObjects.get(parent);
            if (p == null) {
                throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "Table not found:" + parent + " for " + row + " meta:" + metaObjects);
            }
            RegularTable table = (RegularTable) p.getTable();
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
            IndexType indexType;
            if (ops[3].equals("d")) {
                indexType = IndexType.createPrimaryKey(true, false);
                Column[] tableColumns = table.getColumns();
                for (int i = 0; i < cols.length; i++) {
                    tableColumns[cols[i].column.getColumnId()].setNullable(false);
                }
            } else {
                indexType = IndexType.createNonUnique(true);
            }
            meta = table.addIndex(session, "I" + id, id, cols, indexType, false, null);
        }
        PageIndex index;
        if (meta instanceof MultiVersionIndex) {
            index = (PageIndex) ((MultiVersionIndex) meta).getBaseIndex();
        } else {
            index = (PageIndex) meta;
        }
        metaObjects.put(id, index);
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
    public void addMeta(PageIndex index, Session session) {
        int type = index instanceof PageDataIndex ? META_TYPE_DATA_INDEX : META_TYPE_BTREE_INDEX;
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
        String options = mode.getName()+ "," + mode.getStrength() + ",";
        if (table.isTemporary()) {
            options += "temp";
        }
        options += ",";
        if (index instanceof PageDelegateIndex) {
            options += "d";
        }
        Row row = metaTable.getTemplateRow();
        row.setValue(0, ValueInt.get(index.getId()));
        row.setValue(1, ValueInt.get(type));
        row.setValue(2, ValueInt.get(table.getId()));
        row.setValue(3, ValueInt.get(index.getRootPageId()));
        row.setValue(4, ValueString.get(options));
        row.setValue(5, ValueString.get(columnList));
        row.setKey(index.getId() + 1);
        metaIndex.add(session, row);
    }

    /**
     * Remove the meta data of an index.
     *
     * @param index the index to remove
     * @param session the session
     */
    public void removeMeta(Index index, Session session) {
        if (!recoveryRunning) {
            removeMetaIndex(index, session);
            metaObjects.remove(index.getId());
        }
    }

    private void removeMetaIndex(Index index, Session session) {
        int key = index.getId() + 1;
        Row row = metaIndex.getRow(session, key);
        if (row.getKey() != key) {
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, "key: " + key + " index: " + index + " table: " + index.getTable() + " row: " + row);
        }
        metaIndex.remove(session, row);
    }

    /**
     * Set the maximum transaction log size in megabytes.
     *
     * @param maxSize the new maximum log size
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
    public void setInDoubtTransactionState(int sessionId, int pageId, boolean commit) {
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
    public ArrayList<InDoubtTransaction> getInDoubtTransactions() {
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

    private void checkOpen() {
        if (file == null) {
            throw DbException.get(ErrorCode.DATABASE_IS_CLOSED);
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
     * Get the file write count since the database was created.
     *
     * @return the write count
     */
    public long getWriteCountTotal() {
        return writeCount + writeCountBase;
    }

    /**
     * Get the file write count since the database was opened.
     *
     * @return the write count
     */
    public long getWriteCount() {
        return writeCount;
    }

    /**
     * Get the file read count since the database was opened.
     *
     * @return the read count
     */
    public long getReadCount() {
        return readCount;
    }

    /**
     * A table is truncated.
     *
     * @param session the session
     * @param tableId the table id
     */
    public void logTruncate(Session session, int tableId) {
        synchronized (database) {
            if (!recoveryRunning) {
                log.logTruncate(session, tableId);
            }
        }
    }

    /**
     * Get the root page of an index.
     *
     * @param indexId the index id
     * @return the root page
     */
    public int getRootPageId(int indexId) {
        return metaRootPageId.get(indexId);
    }

    public Cache getCache() {
        return cache;
    }

    private void checksumSet(byte[] d, int pageId) {
        int ps = pageSize;
        int type = d[0];
        if (type == Page.TYPE_EMPTY) {
            return;
        }
        int s1 = 255 + (type & 255), s2 = 255 + s1;
        s2 += s1 += d[6] & 255;
        s2 += s1 += d[(ps >> 1) - 1] & 255;
        s2 += s1 += d[ps >> 1] & 255;
        s2 += s1 += d[ps - 2] & 255;
        s2 += s1 += d[ps - 1] & 255;
        d[1] = (byte) (((s1 & 255) + (s1 >> 8)) ^ pageId);
        d[2] = (byte) (((s2 & 255) + (s2 >> 8)) ^ (pageId >> 8));
    }

    /**
     * Check if the stored checksum is correct
     * @param d the data
     * @param pageId the page id
     * @param pageSize the page size
     * @return true if it is correct
     */
    public static boolean checksumTest(byte[] d, int pageId, int pageSize) {
        int ps = pageSize;
        int s1 = 255 + (d[0] & 255), s2 = 255 + s1;
        s2 += s1 += d[6] & 255;
        s2 += s1 += d[(ps >> 1) - 1] & 255;
        s2 += s1 += d[ps >> 1] & 255;
        s2 += s1 += d[ps - 2] & 255;
        s2 += s1 += d[ps - 1] & 255;
        if (d[1] != (byte) (((s1 & 255) + (s1 >> 8)) ^ pageId)
                || d[2] != (byte) (((s2 & 255) + (s2 >> 8)) ^ (pageId >> 8))) {
            return false;
        }
        return true;
    }

    /**
     * Increment the change count. To be done after the operation has finished.
     */
    public void incrementChangeCount() {
        changeCount++;
    }

    /**
     * Get the current change count. The first value is 1
     *
     * @return the change count
     */
    public int getChangeCount() {
        return changeCount;
    }

}
