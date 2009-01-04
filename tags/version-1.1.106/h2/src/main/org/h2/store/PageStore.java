/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.util.Cache;
import org.h2.util.Cache2Q;
import org.h2.util.CacheLRU;
import org.h2.util.CacheObject;
import org.h2.util.CacheWriter;
import org.h2.util.FileUtils;
import org.h2.util.ObjectArray;

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
 * <li>54-57: system table root page number (usually 1)</li>
 * <li>58-61: free list head page number (usually 2)</li>
 * <li>62-65: log head page number (usually 3)</li>
 * </ul>
 */
public class PageStore implements CacheWriter {

    private static final int PAGE_SIZE_MIN = 512;
    private static final int PAGE_SIZE_MAX = 32768;
    private static final int PAGE_SIZE_DEFAULT = 1024;
    private static final int INCREMENT_PAGES = 128;
    private static final int READ_VERSION = 0;
    private static final int WRITE_VERSION = 0;

    private Database database;
    private String fileName;
    private FileStore file;
    private String accessMode;
    private int cacheSize;
    private Cache cache;

    private int pageSize;
    private int pageSizeShift;
    private int systemRootPageId;
    private int freeListRootPageId;
    private int logRootPageId;

    /**
     * The file size in bytes.
     */
    private long fileLength;

    /**
     * Number of pages (including free pages).
     */
    private int pageCount;

    /**
     * The last page that is in use.
     */
    private int lastUsedPage;

    /**
     * Number of free pages in the free list.
     * This does not include empty pages at the end of the file
     * (after the last used page).
     */
    private int freePageCount;

    /**
     * The transaction log.
     */
    private PageLog log;

    /**
     * True if this is a new file.
     */
    private boolean isNew;

    /**
     * Create a new page store object.
     *
     * @param database the database
     * @param fileName the file name
     * @param accessMode the access mode
     * @param cacheSizeDefault the default cache size
     */
    public PageStore(Database database, String fileName, String accessMode, int cacheSizeDefault) {
        this.database = database;
        this.fileName = fileName;
        this.accessMode = accessMode;
        this.cacheSize = cacheSizeDefault;
        String cacheType = database.getCacheType();
        if (Cache2Q.TYPE_NAME.equals(cacheType)) {
            this.cache = new Cache2Q(this, cacheSize);
        } else {
            this.cache = new CacheLRU(this, cacheSize);
        }
    }

    /**
     * Open the file and read the header.
     */
    public void open() throws SQLException {
        try {
            if (FileUtils.exists(fileName)) {
                file = database.openFile(fileName, accessMode, true);
                readHeader();
                fileLength = file.length();
                pageCount = (int) (fileLength / pageSize);
                log = new PageLog(this, logRootPageId);
            } else {
                isNew = true;
                setPageSize(PAGE_SIZE_DEFAULT);
                file = database.openFile(fileName, accessMode, false);
                systemRootPageId = 1;
                freeListRootPageId = 2;
                PageFreeList free = new PageFreeList(this, freeListRootPageId, 0);
                updateRecord(free, null);
                logRootPageId = 3;
                lastUsedPage = 3;
                pageCount = 3;
                increaseFileSize(INCREMENT_PAGES - pageCount);
                writeHeader();
                log = new PageLog(this, logRootPageId);
            }
            log.openForWriting();
        } catch (SQLException e) {
            close();
            throw e;
        }
    }

    /**
     * Flush all pending changes to disk, and re-open the log file.
     */
    public void checkpoint() throws SQLException {
System.out.println("PageStore.checkpoint");
        synchronized (database) {
            database.checkPowerOff();
            ObjectArray list = cache.getAllChanged();
            CacheObject.sort(list);
            for (int i = 0; i < list.size(); i++) {
                Record rec = (Record) list.get(i);
                writeBack(rec);
            }
            log.reopen();
            int todoWriteDeletedPages;
        }
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
        systemRootPageId = page.readInt();
        freeListRootPageId = page.readInt();
        logRootPageId = page.readInt();
    }

    /**
     * Check if this page store was just created.
     *
     * @return true if it was
     */
    public boolean isNew() {
        return isNew;
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
        page.writeInt(systemRootPageId);
        page.writeInt(freeListRootPageId);
        page.writeInt(logRootPageId);
        file.seek(FileStore.HEADER_LENGTH);
        file.write(page.getBytes(), 0, pageSize - FileStore.HEADER_LENGTH);
    }

    /**
     * Close the file without flushing the cache.
     */
    public void close() throws SQLException {
        int todoTruncateLog;
        try {
            if (file != null) {
                file.close();
            }
        } catch (IOException e) {
            throw Message.convertIOException(e, "close");
        }
    }

    public void flushLog() throws SQLException {
        int todo;
    }

    public Trace getTrace() {
        return database.getTrace(Trace.DATABASE);
    }

    public void writeBack(CacheObject obj) throws SQLException {
        synchronized (database) {
            Record record = (Record) obj;
int test;
System.out.println("writeBack " + record);
            int todoRemoveParameter;
            record.write(null);
            record.setChanged(false);
        }
    }

    /**
     * Update a record.
     *
     * @param record the record
     * @param old the old data
     */
    public void updateRecord(Record record, DataPage old) throws SQLException {
        int todoLogHeaderPageAsWell;
        synchronized (database) {
            record.setChanged(true);
            int pos = record.getPos();
            cache.update(pos, record);
            if (old != null) {
                log.addUndo(record.getPos(), old);
            }
        }
    }

    /**
     * Allocate a page.
     *
     * @return the page id
     */
    public int allocatePage() throws SQLException {
        if (freePageCount == 0) {
            if (pageCount * pageSize >= fileLength) {
                increaseFileSize(INCREMENT_PAGES);
            }
        }
        if (lastUsedPage < pageCount) {
            return ++lastUsedPage;
        }
        if (freeListRootPageId == 0) {
            Message.throwInternalError();
        }
        PageFreeList free = (PageFreeList) cache.find(freeListRootPageId);
        if (free == null) {
            free = new PageFreeList(this, freeListRootPageId, 0);
            free.read();
        }
        int id = free.allocate();
        freePageCount--;
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
     */
    public void freePage(int pageId) throws SQLException {
        freePageCount++;
        PageFreeList free;
        cache.remove(pageId);
        free = (PageFreeList) cache.find(freeListRootPageId);
        if (free == null) {
            free = new PageFreeList(this, freeListRootPageId, 0);
            free.read();
        }
        free.free(pageId);
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
            updateRecord(free, null);
        }
    }

    /**
     * Get the system table root page number.
     *
     * @return the page number
     */
    public int getSystemRootPageId() {
        return systemRootPageId;
    }

    public PageLog getLog() {
        return log;
    }

    Database getDatabase() {
        return database;
    }

}
