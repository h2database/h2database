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
 * This class represents a file that is split into pages. The first page (page
 * 0) contains the file header, the second page (page 1) is the root of the
 * system table. The file header is 128 bytes, the format is:
 * <ul><li>0-47: file header (3 time "-- H2 0.5/B -- \n")
 * </li><li>48-51: database page size in bytes
 *     (512 - 32768, must be a power of 2)
 * </li><li>52: write version (0, otherwise the file is opened in read-only mode)
 * </li><li>53: read version (0, otherwise opening the file fails)
 * </li><li>54-57: page number of the system table root
 * </li><li>58-61: page number of the first free list page
 * </li><li>62-65: number of free pages
 * </li></ul>
 */
public class PageStore implements CacheWriter {

    private static final int PAGE_SIZE_MIN = 512;
    private static final int PAGE_SIZE_MAX = 32768;
    private static final int PAGE_SIZE_DEFAULT = 1024;
    private static final int FILE_HEADER_SIZE = 128;
    private static final int INCREMENT_PAGES = 128;

    private static final int READ_VERSION = 0;
    private static final int WRITE_VERSION = 0;
    private Database database;
    private int pageSize;
    private int pageSizeShift;
    private String fileName;
    private FileStore file;
    private String accessMode;
    private int cacheSize;
    private Cache cache;
    private DataPageBinary fileHeader;
    private int systemRootPageId;
    private int freeListRootPageId;
    private int freePageCount;
    private int pageCount;
    private int writeCount;
    private long fileLength;
    private long currentPos;

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
            fileHeader = new DataPageBinary(database, new byte[FILE_HEADER_SIZE - FileStore.HEADER_LENGTH]);
            if (FileUtils.exists(fileName)) {
                file = database.openFile(fileName, accessMode, true);
                readHeader();
            } else {
                setPageSize(PAGE_SIZE_DEFAULT);
                file = database.openFile(fileName, accessMode, false);
                writeHeader();
            }
            fileLength = file.length();
            pageCount = (int) (fileLength / pageSize);
        } catch (SQLException e) {
            close();
            throw e;
        }
    }

    /**
     * Flush all pending changes to disk.
     */
    public void flush() throws SQLException {
        synchronized (database) {
            database.checkPowerOff();
            ObjectArray list = cache.getAllChanged();
            CacheObject.sort(list);
            for (int i = 0; i < list.size(); i++) {
                Record rec = (Record) list.get(i);
                writeBack(rec);
            }
            int todoWriteDeletedPages;
        }
    }

    private void readHeader() throws SQLException {
        long length = file.length();
        if (length < FILE_HEADER_SIZE) {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, fileName);
        }
        database.notifyFileSize(length);
        file.seek(FileStore.HEADER_LENGTH);
        file.readFully(fileHeader.getBytes(), 0, FILE_HEADER_SIZE - FileStore.HEADER_LENGTH);
        setPageSize(fileHeader.readInt());
        int writeVersion = fileHeader.readByte();
        int readVersion = fileHeader.readByte();
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
        fileHeader.reset();
        systemRootPageId = fileHeader.readInt();
        freeListRootPageId = fileHeader.readInt();
        freePageCount = fileHeader.readInt();
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
        fileHeader.reset();
        fileHeader.writeInt(pageSize);
        fileHeader.writeByte((byte) WRITE_VERSION);
        fileHeader.writeByte((byte) READ_VERSION);
        fileHeader.writeInt(systemRootPageId);
        fileHeader.writeInt(freeListRootPageId);
        fileHeader.writeInt(freePageCount);
        file.seek(FileStore.HEADER_LENGTH);
        file.write(fileHeader.getBytes(), 0, FILE_HEADER_SIZE - FileStore.HEADER_LENGTH);
        byte[] filler = new byte[pageSize - FILE_HEADER_SIZE];
        file.write(filler, 0, filler.length);
    }

    /**
     * Close the file.
     */
    public void close() throws SQLException {
        int todo;
        try {
            flush();
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
            writeCount++;
            Record record = (Record) obj;
            record.write(null);
            record.setChanged(false);
        }
    }

    /**
     * Update a record.
     *
     * @param record the record
     */
    public void updateRecord(Record record) throws SQLException {
        synchronized (database) {
            record.setChanged(true);
            int pos = record.getPos();
            cache.update(pos, record);
            int todoLogChanges;
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
                long newLength = (pageCount + INCREMENT_PAGES) * pageSize;
                file.setLength(newLength);
                fileLength = newLength;
            }
            return pageCount++;
        }
        int todoReturnAFreePage;
        return 0;
    }

    /**
     * Create a data page.
     *
     * @return the data page.
     */
    public DataPageBinary createDataPage() {
        return new DataPageBinary(database, new byte[pageSize]);
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
    public DataPageBinary readPage(int pos) throws SQLException {
        DataPageBinary page = createDataPage();
        readPage(pos, page);
        return page;
    }

    /**
     * Read a page.
     *
     * @param pos the page id
     * @return the page
     */
    public void readPage(int pos, DataPageBinary page) throws SQLException {
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
    public void writePage(int pageId, DataPageBinary data) throws SQLException {
        file.seek(pageId << pageSizeShift);
        file.write(data.getBytes(), 0, pageSize);
    }

    /**
     * Add a page to the free list.
     *
     * @param pageId the page id
     */
    public void freePage(int pageId) {
        int todo;
    }

    /**
     * Remove a page from the cache.
     *
     * @param pageId the page id
     */
    public void removeRecord(int pageId) {
        cache.remove(pageId);
    }

}
