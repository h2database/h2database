/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import org.h2.api.DatabaseEventListener;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.log.LogSystem;
import org.h2.log.RedoLogRecord;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.util.BitField;
import org.h2.util.Cache;
import org.h2.util.CacheLRU;
import org.h2.util.CacheObject;
import org.h2.util.CacheWriter;
import org.h2.util.FileUtils;
import org.h2.util.IntArray;
import org.h2.util.MathUtils;
import org.h2.util.MemoryUtils;
import org.h2.util.New;
import org.h2.util.ObjectArray;

/**
 * This class represents a file that is usually written to disk. The two main
 * files are .data.db and .index.db. For each such file, a number of
 * {@link Storage} objects exists. The disk file is responsible for caching;
 * each object contains a {@link Cache} object. Changes in the file are logged
 * in a {@link LogSystem} object. Reading and writing to the file is delegated
 * to the {@link FileStore} class.
 * <p>
 * There are 'blocks' of 128 bytes (DiskFile.BLOCK_SIZE). Each objects own one
 * or more pages; each page size is 64 blocks (DiskFile.BLOCKS_PER_PAGE). That
 * is 8 KB page size. However pages are not read or written as one unit; only
 * individual objects (multiple blocks at a time) are read or written.
 * <p>
 * Currently there are no in-place updates. Each row occupies one or multiple
 * blocks. Rows can occupy multiple pages. Rows are always contiguous (except
 * LOBs, they are stored in their own files).
 */
public class DiskFile implements CacheWriter {

    /**
     * The number of bits to shift to divide a position to get the page number.
     */
    public static final int BLOCK_PAGE_PAGE_SHIFT = 6;

    /**
     * The size of a page in blocks.
     * Each page contains blocks from the same storage.
     */
    public static final int BLOCKS_PER_PAGE = 1 << BLOCK_PAGE_PAGE_SHIFT;

    /**
     * The size of a block in bytes.
     * A block is the minimum row size.
     */
    public static final int BLOCK_SIZE = 128;

    // TODO storage: header should probably be 4 KB or so
    // (to match block size of operating system)
    private static final int OFFSET = FileStore.HEADER_LENGTH;
    private static final int FREE_PAGE = -1;

    private Database database;
    private String fileName;
    private FileStore file;
    private BitField used;
    private BitField deleted;
    private HashSet<Integer> potentiallyFreePages;
    private int fileBlockCount;
    private IntArray pageOwners;
    private Cache cache;
    private LogSystem log;
    private DataPage rowBuff;
    private DataPage freeBlock;
    private boolean dataFile;
    private boolean logChanges;
    private int recordOverhead;
    private boolean init, initAlreadyTried;
    private ObjectArray<RedoLogRecord> redoBuffer;
    private int redoBufferSize;
    private int readCount, writeCount;
    private String mode;
    private int nextDeleteId = 1;
    // actually this is the first potentially free page
    private int firstFreePage;

    /**
     * Create a new disk file.
     *
     * @param database the database
     * @param fileName the file name
     * @param mode the file opening mode ("r", "rw", "rws", "rwd")
     * @param dataFile if this is the data file
     * @param logChanges if changes should be logged
     * @param cacheSize the number of cache entries
     */
    public DiskFile(Database database, String fileName, String mode, boolean dataFile, boolean logChanges, int cacheSize) throws SQLException {
        reset();
        this.database = database;
        this.log = database.getLog();
        this.fileName = fileName;
        this.mode = mode;
        this.dataFile = dataFile;
        this.logChanges = logChanges;
        String cacheType = database.getCacheType();
        this.cache = CacheLRU.getCache(this, cacheType, cacheSize);

        rowBuff = DataPage.create(database, BLOCK_SIZE);
        // TODO: the overhead is larger in the log file, so this value is too high :-(
        recordOverhead = 4 * DataPage.LENGTH_INT + 1 + DataPage.LENGTH_FILLER;
        freeBlock = DataPage.create(database, BLOCK_SIZE);
        freeBlock.fill(BLOCK_SIZE);
        freeBlock.updateChecksum();
        try {
            if (FileUtils.exists(fileName)) {
                file = database.openFile(fileName, mode, true);
                long length = file.length();
                database.notifyFileSize(length);
                int blocks = (int) ((length - OFFSET) / BLOCK_SIZE);
                setBlockCount(blocks);
            } else {
                create();
            }
        } catch (SQLException e) {
            close();
            throw e;
        }
    }

    private void reset() {
        used = new BitField();
        deleted = new BitField();
        pageOwners = new IntArray();
        // init pageOwners
        setBlockCount(fileBlockCount);
        redoBuffer = ObjectArray.newInstance();
        potentiallyFreePages = New.hashSet();
    }

    private void setBlockCount(int count) {
        fileBlockCount = count;
        int pages = getPage(count);
        while (pages >= pageOwners.size()) {
            pageOwners.add(FREE_PAGE);
        }
    }

    private void create() throws SQLException {
        file = database.openFile(fileName, mode, false);
        DataPage header = DataPage.create(database, OFFSET);
        file.seek(FileStore.HEADER_LENGTH);
        header.fill(OFFSET);
        header.updateChecksum();
        file.write(header.getBytes(), 0, OFFSET);
    }

    private void freeUnusedPages() throws SQLException {
        // first, store the unused pages and current owner in a temporary list
        IntArray freePages = new IntArray();
        HashSet<Integer> owners = New.hashSet();
        for (int i = 0; i < pageOwners.size(); i++) {
            int owner = pageOwners.get(i);
            if (owner != FREE_PAGE && isPageFree(i)) {
                owners.add(owner);
                freePages.add(i);
            }
        }
        // now, for each current owner, remove those
        // this is much faster than removing them individually
        // as this would cause O(n^2) behavior
        for (int owner : owners) {
            database.getStorage(owner, this).removePages(freePages);
        }
        // now free up the pages
        for (int i = 0; i < freePages.size(); i++) {
            int idx = freePages.get(i);
            setPageOwner(idx, FREE_PAGE);
        }
    }

    /**
     * Get the 'storage allocation table' of this file.
     *
     * @return the table
     */
    public byte[] getSummary() throws SQLException {
        synchronized (database) {
            try {
                ByteArrayOutputStream buff = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(buff);
                int blocks = (int) ((file.length() - OFFSET) / BLOCK_SIZE);
                out.writeInt(blocks);
                for (int i = 0, x = 0; i < blocks / 8; i++) {
                    int mask = 0;
                    for (int j = 0; j < 8; j++) {
                        if (used.get(x)) {
                            mask |= 1 << j;
                        }
                        x++;
                    }
                    out.write(mask);
                }
                out.writeInt(pageOwners.size());
                ObjectArray<Storage> storages = ObjectArray.newInstance();
                for (int i = 0; i < pageOwners.size(); i++) {
                    int s = pageOwners.get(i);
                    out.writeInt(s);
                    if (s >= 0 && (s >= storages.size() || storages.get(s) == null)) {
                        Storage storage = database.getStorage(s, this);
                        while (storages.size() <= s) {
                            storages.add(null);
                        }
                        storages.set(s, storage);
                    }
                }
                for (int i = 0; i < storages.size(); i++) {
                    Storage storage = storages.get(i);
                    if (storage != null) {
                        out.writeInt(i);
                        out.writeInt(storage.getRecordCount());
                    }
                }
                out.writeInt(-1);
                out.close();
                byte[] b2 = buff.toByteArray();
                return b2;
            } catch (IOException e) {
                // will probably never happen, because only in-memory structures are
                // used
                return null;
            }
        }
    }

    /**
     * Check if a page is free, that is, if all blocks of the page are not in use.
     *
     * @param page the page id
     * @return true if no blocks are used
     */
    boolean isPageFree(int page) {
        for (int i = page * BLOCKS_PER_PAGE; i < (page + 1) * BLOCKS_PER_PAGE; i++) {
            if (used.get(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Initialize the the 'storage allocation table' of this file from a given
     * byte array.
     *
     * @param summary the storage allocation table
     */
    public void initFromSummary(byte[] summary) {
        synchronized (database) {
            if (summary == null || summary.length == 0) {
                for (Storage s : database.getAllStorages()) {
                    if (s != null && s.getDiskFile() == this) {
                        database.removeStorage(s.getId(), this);
                    }
                }
                reset();
                initAlreadyTried = false;
                init = false;
                return;
            }
            if (database.getRecovery() || (initAlreadyTried && (!dataFile || !SysProperties.CHECK))) {
                return;
            }
            initAlreadyTried = true;
            int stage = 0;
            try {
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(summary));
                int b2 = in.readInt();
                if (b2 > fileBlockCount) {
                    getTrace().info(
                            "unexpected size " + b2 + " when initializing summary for " + fileName + " expected:"
                                    + fileBlockCount);
                    return;
                }
                stage++;
                if (init) {
                    for (int x = 0; x < b2; x += 8) {
                        int mask = in.read();
                        if (mask != used.getByte(x)) {
                            Message.throwInternalError("Redo failure, block: " + x + " expected: " + used.getByte(x) + " got: " + mask);
                        }
                    }
                } else {
                    for (int x = 0; x < b2; x += 8) {
                        int mask = in.read();
                        used.setByte(x, mask);
                    }
                }
                stage++;
                int len = in.readInt();
                ObjectArray<Storage> storages = ObjectArray.newInstance();
                for (int i = 0; i < len; i++) {
                    int s = in.readInt();
                    while (storages.size() <= s) {
                        storages.add(null);
                    }
                    if (init) {
                        int old = getPageOwner(i);
                        if (old != -1 && old != s) {
                            Message.throwInternalError("Redo failure, expected page owner: " + old + " got: " + s);
                        }
                    } else {
                        if (s >= 0) {
                            Storage storage = database.getStorage(s, this);
                            storages.set(s, storage);
                            storage.addPage(i);
                        }
                        setPageOwner(i, s);
                    }
                }
                stage++;
                while (true) {
                    int s = in.readInt();
                    if (s < 0) {
                        break;
                    }
                    int recordCount = in.readInt();
                    Storage storage = storages.get(s);
                    if (init) {
                        if (storage != null) {
                            int current = storage.getRecordCount();
                            if (current != recordCount) {
                                Message.throwInternalError("Redo failure, expected row count: " + current + " got: " + recordCount);
                            }
                        }
                    } else {
                        storage.setRecordCount(recordCount);
                    }
                }
                stage++;
                freeUnusedPages();
                init = true;
            } catch (Throwable e) {
                getTrace().error(
                        "error initializing summary for " + fileName + " size:" + summary.length + " stage:" + stage, e);
                // ignore - init is still false in this case
            }
        }
    }

    /**
     * Read the 'storage allocation table' from the file if required.
     */
    public void init() throws SQLException {
        synchronized (database) {
            if (init) {
                return;
            }
            ObjectArray<Storage> storages = database.getAllStorages();
            for (int i = 0; i < storages.size(); i++) {
                Storage s = storages.get(i);
                if (s != null && s.getDiskFile() == this) {
                    s.setRecordCount(0);
                }
            }
            int blockHeaderLen = Math.max(Constants.FILE_BLOCK_SIZE, 2 * DataPage.LENGTH_INT);
            byte[] buff = new byte[blockHeaderLen];
            DataPage s = DataPage.create(database, buff);
            long time = 0;
            for (int i = 0; i < fileBlockCount;) {
                long t2 = System.currentTimeMillis();
                if (t2 > time + 10) {
                    time = t2;
                    database.setProgress(DatabaseEventListener.STATE_SCAN_FILE, this.fileName, i, fileBlockCount);
                }
                go(i);
                file.readFully(buff, 0, blockHeaderLen);
                s.reset();
                int blockCount = s.readInt();
                if (SysProperties.CHECK && blockCount < 0) {
                    Message.throwInternalError();
                }
                if (blockCount == 0) {
                    setUnused(null, i, 1);
                    i++;
                } else {
                    int id = s.readInt();
                    if (SysProperties.CHECK && id < 0) {
                        Message.throwInternalError();
                    }
                    Storage storage = database.getStorage(id, this);
                    setUnused(null, i, blockCount);
                    setBlockOwner(null, storage, i, blockCount, true);
                    storage.incrementRecordCount();
                    i += blockCount;
                }
            }
            database.setProgress(DatabaseEventListener.STATE_SCAN_FILE, this.fileName, fileBlockCount, fileBlockCount);
            init = true;
        }
    }

    /**
     * Flush all pending changes to disk.
     */
    public void flush() throws SQLException {
        synchronized (database) {
            database.checkPowerOff();
            ObjectArray<CacheObject> list = cache.getAllChanged();
            CacheObject.sort(list);
            for (CacheObject rec : list) {
                writeBack(rec);
            }
            for (int i = 0; i < fileBlockCount; i++) {
                i = deleted.nextSetBit(i);
                if (i < 0) {
                    break;
                }
                if (deleted.get(i)) {
                    writeDirectDeleted(i, 1);
                    deleted.clear(i);
                }
            }
        }
    }

    // this implementation accesses the file in a linear way
//    public void flushNew() throws SQLException {
//        int todoTest;
//        synchronized (database) {
//            database.checkPowerOff();
//            ObjectArray list = cache.getAllChanged();
//            CacheObject.sort(list);
//            int deletePos = deleted.nextSetBit(0);
//            int writeIndex = 0;
//            Record writeRecord = null;
//            while (true) {
//                if (writeRecord == null && writeIndex < list.size()) {
//                    writeRecord = (Record) list.get(writeIndex++);
//                }
//                if (writeRecord != null &&
//                       (deletePos < 0 || writeRecord.getPos() < deletePos)) {
//                    writeBack(writeRecord);
//                    writeRecord = null;
//                } else if (deletePos < fileBlockCount && deletePos >= 0) {
//                    writeDirectDeleted(deletePos, 1);
//                    deleted.clear(deletePos);
//                    deletePos = deleted.nextSetBit(deletePos);
//                } else {
//                    break;
//                }
//            }
//        }
//    }

    /**
     * Flush all pending changes and close the file.
     */
    public void close() throws SQLException {
        synchronized (database) {
            SQLException closeException = null;
            if (!database.isReadOnly()) {
                try {
                    flush();
                } catch (SQLException e) {
                    closeException = e;
                }
            }
            cache.clear();
            // continue with close even if flush was not possible (file storage
            // problem)
            if (file != null) {
                file.closeSilently();
                file = null;
            }
            if (closeException != null) {
                throw closeException;
            }
            readCount = writeCount = 0;
        }
    }

    private void go(int block) throws SQLException {
        database.checkPowerOff();
        file.seek(getFilePos(block));
    }

    private long getFilePos(int block) {
        return ((long) block * BLOCK_SIZE) + OFFSET;
    }

    /**
     * Get the record if it is stored in the file, or null if not.
     *
     * @param session the session
     * @param pos the block id
     * @param reader the record reader that can parse the data
     * @param storageId the storage id
     * @return the record or null
     */
    Record getRecordIfStored(Session session, int pos, RecordReader reader, int storageId)
            throws SQLException {
        synchronized (database) {
            try {
                int owner = getPageOwner(getPage(pos));
                if (owner != storageId) {
                    return null;
                }
                go(pos);
                rowBuff.reset();
                byte[] buff = rowBuff.getBytes();
                file.readFully(buff, 0, BLOCK_SIZE);
                DataPage s = DataPage.create(database, buff);
                // blockCount
                s.readInt();
                int id = s.readInt();
                if (id != storageId) {
                    return null;
                }
            } catch (Exception e) {
                return null;
            }
            return getRecord(session, pos, reader, storageId);
        }
    }

    /**
     * Get a record from the cache or read it from the file if required.
     *
     * @param session the session
     * @param pos the block id
     * @param reader the record reader that can parse the data
     * @param storageId the storage id
     * @return the record
     */
    Record getRecord(Session session, int pos, RecordReader reader, int storageId) throws SQLException {
        synchronized (database) {
            if (file == null) {
                throw Message.getSQLException(ErrorCode.SIMULATED_POWER_OFF);
            }
            Record record = (Record) cache.get(pos);
            if (record != null) {
                return record;
            }
            readCount++;
            go(pos);
            rowBuff.reset();
            byte[] buff = rowBuff.getBytes();
            file.readFully(buff, 0, BLOCK_SIZE);
            DataPage s = DataPage.create(database, buff);
            int blockCount = s.readInt();
            int id = s.readInt();
            if (storageId != id) {
                Message.throwInternalError("File ID mismatch got=" + id + " expected=" + storageId + " pos=" + pos
                        + " " + logChanges + " " + this + " blockCount:" + blockCount);
            }
            if (blockCount == 0) {
                Message.throwInternalError("0 blocks to read pos=" + pos);
            }
            if (blockCount > 1) {
                byte[] b2 = MemoryUtils.newBytes(blockCount * BLOCK_SIZE);
                System.arraycopy(buff, 0, b2, 0, BLOCK_SIZE);
                buff = b2;
                file.readFully(buff, BLOCK_SIZE, blockCount * BLOCK_SIZE - BLOCK_SIZE);
                s = DataPage.create(database, buff);
                s.readInt();
                s.readInt();
            }
            s.check(blockCount * BLOCK_SIZE);
            Record r = reader.read(session, s);
            r.setStorageId(storageId);
            r.setPos(pos);
            r.setBlockCount(blockCount);
            cache.put(r);
            return r;
        }
    }

    /**
     * Allocate space in the file.
     *
     * @param storage the storage
     * @param blockCount the number of blocks required
     * @return the position of the first entry
     */
    int allocate(Storage storage, int blockCount) throws SQLException {
        reuseSpace();
        synchronized (database) {
            if (file == null) {
                throw Message.getSQLException(ErrorCode.SIMULATED_POWER_OFF);
            }
            blockCount = getPage(blockCount + BLOCKS_PER_PAGE - 1) * BLOCKS_PER_PAGE;
            int lastPage = getPage(fileBlockCount);
            int pageCount = getPage(blockCount);
            int pos = -1;
            boolean found = false;
            // correct firstFreePage
            int i = firstFreePage;
            for (; i < lastPage; i++) {
                if (getPageOwner(i) == FREE_PAGE) {
                    break;
                }
            }
            firstFreePage = i;
            for (; i < lastPage; i++) {
                found = true;
                for (int j = i; j < i + pageCount; j++) {
                    if (j >= lastPage || getPageOwner(j) != FREE_PAGE) {
                        found = false;
                        break;
                    }
                }
                if (found) {
                    pos = i * BLOCKS_PER_PAGE;
                    break;
                }
            }
            if (!found) {
                int max = fileBlockCount;
                pos = MathUtils.roundUp(max, BLOCKS_PER_PAGE);
                long min = ((long) pos + blockCount) * BLOCK_SIZE;
                min = MathUtils.scaleUp50Percent(Constants.FILE_MIN_SIZE, min, Constants.FILE_PAGE_SIZE, Constants.FILE_MAX_INCREMENT) + OFFSET;
                if (min > file.length()) {
                    file.setLength(min);
                    database.notifyFileSize(min);
                }
            }
            setBlockOwner(null, storage, pos, blockCount, false);
            for (i = 0; i < blockCount; i++) {
                storage.free(i + pos, 1);
            }
            return pos;
        }
    }

    private void setBlockOwner(Session session, Storage storage, int pos, int blockCount, boolean inUse) throws SQLException {
        if (pos + blockCount > fileBlockCount) {
            setBlockCount(pos + blockCount);
        }
        if (!inUse) {
            setUnused(session, pos, blockCount);
        }
        for (int i = getPage(pos); i <= getPage(pos + blockCount - 1); i++) {
            setPageOwner(i, storage.getId());
        }
        if (inUse) {
            used.setRange(pos, blockCount, true);
            deleted.setRange(pos, blockCount, false);
        }
    }

    private void setUnused(Session session, int pos, int blockCount) throws SQLException {
        if (pos + blockCount > fileBlockCount) {
            setBlockCount(pos + blockCount);
        }
        uncommittedDelete(session);
        for (int i = pos; i < pos + blockCount; i++) {
            used.clear(i);
            if ((i % BLOCKS_PER_PAGE == 0) && (pos + blockCount >= i + BLOCKS_PER_PAGE)) {
                // if this is the first page of a block and if the whole page is free
                freePage(getPage(i));
            }
        }
    }

    private void reuseSpace() throws SQLException {
        if (SysProperties.REUSE_SPACE_QUICKLY) {
            synchronized (potentiallyFreePages) {
                if (potentiallyFreePages.size() >= SysProperties.REUSE_SPACE_AFTER) {
                    Session[] sessions = database.getSessions(true);
                    int oldest = 0;
                    for (int i = 0; i < sessions.length; i++) {
                        int deleteId = sessions[i].getLastUncommittedDelete();
                        if (oldest == 0 || (deleteId != 0 && deleteId < oldest)) {
                            oldest = deleteId;
                        }
                    }
                    for (Iterator<Integer> it = potentiallyFreePages.iterator(); it.hasNext();) {
                        int p = it.next();
                        if (oldest == 0) {
                            if (isPageFree(p)) {
                                // the page may not be free: the storage
                                // could have re-used it using the storage local free list
                                setPageOwner(p, FREE_PAGE);
                            }
                            it.remove();
                        }
                    }
                }
            }
        }
    }

    /**
     * Called after a session deleted a row. This sets the last uncommitted
     * delete id in the session. This is used to make sure empty space is not
     * re-used before the change is committed.
     *
     * @param session the session
     */
    void uncommittedDelete(Session session) {
        if (session != null && logChanges && SysProperties.REUSE_SPACE_QUICKLY) {
            int deleteId = session.getLastUncommittedDelete();
            if (deleteId == 0 || deleteId < nextDeleteId) {
                deleteId = ++nextDeleteId;
                session.setLastUncommittedDelete(deleteId);
            }
        }
    }

    /**
     * Free a page, that is, reset the page owner.
     *
     * @param page the page
     */
    void freePage(int page) throws SQLException {
        if (!logChanges) {
            setPageOwner(page, FREE_PAGE);
        } else {
            if (SysProperties.REUSE_SPACE_QUICKLY) {
                synchronized (potentiallyFreePages) {
                    potentiallyFreePages.add(page);
                }
            }
        }
    }

    /**
     * Calculate the page number from a block number.
     *
     * @param pos the block number
     * @return the page number
     */
    int getPage(int pos) {
        return pos >>> BLOCK_PAGE_PAGE_SHIFT;
    }

    /**
     * Get the storage id of a page.
     *
     * @param page the page id
     * @return the storage id
     */
    int getPageOwner(int page) {
        if (page * BLOCKS_PER_PAGE > fileBlockCount || page >= pageOwners.size()) {
            return FREE_PAGE;
        }
        return pageOwners.get(page);
    }

    /**
     * Set the owner of a page.
     *
     * @param page the page id
     * @param storageId the storage id of this page
     */
    public void setPageOwner(int page, int storageId) throws SQLException {
        int old = pageOwners.get(page);
        if (old == storageId) {
            return;
        }
        if (SysProperties.CHECK && old >= 0 && storageId >= 0 && old != storageId) {
            for (int i = 0; i < BLOCKS_PER_PAGE; i++) {
                if (used.get(i + page * BLOCKS_PER_PAGE)) {
                    Message.throwInternalError(
                            "double allocation in file " + fileName +
                            " page " + page +
                            " blocks " + (BLOCKS_PER_PAGE * page) +
                            "-" + (BLOCKS_PER_PAGE * (page + 1) - 1));
                }
            }
        }
        if (old >= 0) {
            database.getStorage(old, this).removePage(page);
            if (!logChanges) {
                // need to clean the page, otherwise it may never get cleaned
                // and can become corrupted
                writeDirectDeleted(page * BLOCKS_PER_PAGE, BLOCKS_PER_PAGE);
            }
        }
        if (storageId >= 0) {
            database.getStorage(storageId, this).addPage(page);
            if (SysProperties.REUSE_SPACE_QUICKLY) {
                synchronized (potentiallyFreePages) {
                    potentiallyFreePages.remove(page);
                }
            }
        } else {
            firstFreePage = Math.min(firstFreePage, page);
        }
        pageOwners.set(page, storageId);
    }

    /**
     * Mark a number of blocks as used.
     *
     * @param pos the first block id
     * @param blockCount the number of blocks
     */
    void setUsed(int pos, int blockCount) {
        synchronized (database) {
            if (pos + blockCount > fileBlockCount) {
                setBlockCount(pos + blockCount);
            }
            used.setRange(pos, blockCount, true);
            deleted.setRange(pos, blockCount, false);
        }
    }

    /**
     * Close the file and delete it.
     */
    public void delete() throws SQLException {
        synchronized (database) {
            try {
                cache.clear();
                file.close();
                FileUtils.delete(fileName);
            } catch (IOException e) {
                throw Message.convertIOException(e, fileName);
            } finally {
                file = null;
                fileName = null;
            }
        }
    }

    //    private int allocateBest(int start, int blocks) {
    //        while (true) {
    //            int p = getLastUsedPlusOne(start, blocks);
    //            if (p == start) {
    //                start = p;
    //                break;
    //            }
    //            start = p;
    //        }
    //        allocate(start, blocks);
    //        return start;
    //    }

    /**
     * Write a record to the file immediately.
     * This method is called by the cache, and when flushing pending changes.
     *
     * @param obj the record to write
     */
    public void writeBack(CacheObject obj) throws SQLException {
        synchronized (database) {
            writeCount++;
            Record record = (Record) obj;
            int blockCount = record.getBlockCount();
            record.prepareWrite();
            go(record.getPos());
            DataPage buff = rowBuff;
            buff.reset();
            buff.checkCapacity(blockCount * BLOCK_SIZE);
            buff.writeInt(blockCount);
            buff.writeInt(record.getStorageId());
            record.write(buff);
            buff.fill(blockCount * BLOCK_SIZE);
            buff.updateChecksum();
            file.write(buff.getBytes(), 0, buff.length());
            record.setChanged(false);
        }
    }

    /**
     * Get the usage bits. The bit field must be synchronized externally.
     *
     * @return the bit field of used blocks.
     */
    BitField getUsed() {
        return used;
    }

    /**
     * Update a record.
     *
     * @param session the session
     * @param record the record
     */
    void updateRecord(Session session, Record record) throws SQLException {
        synchronized (database) {
            record.setChanged(true);
            int pos = record.getPos();
            Record old = (Record) cache.update(pos, record);
            if (SysProperties.CHECK) {
                if (old != null) {
                    if (old != record) {
                        database.checkPowerOff();
                        Message.throwInternalError("old != record old=" + old + " new=" + record);
                    }
                    int blockCount = record.getBlockCount();
                    for (int i = 0; i < blockCount; i++) {
                        if (deleted.get(i + pos)) {
                            Message.throwInternalError("update marked as deleted: " + (i + pos));
                        }
                    }
                }
            }
            if (logChanges) {
                log.add(session, this, record);
            }
        }
    }

    private void writeDirectDeleted(int recordId, int blockCount) throws SQLException {
        synchronized (database) {
            go(recordId);
            for (int i = 0; i < blockCount; i++) {
                file.write(freeBlock.getBytes(), 0, freeBlock.length());
            }
            free(recordId, blockCount);
        }
    }

    private void writeDirect(Storage storage, int pos, byte[] data, int offset) throws SQLException {
        synchronized (database) {
            go(pos);
            file.write(data, offset, BLOCK_SIZE);
            setBlockOwner(null, storage, pos, 1, true);
        }
    }

    /**
     * Copy a number of bytes at the specified location to the output stream.
     *
     * @param pos the position
     * @param out the output stream
     * @return the new position, or -1 if there is no more data to copy
     */
    public int copyDirect(int pos, OutputStream out) throws SQLException {
        synchronized (database) {
            try {
                if (pos < 0) {
                    // read the header
                    byte[] buffer = new byte[OFFSET];
                    file.seek(0);
                    file.readFullyDirect(buffer, 0, OFFSET);
                    out.write(buffer);
                    return 0;
                }
                if (pos >= fileBlockCount) {
                    return -1;
                }
                int blockSize = DiskFile.BLOCK_SIZE;
                byte[] buff = new byte[blockSize];
                DataPage s = DataPage.create(database, buff);
                database.setProgress(DatabaseEventListener.STATE_BACKUP_FILE, this.fileName, pos, fileBlockCount);
                go(pos);
                file.readFully(buff, 0, blockSize);
                s.reset();
                int blockCount = s.readInt();
                if (SysProperties.CHECK && blockCount < 0) {
                    Message.throwInternalError();
                }
                if (blockCount == 0) {
                    blockCount = 1;
                }
                int id = s.readInt();
                if (SysProperties.CHECK && id < 0) {
                    Message.throwInternalError();
                }
                s.checkCapacity(blockCount * blockSize);
                if (blockCount > 1) {
                    file.readFully(s.getBytes(), blockSize, blockCount * blockSize - blockSize);
                }
                if (file.isEncrypted()) {
                    s.reset();
                    go(pos);
                    file.readFullyDirect(s.getBytes(), 0, blockCount * blockSize);
                }
                out.write(s.getBytes(), 0, blockCount * blockSize);
                return pos + blockCount;
            } catch (IOException e) {
                throw Message.convertIOException(e, fileName);
            }
        }
    }

    /**
     * Remove a record from the cache and mark it as deleted. This writes the
     * old data to the transaction log.
     *
     * @param session the session
     * @param pos the block id
     * @param record the record
     * @param blockCount the number of blocks
     */
    void removeRecord(Session session, int pos, Record record, int blockCount) throws SQLException {
        synchronized (database) {
            if (logChanges) {
                log.add(session, this, record);
            }
            cache.remove(pos);
            deleted.setRange(pos, blockCount, true);
            setUnused(session, pos, blockCount);
        }
    }

    /**
     * Add a record to the file. The position of the record must already be set
     * before. This method will write the change to the transaction log and will
     * update the cache.
     *
     * @param session the session
     * @param record the record
     */
    void addRecord(Session session, Record record) throws SQLException {
        synchronized (database) {
            cache.put(record);
            if (logChanges) {
                log.add(session, this, record);
            }
        }
    }

    /**
     * Get the cache. The cache must be synchronized externally.
     *
     * @return the cache
     */
    public Cache getCache() {
        return cache;
    }

    /**
     * Free up a number of blocks.
     *
     * @param pos the position of the first block
     * @param blockCount the number of blocks
     */
    void free(int pos, int blockCount) {
        synchronized (database) {
            used.setRange(pos, blockCount, false);
        }
    }

    /**
     * Get the overhead for each record in bytes.
     *
     * @return the overhead
     */
    int getRecordOverhead() {
        return recordOverhead;
    }

    /**
     * Remove all rows for this storage.
     *
     * @param session the session
     * @param storage the storage
     * @param pages the page id array
     */
    void truncateStorage(Session session, Storage storage, IntArray pages) throws SQLException {
        synchronized (database) {
            int storageId = storage.getId();
            // make sure the cache records of this storage are not flushed to disk
            // afterwards
            for (CacheObject obj : cache.getAllChanged()) {
                Record r = (Record) obj;
                if (r.getStorageId() == storageId) {
                    r.setChanged(false);
                }
            }
            int[] pagesCopy = new int[pages.size()];
            // can not use pages directly, because setUnused removes rows from there
            pages.toArray(pagesCopy);
            for (int i = 0; i < pagesCopy.length; i++) {
                int page = pagesCopy[i];
                for (int j = 0; j < BLOCKS_PER_PAGE; j++) {
                    Record r = (Record) cache.find(page * BLOCKS_PER_PAGE + j);
                    if (r != null) {
                        cache.remove(r.getPos());
                    }
                }
                deleted.setRange(page * BLOCKS_PER_PAGE, BLOCKS_PER_PAGE, true);
                setUnused(session, page * BLOCKS_PER_PAGE, BLOCKS_PER_PAGE);
                // the truncate entry must be written after changing the
                // in-memory structures (page owner, in-use bit set), because
                // the log file could change just after the truncate record
                // and before the flags are reset - this would result in
                // incorrect in use bits written
                if (logChanges) {
                    log.addTruncate(session, this, storageId, page * BLOCKS_PER_PAGE, BLOCKS_PER_PAGE);
                }
            }
        }
    }

    /**
     * Flush pending writes of the underlying file.
     */
    public void sync() {
        synchronized (database) {
            if (file != null) {
                file.sync();
            }
        }
    }

    /**
     * Check if this is the data file.
     *
     * @return true if this is the data file
     */
    public boolean isDataFile() {
        return dataFile;
    }

    /**
     * Set whether changes should be written to the transaction log before they
     * are applied in the file.
     *
     * @param logChanges the new value
     */
    public void setLogChanges(boolean logChanges) {
        synchronized (database) {
            this.logChanges = logChanges;
        }
    }

    /**
     * Add a redo-log entry to the redo buffer.
     *
     * @param storage the storage
     * @param recordId the record id of the entry
     * @param blockCount the number of blocks
     * @param rec the record
     */
    public void addRedoLog(Storage storage, int recordId, int blockCount, DataPage rec) throws SQLException {
        synchronized (database) {
            byte[] data = null;
            if (rec != null) {
                DataPage all = rowBuff;
                all.reset();
                all.writeInt(blockCount);
                all.writeInt(storage.getId());
                all.writeDataPageNoSize(rec);
                // the buffer may have some additional fillers - just ignore them
                all.fill(blockCount * BLOCK_SIZE);
                all.updateChecksum();
                if (SysProperties.CHECK && all.length() != BLOCK_SIZE * blockCount) {
                    Message.throwInternalError("blockCount:" + blockCount + " length: " + all.length() * BLOCK_SIZE);
                }
                data = new byte[all.length()];
                System.arraycopy(all.getBytes(), 0, data, 0, all.length());
            }
            for (int i = 0; i < blockCount; i++) {
                RedoLogRecord log = new RedoLogRecord();
                log.recordId = recordId + i;
                log.offset = i * BLOCK_SIZE;
                log.storage = storage;
                log.data = data;
                log.sequenceId = redoBuffer.size();
                redoBuffer.add(log);
                redoBufferSize += log.getSize();
            }
            if (redoBufferSize > SysProperties.REDO_BUFFER_SIZE) {
                flushRedoLog();
            }
        }
    }

    /**
     * Write all buffered redo log data to the file. Redo log data is buffered
     * to improve recovery performance.
     */
    public void flushRedoLog() throws SQLException {
        synchronized (database) {
            if (redoBuffer.size() == 0) {
                return;
            }
            redoBuffer.sort(new Comparator<RedoLogRecord>() {
                public int compare(RedoLogRecord e1, RedoLogRecord e2) {
                    int comp = e1.recordId - e2.recordId;
                    if (comp == 0) {
                        comp = e1.sequenceId - e2.sequenceId;
                    }
                    return comp;
                }
            });
            // first write all deleted entries
            // because delete entries are always 1 block,
            // while not-deleted entries can be many blocks
            // so for example:
            // (A) block: 1 (delete)
            // (B) block: 2 (delete)
            // (C) block: 1 ('Hello', 2 blocks long)
            // needs to be written in this order and not (A) (C) (B)
            RedoLogRecord last = null;
            for (int i = 0; i < redoBuffer.size(); i++) {
                RedoLogRecord entry = redoBuffer.get(i);
                if (entry.data != null) {
                    continue;
                }
                if (last != null && entry.recordId != last.recordId) {
                    writeRedoLog(last);
                }
                last = entry;
            }
            if (last != null) {
                writeRedoLog(last);
            }
            // now write the last entry, skipping deleted entries
            last = null;
            for (int i = 0; i < redoBuffer.size(); i++) {
                RedoLogRecord entry = redoBuffer.get(i);
                if (last != null && entry.recordId != last.recordId) {
                    if (last.data != null) {
                        writeRedoLog(last);
                    }
                }
                last = entry;
            }
            if (last != null && last.data != null) {
                writeRedoLog(last);
            }
            redoBuffer.clear();
            redoBufferSize = 0;
        }
    }

    private void writeRedoLog(RedoLogRecord entry) throws SQLException {
        if (entry.data == null) {
            writeDirectDeleted(entry.recordId, 1);
        } else {
            writeDirect(entry.storage, entry.recordId, entry.data, entry.offset);
        }
    }

    public int getWriteCount() {
        return writeCount;
    }

    public int getReadCount() {
        return readCount;
    }

    public void flushLog() throws SQLException {
        if (log != null) {
            log.flush();
        }
    }

    public String toString() {
        return getClass().getName() + ":" + fileName;
    }

    public Trace getTrace() {
        return database.getTrace(Trace.DATABASE);
    }

}
