/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
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
import org.h2.util.Cache2Q;
import org.h2.util.CacheLRU;
import org.h2.util.CacheObject;
import org.h2.util.CacheWriter;
import org.h2.util.FileUtils;
import org.h2.util.IntArray;
import org.h2.util.MathUtils;
import org.h2.util.ObjectArray;
import org.h2.util.ObjectUtils;

/**
 * This class represents a file that is usually written to disk.
 * The two main files are .data.db and .index.db.
 * For each such file, a number of {@link Storage} objects exists.
 * The disk file is responsible for caching; each object contains a {@link Cache} object.
 * Changes in the file are logged in a {@link LogSystem} object.
 * Reading and writing to the file is delegated to the {@link FileStore} class.
 * <p>
 * There are 'blocks' of 128 bytes (DiskFile.BLOCK_SIZE). Each objects own one or more pages;
 * each page size is 64 blocks (DiskFile.BLOCKS_PER_PAGE). That is 8 KB page size.
 * However pages are not read or written as one unit; only individual objects (multiple blocks at a time)
 * are read or written.
 * <p>
 * Currently there are no in-place updates. Each row occupies one or multiple blocks.
 * Row can occupy multiple pages. Rows are always contiguous (except LOBs, they are
 * stored in their own files).
 */
public class DiskFile implements CacheWriter {

    /**
     * The size of a block in bytes.
     * A block is the minimum row size.
     */
    public static final int BLOCK_SIZE = 128;

    /**
     * The size of a page in blocks.
     * Each page contains blocks from the same storage.
     */
    static final int BLOCK_PAGE_PAGE_SHIFT = 6;
    public static final int BLOCKS_PER_PAGE = 1 << BLOCK_PAGE_PAGE_SHIFT;
    public static final int OFFSET = FileStore.HEADER_LENGTH;
    static final int FREE_PAGE = -1;
    // TODO storage: header should probably be 4 KB or so (to match block size of operating system)
    private Database database;
    private String fileName;
    private FileStore file;
    private BitField used;
    private BitField deleted;
    private HashSet potentiallyFreePages;
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
    private ObjectArray redoBuffer;
    private int redoBufferSize;
    private int readCount, writeCount;
    private String mode;
    private int nextDeleteId = 1;

    public DiskFile(Database database, String fileName, String mode, boolean dataFile, boolean logChanges, int cacheSize) throws SQLException {
        reset();
        this.database = database;
        this.log = database.getLog();
        this.fileName = fileName;
        this.mode = mode;
        this.dataFile = dataFile;
        this.logChanges = logChanges;
        String cacheType = database.getCacheType();
        if (Cache2Q.TYPE_NAME.equals(cacheType)) {
            this.cache = new Cache2Q(this, cacheSize);
        } else {
            this.cache = new CacheLRU(this, cacheSize);
        }
        rowBuff = DataPage.create(database, BLOCK_SIZE);
        // TODO: the overhead is larger in the log file, so this value is too
        // high :-(
        recordOverhead = 4 * rowBuff.getIntLen() + 1 + rowBuff.getFillerLength();
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
        redoBuffer = new ObjectArray();
        potentiallyFreePages = new HashSet();
    }

    private void setBlockCount(int count) {
        fileBlockCount = count;
        int pages = getPage(count);
        while (pages >= pageOwners.size()) {
            pageOwners.add(FREE_PAGE);
        }
    }

    int getBlockCount() {
        return fileBlockCount;
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
        for (int i = 0; i < pageOwners.size(); i++) {
            if (pageOwners.get(i) != FREE_PAGE && isPageFree(i)) {
                setPageOwner(i, FREE_PAGE);
            }
        }
    }

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
                ObjectArray storages = new ObjectArray();
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
                    Storage storage = (Storage) storages.get(i);
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

    boolean isPageFree(int page) {
        for (int i = page * BLOCKS_PER_PAGE; i < (page + 1) * BLOCKS_PER_PAGE; i++) {
            if (used.get(i)) {
                return false;
            }
        }
        return true;
    }

    public void initFromSummary(byte[] summary) {
        synchronized (database) {
            if (summary == null || summary.length == 0) {
                ObjectArray list = database.getAllStorages();
                for (int i = 0; i < list.size(); i++) {
                    Storage s = (Storage) list.get(i);
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
                    database.getTrace(Trace.DATABASE).info(
                            "unexpected size " + b2 + " when initializing summary for " + fileName + " expected:"
                                    + fileBlockCount);
                    return;
                }
                stage++;
                for (int i = 0, x = 0; i < b2 / 8; i++) {
                    int mask = in.read();
                    if (init) {
                        for (int j = 0; j < 8; j++, x++) {
                            if (used.get(x) != ((mask & (1 << j)) != 0)) {
                                throw Message.getInternalError("Redo failure, block: " + x + " expected in-use bit: " + used.get(x));
                            }
                        }
                    } else {
                        for (int j = 0; j < 8; j++, x++) {
                            if ((mask & (1 << j)) != 0) {
                                used.set(x);
                            }
                        }
                    }
                }
                stage++;
                int len = in.readInt();
                ObjectArray storages = new ObjectArray();
                for (int i = 0; i < len; i++) {
                    int s = in.readInt();
                    while (storages.size() <= s) {
                        storages.add(null);
                    }
                    if (init) {
                        int old = getPageOwner(i);
                        if (old != -1 && old != s) {
                            throw Message.getInternalError("Redo failure, expected page owner: " + old + " got: " + s);
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
                    Storage storage = (Storage) storages.get(s);
                    if (init) {
                        if (storage != null) {
                            int current = storage.getRecordCount();
                            if (current != recordCount) {
                                throw Message.getInternalError("Redo failure, expected row count: " + current + " got: " + recordCount);
                            }
                        }
                    } else {
                        storage.setRecordCount(recordCount);
                    }
                }
                stage++;
                freeUnusedPages();
                init = true;
            } catch (Exception e) {
                database.getTrace(Trace.DATABASE).error(
                        "error initializing summary for " + fileName + " size:" + summary.length + " stage:" + stage, e);
                // ignore - init is still false in this case
            }
        }
    }


    public void init() throws SQLException {
        synchronized (database) {
            if (init) {
                return;
            }
            ObjectArray storages = database.getAllStorages();
            for (int i = 0; i < storages.size(); i++) {
                Storage s = (Storage) storages.get(i);
                if (s != null && s.getDiskFile() == this) {
                    s.setRecordCount(0);
                }
            }
            int blockHeaderLen = Math.max(Constants.FILE_BLOCK_SIZE, 2 * rowBuff.getIntLen());
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
                    throw Message.getInternalError();
                }
                if (blockCount == 0) {
                    setUnused(null, i, 1);
                    i++;
                } else {
                    int id = s.readInt();
                    if (SysProperties.CHECK && id < 0) {
                        throw Message.getInternalError();
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

    public void flush() throws SQLException {
        synchronized (database) {
            database.checkPowerOff();
            ObjectArray list = cache.getAllChanged();
            CacheObject.sort(list);
            for (int i = 0; i < list.size(); i++) {
                Record rec = (Record) list.get(i);
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
//                if (writeRecord != null && (deletePos < 0 || writeRecord.getPos() < deletePos)) {
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

    public void close() throws SQLException {
        synchronized (database) {
            SQLException closeException = null;
            if (!database.getReadOnly()) {
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
                s.readInt(); // blockCount
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
            if (SysProperties.CHECK && storageId != id) {
                throw Message.getInternalError("File ID mismatch got=" + id + " expected=" + storageId + " pos=" + pos
                        + " " + logChanges + " " + this + " blockCount:" + blockCount);
            }
            if (SysProperties.CHECK && blockCount == 0) {
                throw Message.getInternalError("0 blocks to read pos=" + pos);
            }
            if (blockCount > 1) {
                byte[] b2 = new byte[blockCount * BLOCK_SIZE];
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

    int allocate(Storage storage, int blockCount) throws SQLException {
        reuseSpace();
        synchronized (database) {
            if (file == null) {
                throw Message.getSQLException(ErrorCode.SIMULATED_POWER_OFF);
            }
            blockCount = getPage(blockCount + BLOCKS_PER_PAGE - 1) * BLOCKS_PER_PAGE;
            int lastPage = getPage(getBlockCount());
            int pageCount = getPage(blockCount);
            int pos = -1;
            boolean found = false;
            for (int i = 0; i < lastPage; i++) {
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
                int max = getBlockCount();
                pos = MathUtils.roundUp(max, BLOCKS_PER_PAGE);
                if (rowBuff instanceof DataPageText) {
                    if (pos > max) {
                        writeDirectDeleted(max, pos - max);
                    }
                    writeDirectDeleted(pos, blockCount);
                } else {
                    long min = ((long) pos + blockCount) * BLOCK_SIZE;
                    min = MathUtils.scaleUp50Percent(Constants.FILE_MIN_SIZE, min, Constants.FILE_PAGE_SIZE, Constants.FILE_MAX_INCREMENT) + OFFSET;
                    if (min > file.length()) {
                        file.setLength(min);
                        database.notifyFileSize(min);
                    }
                }
            }
            setBlockOwner(null, storage, pos, blockCount, false);
            for (int i = 0; i < blockCount; i++) {
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

    void reuseSpace() throws SQLException {
        if (SysProperties.REUSE_SPACE_QUICKLY) {
            if (potentiallyFreePages.size() >= SysProperties.REUSE_SPACE_AFTER) {
                Session[] sessions = database.getSessions();
                int oldest = 0;
                for (int i = 0; i < sessions.length; i++) {
                    int deleteId = sessions[i].getLastUncommittedDelete();
                    if (oldest == 0 || (deleteId != 0 && deleteId < oldest)) {
                        oldest = deleteId;
                    }
                }
                for (Iterator it = potentiallyFreePages.iterator(); it.hasNext();) {
                    int p = ((Integer) it.next()).intValue();
                    if (oldest == 0) {
                        setPageOwner(p, FREE_PAGE);
                        it.remove();
                    }
                }
            }
        }
    }

    void uncommittedDelete(Session session) throws SQLException {
        if (session != null && logChanges && SysProperties.REUSE_SPACE_QUICKLY) {
            int deleteId = session.getLastUncommittedDelete();
            if (deleteId == 0 || deleteId < nextDeleteId) {
                deleteId = ++nextDeleteId;
                session.setLastUncommittedDelete(deleteId);
            }
        }
    }

    void freePage(int page) throws SQLException {
        if (!logChanges) {
            setPageOwner(page, FREE_PAGE);
        } else {
            if (SysProperties.REUSE_SPACE_QUICKLY) {
                potentiallyFreePages.add(ObjectUtils.getInteger(page));
                reuseSpace();
            }
        }
    }

    int getPage(int pos) {
        return pos >>> BLOCK_PAGE_PAGE_SHIFT;
    }

    int getPageOwner(int page) {
        if (page * BLOCKS_PER_PAGE > fileBlockCount || page >= pageOwners.size()) {
            return FREE_PAGE;
        }
        return pageOwners.get(page);
    }

    public void setPageOwner(int page, int storageId) throws SQLException {
        int old = pageOwners.get(page);
        if (old == storageId) {
            return;
        }
        if (SysProperties.CHECK && old >= 0 && storageId >= 0 && old != storageId) {
            for (int i = 0; i < BLOCKS_PER_PAGE; i++) {
                if (used.get(i + page * BLOCKS_PER_PAGE)) {
                    throw Message.getInternalError(
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
                potentiallyFreePages.remove(ObjectUtils.getInteger(page));
            }
        }
        pageOwners.set(page, storageId);
    }

    void setUsed(int pos, int blockCount) {
        synchronized (database) {
            if (pos + blockCount > fileBlockCount) {
                setBlockCount(pos + blockCount);
            }
            used.setRange(pos, blockCount, true);
            deleted.setRange(pos, blockCount, false);
        }
    }

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

    /*
     * Must be synchronized externally
     */
    BitField getUsed() {
        return used;
    }

    void updateRecord(Session session, Record record) throws SQLException {
        synchronized (database) {
            record.setChanged(true);
            int pos = record.getPos();
            Record old = (Record) cache.update(pos, record);
            if (SysProperties.CHECK) {
                if (old != null) {
                    if (old != record) {
                        database.checkPowerOff();
                        throw Message.getInternalError("old != record old=" + old + " new=" + record);
                    }
                    int blockCount = record.getBlockCount();
                    for (int i = 0; i < blockCount; i++) {
                        if (deleted.get(i + pos)) {
                            throw Message.getInternalError("update marked as deleted: " + (i + pos));
                        }
                    }
                }
            }
            if (logChanges) {
                log.add(session, this, record);
            }
        }
    }

    void writeDirectDeleted(int recordId, int blockCount) throws SQLException {
        synchronized (database) {
            go(recordId);
            for (int i = 0; i < blockCount; i++) {
                file.write(freeBlock.getBytes(), 0, freeBlock.length());
            }
            free(recordId, blockCount);
        }
    }

    void writeDirect(Storage storage, int pos, byte[] data, int offset) throws SQLException {
        synchronized (database) {
            go(pos);
            file.write(data, offset, BLOCK_SIZE);
            setBlockOwner(null, storage, pos, 1, true);
        }
    }

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
                    throw Message.getInternalError();
                }
                if (blockCount == 0) {
                    blockCount = 1;
                }
                int id = s.readInt();
                if (SysProperties.CHECK && id < 0) {
                    throw Message.getInternalError();
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

    void addRecord(Session session, Record record) throws SQLException {
        synchronized (database) {
            if (logChanges) {
                log.add(session, this, record);
            }
            cache.put(record);
        }
    }

    /*
     * Must be synchronized externally
     */
    public Cache getCache() {
        return cache;
    }

    void free(int pos, int blockCount) {
        synchronized (database) {
            used.setRange(pos, blockCount, false);
        }
    }

    public int getRecordOverhead() {
        return recordOverhead;
    }

    public void truncateStorage(Session session, Storage storage, IntArray pages) throws SQLException {
        synchronized (database) {
            int storageId = storage.getId();
            // make sure the cache records of this storage are not flushed to disk
            // afterwards
            ObjectArray list = cache.getAllChanged();
            for (int i = 0; i < list.size(); i++) {
                Record r = (Record) list.get(i);
                if (r.getStorageId() == storageId) {
                    r.setChanged(false);
                }
            }
            int[] pagesCopy = new int[pages.size()];
            // can not use pages directly, because setUnused removes rows from there
            pages.toArray(pagesCopy);
            for (int i = 0; i < pagesCopy.length; i++) {
                int page = pagesCopy[i];
                if (logChanges) {
                    log.addTruncate(session, this, storageId, page * BLOCKS_PER_PAGE, BLOCKS_PER_PAGE);
                }
                for (int j = 0; j < BLOCKS_PER_PAGE; j++) {
                    Record r = (Record) cache.find(page * BLOCKS_PER_PAGE + j);
                    if (r != null) {
                        cache.remove(r.getPos());
                    }
                }
                deleted.setRange(page * BLOCKS_PER_PAGE, BLOCKS_PER_PAGE, true);
                setUnused(session, page * BLOCKS_PER_PAGE, BLOCKS_PER_PAGE);
            }
        }
    }

    public void sync() {
        synchronized (database) {
            if (file != null) {
                file.sync();
            }
        }
    }

    public boolean isDataFile() {
        return dataFile;
    }

    public void setLogChanges(boolean b) {
        synchronized (database) {
            this.logChanges = b;
        }
    }

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
                    throw Message.getInternalError("blockCount:" + blockCount + " length: " + all.length() * BLOCK_SIZE);
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

    public void flushRedoLog() throws SQLException {
        synchronized (database) {
            if (redoBuffer.size() == 0) {
                return;
            }
            redoBuffer.sort(new Comparator() {
                public int compare(Object o1, Object o2) {
                    RedoLogRecord e1 = (RedoLogRecord) o1;
                    RedoLogRecord e2 = (RedoLogRecord) o2;
                    int comp = e1.recordId - e2.recordId;
                    if (comp == 0) {
                        comp = e1.sequenceId - e2.sequenceId;
                    }
                    return comp;
                }
            });
            // first write all deleted entries
            RedoLogRecord last = null;
            for (int i = 0; i < redoBuffer.size(); i++) {
                RedoLogRecord entry = (RedoLogRecord) redoBuffer.get(i);
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
                RedoLogRecord entry = (RedoLogRecord) redoBuffer.get(i);
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

}
