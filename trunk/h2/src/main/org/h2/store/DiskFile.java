/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Comparator;

import org.h2.api.DatabaseEventListener;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
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

/**
 * @author Thomas
 */
public class DiskFile implements CacheWriter {

    public static final int BLOCK_SIZE = 128;
    // each page contains blocks from the same storage
    static final int BLOCK_PAGE_PAGE_SHIFT = 6;
    public static final int BLOCKS_PER_PAGE = 1 << BLOCK_PAGE_PAGE_SHIFT;
    public static final int OFFSET = FileStore.HEADER_LENGTH;
    // TODO storage: header should probably be 4 KB or so (to match block size of operating system)
    private Database database;
    private String fileName;
    private FileStore file;
    private BitField used = new BitField();
    private BitField deleted = new BitField();
    private int fileBlockCount;
    private IntArray pageOwners = new IntArray();
    private Cache cache;
    private LogSystem log;
    private DataPage rowBuff;
    private DataPage freeBlock;
    private boolean dataFile;
    private boolean logChanges;
    private int recordOverhead;
    private boolean init, initAlreadyTried;
    private ObjectArray redoBuffer = new ObjectArray();
    private int redoBufferSize;
    private int readCount, writeCount;

    public DiskFile(Database database, String fileName, boolean dataFile, boolean logChanges, int cacheSize) throws SQLException {
        this.database = database;
        this.log = database.getLog();
        this.fileName = fileName;
        this.dataFile = dataFile;
        this.logChanges = logChanges;
        String cacheType = database.getCacheType();
        if(Cache2Q.TYPE_NAME.equals(cacheType)) {
            this.cache = new Cache2Q(this, cacheSize);
        } else {
            this.cache = new CacheLRU(this, cacheSize);
        }
        rowBuff = DataPage.create(database, BLOCK_SIZE);
        // TODO: the overhead is larger in the log file, so this value is too high :-(
        recordOverhead = 4 * rowBuff.getIntLen() + 1 + rowBuff.getFillerLength();
        freeBlock = DataPage.create(database, BLOCK_SIZE);
        freeBlock.fill(BLOCK_SIZE);
        freeBlock.updateChecksum();
        try {
            if(FileUtils.exists(fileName)) {
                file = database.openFile(fileName, true);
                long length = file.length();
                database.notifyFileSize(length);
                int blocks = (int)((length - OFFSET) / BLOCK_SIZE);
                setBlockCount(blocks);
            } else {
                create();
            }
        } catch(SQLException e) {
            close();
            throw e;
        }
    }

    private void setBlockCount(int count) {
        fileBlockCount = count;
        int pages = getPage(count);
        while(pages >= pageOwners.size()) {
            pageOwners.add(-1);
        }
    }

    int getBlockCount() {
        return fileBlockCount;
    }

    private void create() throws SQLException {
        try {
            file =  database.openFile(fileName, false);
            DataPage header = DataPage.create(database, OFFSET);
            file.seek(FileStore.HEADER_LENGTH);
            header.fill(OFFSET);
            header.updateChecksum();
            file.write(header.getBytes(), 0, OFFSET);
        } catch (Exception e) {
            throw Message.convert(e);
        }
    }

    private void freeUnusedPages() throws SQLException {
        for(int i=0; i<pageOwners.size(); i++) {
            if(pageOwners.get(i) != -1 && isPageFree(i)) {
                setPageOwner(i, -1);
            }
        }
    }

    public synchronized byte[] getSummary() throws SQLException {
        try {
            ByteArrayOutputStream buff = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buff);
            int blocks = (int)((file.length() - OFFSET) / BLOCK_SIZE);
            out.writeInt(blocks);
            for(int i=0, x = 0; i<blocks / 8; i++) {
                int mask = 0;
                for(int j=0; j<8; j++) {
                    if(used.get(x)) {
                        mask |= 1 << j ;
                    }
                    x++;
                }
                out.write(mask);
            }
            out.writeInt(pageOwners.size());
            ObjectArray storages = new ObjectArray();
            for(int i=0; i<pageOwners.size(); i++) {
                int s = pageOwners.get(i);
                out.writeInt(s);
                if(s >= 0 && (s >= storages.size() || storages.get(s) == null)) {
                    Storage storage = database.getStorage(s, this);
                    while(storages.size() <= s) {
                        storages.add(null);
                    }
                    storages.set(s, storage);
                }
            }
            for(int i=0; i<storages.size(); i++) {
                Storage storage = (Storage) storages.get(i);
                if(storage != null) {
                    out.writeInt(i);
                    out.writeInt(storage.getRecordCount());
                }
            }
            out.writeInt(-1);
            out.close();
            byte[] b2 = buff.toByteArray();
            return b2;
        } catch(IOException e) {
            // will probably never happend, because only in-memory strutures are used
            return null;
        }
    }

    private boolean isPageFree(int page) {
        for(int i=page * BLOCKS_PER_PAGE; i < (page+1) * BLOCKS_PER_PAGE; i++) {
            if(used.get(i)) {
                return false;
            }
        }
        return true;
    }

    public synchronized void initFromSummary(byte[] summary) {
        if(summary == null || summary.length==0) {
            init = false;
            return;
        }
        if(database.getRecovery() || initAlreadyTried) {
            return;
        }
        initAlreadyTried = true;
        int stage = 0;
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(summary));
            int b2 = in.readInt();
            stage++;
            for(int i=0, x = 0; i<b2 / 8; i++) {
                int mask = in.read();
                for(int j=0; j<8; j++) {
                    if((mask & (1 << j)) != 0) {
                        used.set(x);
                    }
                    x++;
                }
            }
            stage++;
            int len = in.readInt();
            ObjectArray storages = new ObjectArray();
            for(int i=0; i<len; i++) {
                int s = in.readInt();
                if(s>=0) {
                    Storage storage = database.getStorage(s, this);
                    while(storages.size() <= s) {
                        storages.add(null);
                    }
                    storages.set(s, storage);
                    storage.addPage(i);
                }
                setPageOwner(i, s);
            }
            stage++;
            while(true) {
                int s = in.readInt();
                if(s < 0) {
                    break;
                }
                int recordCount = in.readInt();
                Storage storage = (Storage) storages.get(s);
                storage.setRecordCount(recordCount);
            }
            stage++;
            freeUnusedPages();
            init = true;
        } catch (Exception e) {
            database.getTrace(Trace.DATABASE).error("error initializing summary for " +fileName+" size:" +summary.length + " stage:"+stage, e);
            // ignore - init is still false in this case
        }
    }

    public synchronized void init() throws SQLException {
        if(init) {
            return;
        }
        ObjectArray storages = database.getAllStorages();
        for(int i=0; i<storages.size(); i++) {
            Storage s = (Storage) storages.get(i);
            if(s != null && s.getDiskFile() == this) {
                s.setRecordCount(0);
            }
        }
        try {
            int blockHeaderLen = Math.max(Constants.FILE_BLOCK_SIZE, 2 * rowBuff.getIntLen());
            byte[] buff = new byte[blockHeaderLen];
            DataPage s = DataPage.create(database, buff);
            long time = 0;
            for (int i = 0; i < fileBlockCount;) {
                long t2 = System.currentTimeMillis();
                if(t2 > time + 10) {
                    time = t2;
                    database.setProgress(DatabaseEventListener.STATE_SCAN_FILE, this.fileName, i, fileBlockCount);
                }
                go(i);
                file.readFully(buff, 0, blockHeaderLen);
                s.reset();
                int blockCount = s.readInt();
                if(Constants.CHECK && blockCount < 0) {
                    throw Message.getInternalError();
                }
                if(blockCount == 0) {
                    setUnused(i, 1);
                    i++;
                } else {
                    int id = s.readInt();
                    if(Constants.CHECK && id < 0) {
                        throw Message.getInternalError();
                    }
                    Storage storage = database.getStorage(id, this);
                    setBlockOwner(storage, i, blockCount, true);
                    storage.incrementRecordCount();
                    i += blockCount;
                }
            }
            database.setProgress(DatabaseEventListener.STATE_SCAN_FILE, this.fileName, fileBlockCount, fileBlockCount);
            init = true;
        } catch (Exception e) {
            throw Message.convert(e);
        }
    }

    synchronized void flush() throws SQLException {
        ObjectArray list = cache.getAllChanged();
        CacheObject.sort(list);
        for(int i=0; i<list.size(); i++) {
            Record rec = (Record) list.get(i);
            writeBack(rec);
        }
        // TODO flush performance: maybe it would be faster to write records in the same loop
        for(int i=0; i<fileBlockCount; i++) {
            i = deleted.nextSetBit(i);
            if(i < 0) {
                break;
            }
            if(deleted.get(i)) {
                writeDirectDeleted(i, 1);
                deleted.clear(i);
            }
        }
    }

    public synchronized void close() throws SQLException {
        SQLException closeException = null;
        if(!database.getReadOnly()) {
            try {
                flush();
            } catch (SQLException e) {
                closeException = e;
            }
        }
        cache.clear();
        // continue with close even if flush was not possible (file storage problem)
        if(file != null) {
            file.closeSilently();
            file = null;
        }
        if(closeException != null) {
            throw closeException;
        }
        readCount = writeCount = 0;
    }

    private void go(int block) throws SQLException {
        file.seek(getFilePos(block));
    }
    
    private long getFilePos(int block) {
        return ((long)block * BLOCK_SIZE) + OFFSET;
    }

    synchronized Record getRecordIfStored(int pos, RecordReader reader, int storageId) throws SQLException {
        try {
            int owner = getPageOwner(getPage(pos));
            if(owner != storageId) {
                return null;
            }
            go(pos);
            rowBuff.reset();
            byte[] buff = rowBuff.getBytes();
            file.readFully(buff, 0, BLOCK_SIZE);
            DataPage s = DataPage.create(database, buff);
            s.readInt(); // blockCount
            int id = s.readInt();
            if(id != storageId) {
                return null;
            }
        } catch (Exception e) {
            return null;
        }
        return getRecord(pos, reader, storageId);
    }
    
    synchronized Record getRecord(int pos, RecordReader reader, int storageId) throws SQLException {
        if(file == null) {
            throw Message.getSQLException(Message.SIMULATED_POWER_OFF);
        }
        synchronized(this) {
            Record record = (Record) cache.get(pos);
            if(record != null) {
                return record;
            }
            readCount++;
            try {
                go(pos);
                rowBuff.reset();
                byte[] buff = rowBuff.getBytes();
                file.readFully(buff, 0, BLOCK_SIZE);
                DataPage s = DataPage.create(database, buff);
                int blockCount = s.readInt();
                int id = s.readInt();
                if(Constants.CHECK && storageId != id) {
                    throw Message.getInternalError("File ID mismatch got="+id+" expected="+storageId+" pos="+pos+" "+logChanges+" "+this +" blockCount:"+blockCount);
                }
                if(Constants.CHECK && blockCount == 0) {
                    throw Message.getInternalError("0 blocks to read pos="+pos);
                }
                if(blockCount > 1) {
                    byte[] b2 = new byte[blockCount * BLOCK_SIZE];
                    System.arraycopy(buff, 0, b2, 0, BLOCK_SIZE);
                    buff = b2;
                    file.readFully(buff, BLOCK_SIZE, blockCount * BLOCK_SIZE - BLOCK_SIZE);
                    s = DataPage.create(database, buff);
                    s.readInt();
                    s.readInt();
                }
                s.check(blockCount*BLOCK_SIZE);
                Record r = reader.read(s);
                r.setStorageId(storageId);
                r.setPos(pos);
                r.setBlockCount(blockCount);
                cache.put(r);
                return r;
            } catch (Exception e) {
                throw Message.convert(e);
            }
        }
    }

    synchronized int allocate(Storage storage, int blockCount) throws SQLException {
        if(file == null) {
            throw Message.getSQLException(Message.SIMULATED_POWER_OFF);
        }
        blockCount = getPage(blockCount+BLOCKS_PER_PAGE-1)*BLOCKS_PER_PAGE;
        int lastPage = getPage(getBlockCount());
        int pageCount = getPage(blockCount);
        int pos = -1;
        boolean found = false;
        for (int i = 0; i < lastPage; i++) {
            found = true;
            for(int j = i; j < i + pageCount; j++) {
                if(j >= lastPage || getPageOwner(j) != -1) {
                    found = false;
                    break;
                }
            }
            if(found) {
                pos = i * BLOCKS_PER_PAGE;
                break;
            }
        }
        if(!found) {
            int max = getBlockCount();
            pos = MathUtils.roundUp(max, BLOCKS_PER_PAGE);
            if(rowBuff instanceof DataPageText) {
                if(pos > max) {
                    writeDirectDeleted(max, pos-max);
                }
                writeDirectDeleted(pos, blockCount);
            } else {
                long min = ((long)pos + blockCount) * BLOCK_SIZE ;
                min = MathUtils.scaleUp50Percent(128 * 1024, min, 8 * 1024) + OFFSET;
                if(min > file.length()) {
                    file.setLength(min);
                    database.notifyFileSize(min);
                }
            }
        }
        setBlockOwner(storage, pos, blockCount, false);
        for(int i=0; i<blockCount; i++) {
            storage.free(i + pos, 1);
        }
        return pos;
    }

    private void setBlockOwner(Storage storage, int pos, int blockCount, boolean inUse) throws SQLException {
        if (pos + blockCount > fileBlockCount) {
            setBlockCount(pos + blockCount);
        }
        if(!inUse) {
            setUnused(pos, blockCount);
        }
        for(int i = getPage(pos); i <= getPage(pos+blockCount-1); i++) {
            setPageOwner(i, storage.getId());
        }
        if(inUse) {
            used.setRange(pos, blockCount, true);
            deleted.setRange(pos, blockCount, false);
        }
    }

    private void setUnused(int pos, int blockCount) throws SQLException {
        if (pos + blockCount > fileBlockCount) {
            setBlockCount(pos + blockCount);
        }
        for (int i = pos; i < pos + blockCount; i++) {
            used.clear(i);
            if((i % BLOCKS_PER_PAGE == 0) && (pos + blockCount >= i + BLOCKS_PER_PAGE)) {
                // if this is the first page of a block and if the whole page is free
                setPageOwner(getPage(i), -1);
            }
        }
    }

    int getPage(int pos) {
        return pos >>> BLOCK_PAGE_PAGE_SHIFT;
    }

    int getPageOwner(int page) {
        if(page * BLOCKS_PER_PAGE > fileBlockCount || page >= pageOwners.size()) {
            return -1;
        }
        return pageOwners.get(page);
    }

    synchronized void setPageOwner(int page, int storageId) throws SQLException {
        int old = pageOwners.get(page);
        if(old == storageId) {
            return;
        }
        if(Constants.CHECK &&  old >= 0 && storageId >= 0 && old != storageId) {
            for(int i=0; i<BLOCKS_PER_PAGE; i++) {
                if(used.get(i + page*BLOCKS_PER_PAGE)) {
                    throw Message.getInternalError("double allocation");
                }
            }
        }
        if(old >= 0) {
            database.getStorage(old, this).removePage(page);
            if(!logChanges) {
                // need to clean the page, otherwise it may never get cleaned and can become corrupted
                writeDirectDeleted(page * BLOCKS_PER_PAGE, BLOCKS_PER_PAGE);
            }
        }
        if(storageId >= 0) {
            database.getStorage(storageId, this).addPage(page);
        }
        pageOwners.set(page, storageId);
    }

    synchronized void setUsed(int pos, int blockCount) {
        if (pos + blockCount > fileBlockCount) {
            setBlockCount(pos + blockCount);
        }
        used.setRange(pos, blockCount, true);
        deleted.setRange(pos, blockCount, false);
    }

//    public void finalize() {
//        if (!Database.RUN_FINALIZERS) {
//            return;
//        }
//        if (file != null) {
//            try {
//                file.close();
//            } catch (Exception e) {
//                // ignore
//            }
//        }
//    }

    public synchronized void delete() throws SQLException {
        try {
            cache.clear();
            file.close();
            FileUtils.delete(fileName);
        } catch (Exception e) {
            throw Message.convert(e);
        } finally {
            file = null;
            fileName = null;
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

    public synchronized void writeBack(CacheObject obj) throws SQLException {
        writeCount++;
        Record record = (Record) obj;
        synchronized(this) {
            try {
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
            } catch (Exception e) {
                throw Message.convert(e);
            }
        }
        record.setChanged(false);
    }

    /*
     * Must be synchronized externally
     */
    BitField getUsed() {
        return used;
    }

    synchronized void updateRecord(Session session, Record record) throws SQLException {
        try {
            record.setChanged(true);
            int pos = record.getPos();
            Record old = (Record) cache.update(pos, record);
            if(Constants.CHECK) {
                if(old != null) {
                    if(old!=record) {
                        database.checkPowerOff();
                        throw Message.getInternalError("old != record old="+old+" new="+record);
                    }
                    int blockCount = record.getBlockCount();
                    for(int i=0; i<blockCount; i++) {
                        if(deleted.get(i + pos)) {
                            throw Message.getInternalError("update marked as deleted: " + (i+pos));
                        }
                    }
                }
            }
            if(logChanges) {
                log.add(session, this, record);
            }
        } catch (Exception e) {
            throw Message.convert(e);
        }
    }

    synchronized void writeDirectDeleted(int recordId, int blockCount) throws SQLException {
        synchronized(this) {
            try {
                go(recordId);
                for(int i=0; i<blockCount; i++) {
                    file.write(freeBlock.getBytes(), 0, freeBlock.length());
                }
                free(recordId, blockCount);
            } catch (Exception e) {
                throw Message.convert(e);
            }
        }
    }

    synchronized void writeDirect(Storage storage, int pos, byte[] data, int offset) throws SQLException {
        synchronized(this) {
            try {
                go(pos);
                file.write(data, offset, BLOCK_SIZE);
                setBlockOwner(storage, pos, 1, true);
            } catch (Exception e) {
                throw Message.convert(e);
            }
        }
    }

    synchronized void removeRecord(Session session, int pos, Record record, int blockCount) throws SQLException {
        if(logChanges) {
            log.add(session, this, record);
        }
        cache.remove(pos);
        deleted.setRange(pos, blockCount, true);
        setUnused(pos, blockCount);
    }

    synchronized void addRecord(Session session, Record record) throws SQLException {
        if(logChanges) {
            log.add(session, this, record);
        }
        cache.put(record);
    }

    /*
     * Must be synchronized externally
     */
    public Cache getCache() {
        return cache;
    }

    synchronized void free(int pos, int blockCount) {
        used.setRange(pos, blockCount, false);
    }

    public int getRecordOverhead() {
        return recordOverhead;
    }

    public synchronized void truncateStorage(Session session, Storage storage, IntArray pages) throws SQLException {
        int storageId = storage.getId();
        // make sure the cache records of this storage are not flushed to disk afterwards
        ObjectArray list = cache.getAllChanged();
        for(int i=0; i<list.size(); i++) {
            Record r = (Record) list.get(i);
            if(r.getStorageId() == storageId) {
                r.setChanged(false);
            }
        }
        int[] pagesCopy = new int[pages.size()];
        // can not use pages directly, because setUnused removes rows from there
        pages.toArray(pagesCopy);
        for(int i=0; i<pagesCopy.length; i++) {
            int page = pagesCopy[i];
            if(logChanges) {
                log.addTruncate(session, this, storageId, page * BLOCKS_PER_PAGE, BLOCKS_PER_PAGE);
            }
            for(int j=0; j<BLOCKS_PER_PAGE; j++) {
                Record r = (Record) cache.find(page * BLOCKS_PER_PAGE + j);
                if(r != null) {
                    cache.remove(r.getPos());
                }
            }
            deleted.setRange(page * BLOCKS_PER_PAGE, BLOCKS_PER_PAGE, true);
            setUnused(page * BLOCKS_PER_PAGE, BLOCKS_PER_PAGE);
        }
    }

    public synchronized void sync() {
        if(file != null) {
            file.sync();
        }
    }

    public boolean isDataFile() {
        return dataFile;
    }

    public void setLogChanges(boolean b) {
        this.logChanges = b;
    }

    synchronized void addRedoLog(Storage storage, int recordId, int blockCount, DataPage rec) throws SQLException {
        byte[] data = null;
        if(rec != null) {
            DataPage all = rowBuff;
            all.reset();
            all.writeInt(blockCount);
            all.writeInt(storage.getId());
            all.writeDataPageNoSize(rec);
            // the buffer may have some additional fillers - just ignore them
            all.fill(blockCount * BLOCK_SIZE);
            all.updateChecksum();
            if(Constants.CHECK && all.length() != BLOCK_SIZE * blockCount) {
                throw Message.getInternalError("blockCount:" + blockCount + " length: " + all.length()*BLOCK_SIZE);
            }
            data = new byte[all.length()];
            System.arraycopy(all.getBytes(), 0, data, 0, all.length());
        }
        for(int i=0; i<blockCount; i++) {
            RedoLogRecord log = new RedoLogRecord();
            log.recordId = recordId + i;
            log.offset = i * BLOCK_SIZE;
            log.storage = storage;
            log.data= data;
            log.sequenceId = redoBuffer.size();
            redoBuffer.add(log);
            redoBufferSize += log.getSize();
        }
        if (redoBufferSize > Constants.REDO_BUFFER_SIZE) {
            flushRedoLog();
        }
    }

    synchronized void flushRedoLog() throws SQLException {
        if(redoBuffer.size() == 0) {
            return;
        }
        redoBuffer.sort(new Comparator() {
            public int compare(Object o1, Object o2) {
                RedoLogRecord e1 = (RedoLogRecord) o1;
                RedoLogRecord e2 = (RedoLogRecord) o2;
                int comp = e1.recordId - e2.recordId;
                if(comp == 0) {
                    comp = e1.sequenceId - e2.sequenceId;
                }
                return comp;
            }
        });
        RedoLogRecord last = null;
        for(int i=0; i<redoBuffer.size(); i++) {
            RedoLogRecord entry = (RedoLogRecord) redoBuffer.get(i);
            if(last != null && entry.recordId != last.recordId) {
                writeRedoLog(last);
            }
            last = entry;
        }
        if(last != null) {
            writeRedoLog(last);
        }
        redoBuffer.clear();
        redoBufferSize = 0;
    }

    private void writeRedoLog(RedoLogRecord entry) throws SQLException {
        if(entry.data == null) {
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

}
