/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;

import org.h2.constant.SysProperties;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.util.BitField;
import org.h2.util.IntArray;
import org.h2.util.MathUtils;

/**
 * This class represents an persistent container that stores data of a table or
 * an index. An object contains a list of records, see {@link Record}. For each
 * storage there is a {@link RecordReader} object that knows how to convert
 * records into a byte array and vice versa. The data is stored in a
 * {@link DiskFile}. A storage occupies a number of pages in a file.
 * File format:
 * 
 * <pre>
 * int block size
 * int storage id
 * record data
 * byte checksum
 * [bytes * fillerLength]
 * </pre>
 */
public class Storage {

    public static final int ALLOCATE_POS = -1;
    private static final int FREE_LIST_SIZE = Math.max(1024, DiskFile.BLOCKS_PER_PAGE * 4);
    private DiskFile file;
    private int recordCount;
    private RecordReader reader;
    private IntArray freeList = new IntArray();
    private IntArray pages = new IntArray();
    private int id;
    private Database database;
    private DataPage dummy;
    private int pageCheckIndex;

    public Storage(Database database, DiskFile file, RecordReader reader, int id) {
        this.database = database;
        this.file = file;
        this.reader = reader;
        this.id = id;
        dummy = DataPage.create(database, 0);
    }

    public RecordReader getRecordReader() {
        return reader;
    }

    void incrementRecordCount() {
        recordCount++;
    }

    public Record getRecord(Session session, int pos) throws SQLException {
        return file.getRecord(session, pos, reader, id);
    }

    public Record getRecordIfStored(Session session, int pos) throws SQLException {
        return file.getRecordIfStored(session, pos, reader, id);
    }

    /**
     * Gets the position of the next record.
     * @param record the last record (null to get the first record)
     * @return -1 if no record is found, otherwise the position
     */
    public int getNext(Record record) {
        int next;
        int lastCheckedPage;
        int pageIndex = -1;
        if (record == null) {
            if (pages.size() == 0) {
                return -1;
            }
            pageIndex = 0;
            lastCheckedPage = pages.get(0);
            next = lastCheckedPage * DiskFile.BLOCKS_PER_PAGE;
        } else {
            int blockCount = record.getBlockCount();
            lastCheckedPage = file.getPage(record.getPos());
            next = record.getPos() + blockCount;
        }
        synchronized (database) {
            BitField used = file.getUsed();
            while (true) {
                int page = file.getPage(next);
                if (lastCheckedPage != page) {
                    if (pageIndex < 0) {
                        pageIndex = pages.findNextValueIndex(page);
                    } else {
                        pageIndex++;
                    }
                    if (pageIndex >= pages.size()) {
                        return -1;
                    }
                    lastCheckedPage = pages.get(pageIndex);
                    next = Math.max(next, DiskFile.BLOCKS_PER_PAGE * lastCheckedPage);
                }
                if (used.get(next)) {
                    return next;
                }
                if (used.getLong(next) == 0) {
                    next = MathUtils.roundUp(next + 1, 64);
                } else {
                    next++;
                }
            }
        }
    }

    public void updateRecord(Session session, Record record) throws SQLException {
        record.setDeleted(false);
        file.updateRecord(session, record);
    }

    public void addRecord(Session session, Record record, int pos) throws SQLException {
        record.setStorageId(id);
        int size = file.getRecordOverhead() + record.getByteCount(dummy);
        size = MathUtils.roundUp(size, DiskFile.BLOCK_SIZE);
        record.setDeleted(false);
        int blockCount = size / DiskFile.BLOCK_SIZE;
        if (pos == ALLOCATE_POS) {
            pos = allocate(blockCount);
        } else {
            file.setUsed(pos, blockCount);
        }
        record.setPos(pos);
        record.setBlockCount(blockCount);
        record.setChanged(true);
        recordCount++;
        file.addRecord(session, record);
    }

    public void removeRecord(Session session, int pos) throws SQLException {
        checkOnePage();
        Record record = getRecord(session, pos);
        if (SysProperties.CHECK && record.getDeleted()) {
            throw Message.getInternalError("duplicate delete " + pos);
        }
        record.setDeleted(true);
        int blockCount = record.getBlockCount();
        file.uncommittedDelete(session);
        free(pos, blockCount);
        recordCount--;
        file.removeRecord(session, pos, record, blockCount);
    }

    private boolean isFreeAndMine(int pos, int blocks) {
        synchronized (database) {
            BitField used = file.getUsed();
            for (int i = blocks + pos - 1; i >= pos; i--) {
                if (file.getPageOwner(file.getPage(i)) != id || used.get(i)) {
                    return false;
                }
            }
            return true;
        }
    }

    public int allocate(int blockCount) throws SQLException {
        if (freeList.size() > 0) {
            synchronized (database) {
                BitField used = file.getUsed();
                for (int i = 0; i < freeList.size(); i++) {
                    int px = freeList.get(i);
                    if (used.get(px)) {
                        // sometime there may stay some entries in the freeList 
                        // that are not free (free 2, free 1, allocate 1+2)
                        // these entries are removed right here
                        freeList.remove(i--);
                    } else {
                        if (isFreeAndMine(px, blockCount)) {
                            int pos = px;
                            freeList.remove(i--);
                            file.setUsed(pos, blockCount);
                            return pos;
                        }
                    }
                }
            }
        }
        int pos = file.allocate(this, blockCount);
        file.setUsed(pos, blockCount);
        return pos;
    }

    void free(int pos, int blockCount) {
        file.free(pos, blockCount);
        if (freeList.size() < FREE_LIST_SIZE) {
            freeList.add(pos);
        }
    }

    public void delete(Session session) throws SQLException {
        truncate(session);
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

    public int getId() {
        return id;
    }

    public int getRecordCount() {
        return recordCount;
    }

    public void truncate(Session session) throws SQLException {
        freeList = new IntArray();
        recordCount = 0;
        file.truncateStorage(session, this, pages);
    }

    public void setReader(RecordReader reader) {
        this.reader = reader;
    }

    public void flushRecord(Record rec) throws SQLException {
        file.writeBack(rec);
    }

    public void flushFile() throws SQLException {
        file.flush();
    }

    public int getRecordOverhead() {
        return file.getRecordOverhead();
    }

    public DiskFile getDiskFile() {
        return file;
    }

    public void setRecordCount(int recordCount) {
        this.recordCount = recordCount;
    }

    void addPage(int i) {
        pages.addValueSorted(i);
    }

    void removePage(int i) {
        pages.removeValue(i);
    }

    private void checkOnePage() throws SQLException {
        pageCheckIndex = (pageCheckIndex + 1) % pages.size();
        int page = pages.get(pageCheckIndex);
        if (file.isPageFree(page) && file.getPageOwner(page) == id) {
            file.freePage(page);
        }
    }

}
