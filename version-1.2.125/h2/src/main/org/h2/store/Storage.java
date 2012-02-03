/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
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

    /**
     * This value is used to indicate that the position is not yet known, and
     * space needs to be allocated.
     */
    public static final int ALLOCATE_POS = -1;
    private static final int FREE_LIST_SIZE = Math.max(1024, DiskFile.BLOCKS_PER_PAGE * 4);
    private DiskFile file;
    private int recordCount;
    private RecordReader reader;
    private int freeCount;
    private IntArray freeList = new IntArray();
    private IntArray pages = new IntArray();
    private int id;
    private Database database;
    private DataPage dummy;
    private int pageCheckIndex;

    /**
     * Create a new storage object for this file.
     *
     * @param database the database
     * @param file the file
     * @param reader the reader that can parse records
     * @param id the storage id
     */
    public Storage(Database database, DiskFile file, RecordReader reader, int id) {
        this.database = database;
        this.file = file;
        this.reader = reader;
        this.id = id;
        dummy = DataPage.create(database, 0);
    }

    /**
     * Get the record parser for this storage.
     *
     * @return the record parser
     */
    public RecordReader getRecordReader() {
        return reader;
    }

    /**
     * Increment the record count (used when initializing the file).
     */
    void incrementRecordCount() {
        recordCount++;
    }

    /**
     * Read a record from the file or cache.
     *
     * @param session the session
     * @param pos the position of the record
     * @return the record
     */
    public Record getRecord(Session session, int pos) throws SQLException {
        return file.getRecord(session, pos, reader, id);
    }

    /**
     * Read a record if it is stored at that location.
     *
     * @param session the session
     * @param pos the position where it is stored
     * @return the record or null
     */
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
                        pageIndex = pages.findNextIndexSorted(page);
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

    /**
     * Update an existing record.
     *
     * @param session the session
     * @param record the record
     */
    public void updateRecord(Session session, Record record) throws SQLException {
        record.setDeleted(false);
        file.updateRecord(session, record);
    }

    /**
     * Add or update a record in the file.
     *
     * @param session the session
     * @param record the record
     * @param pos the position (use ALLOCATE_POS to add a new record)
     */
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

    /**
     * Remove a record.
     *
     * @param session the session
     * @param pos where the record is stored
     */
    public void removeRecord(Session session, int pos) throws SQLException {
        checkOnePage();
        Record record = getRecord(session, pos);
        if (SysProperties.CHECK && record.isDeleted()) {
            Message.throwInternalError("duplicate delete " + pos);
        }
        record.setDeleted(true);
        int blockCount = record.getBlockCount();
        file.uncommittedDelete(session);
        free(pos, blockCount);
        recordCount--;
        file.removeRecord(session, pos, record, blockCount);
    }

    private void refillFreeList() {
        if (freeList.size() != 0 || freeCount == 0) {
            return;
        }
        BitField used = file.getUsed();
        for (int i = 0; i < pages.size(); i++) {
            int p = pages.get(i);
            int block = DiskFile.BLOCKS_PER_PAGE * p;
            for (int j = 0; j < DiskFile.BLOCKS_PER_PAGE; j++) {
                if (!used.get(block)) {
                    if (freeList.size() < FREE_LIST_SIZE) {
                        freeList.add(block);
                    } else {
                        return;
                    }
                }
                block++;
            }
        }
        // if we came here, all free records must be in the list
        // otherwise it would have returned early
        if (SysProperties.CHECK2 && freeCount > freeList.size()) {
            Message.throwInternalError("freeCount expected " + freeList.size() + ", got: " + freeCount);
        }
        freeCount = freeList.size();
    }

    private int allocate(int blockCount) throws SQLException {
        refillFreeList();
        if (freeList.size() > 0) {
            synchronized (database) {
                BitField used = file.getUsed();
                int lastPage = Integer.MIN_VALUE;
                int lastBlockLow  = Integer.MAX_VALUE;
                int lastBlockHigh = 0;

                nextEntry:
                for (int i = 0; i < freeList.size(); i++) {
                    int px = freeList.get(i);

                    if (px >= lastBlockLow && px <= lastBlockHigh) {
                        // we have already tested this block
                        // and found that it's used
                        continue;
                    }

                    if (used.get(px)) {
                        // sometimes some entries in the freeList
                        // are not free (free 2, free 1, allocate 1+2)
                        // these entries are removed right here
                        freeList.remove(i--);
                        continue;
                    }

                    lastBlockLow = px;
                    lastBlockHigh = px + blockCount - 1;

                    while (lastBlockHigh >= lastBlockLow) {
                        int page = file.getPage(lastBlockHigh);
                        if (page != lastPage) {
                            if (file.getPageOwner(page) != id) {
                                continue nextEntry;
                            }
                            lastPage = page;
                        }
                        if (used.get(lastBlockHigh)) {
                            continue nextEntry;
                        }
                        --lastBlockHigh;
                    }

                    // range found
                    int pos = px;
                    freeList.remove(i);
                    file.setUsed(pos, blockCount);
                    freeCount -= blockCount;
                    return pos;
                }
            }
        }
        int pos = file.allocate(this, blockCount);
        file.setUsed(pos, blockCount);
        freeCount -= blockCount;
        return pos;
    }

    /**
     * Called after a record has been deleted.
     *
     * @param pos the position
     * @param blockCount the number of blocks
     */
    void free(int pos, int blockCount) {
        file.free(pos, blockCount);
        if (freeList.size() < FREE_LIST_SIZE) {
            freeList.add(pos);
        }
        freeCount += blockCount;
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
     * Get the unique storage id.
     *
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * Get the number of records in this storage.
     *
     * @return the number of records
     */
    public int getRecordCount() {
        return recordCount;
    }

    /**
     * Delete all records from this storage.
     *
     * @param session the session
     */
    public void truncate(Session session) throws SQLException {
        freeList = new IntArray();
        freeCount = 0;
        recordCount = 0;
        file.truncateStorage(session, this, pages);
    }

    /**
     * Set the record parser for this storage.
     *
     * @param reader the record parser
     */
    public void setReader(RecordReader reader) {
        this.reader = reader;
    }

    /**
     * Write this record now.
     *
     * @param rec the record to write
     */
    public void flushRecord(Record rec) throws SQLException {
        file.writeBack(rec);
    }

    /**
     * Get the overhead to store a record (header data) in number of bytes.
     *
     * @return the overhead
     */
    public int getRecordOverhead() {
        return file.getRecordOverhead();
    }

    public DiskFile getDiskFile() {
        return file;
    }

    /**
     * Update the record count.
     *
     * @param recordCount the new record count
     */
    public void setRecordCount(int recordCount) {
        this.recordCount = recordCount;
    }

    /**
     * Add a page to this storage.
     *
     * @param i the page id to add
     */
    void addPage(int i) {
        pages.addValueSorted(i);
    }

    /**
     * Remove a list of page from this storage.
     *
     * @param removeSorted the pages to remove
     */
    void removePages(IntArray removeSorted) {
        pages.removeAllSorted(removeSorted);
    }

    /**
     * Remove a page from this storage.
     *
     * @param i the page to remove
     */
    void removePage(int i) {
        int idx = pages.findIndexSorted(i);
        if (idx != -1) {
            pages.remove(idx);
        }
    }

    private void checkOnePage() throws SQLException {
        int size = pages.size();
        if (size > 0) {
            pageCheckIndex = (pageCheckIndex + 1) % size;
            int page = pages.get(pageCheckIndex);
            if (file.isPageFree(page) && file.getPageOwner(page) == id) {
                file.freePage(page);
            }
        }
    }

}
