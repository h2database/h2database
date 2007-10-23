/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.DataPage;
import org.h2.store.DiskFile;
import org.h2.store.Record;
import org.h2.store.RecordReader;
import org.h2.store.Storage;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;
import org.h2.value.Value;
import org.h2.value.ValueArray;

/**
 * @author Thomas
 */
public class LinearHashIndex extends BaseIndex implements RecordReader {

    // TODO index / linear hash: tune page size
    // private static final int MAX_PAGE_SIZE = 256;
    private static final int RECORDS_PER_BUCKET = 10;
    private static final int UTILIZATION_FOR_SPLIT = 70;
    private static final int UTILIZATION_FOR_MERGE = 60;
    private int readCount;
    // private static final boolean TRACE = false;
    private DiskFile diskFile;
    private Storage storage;
    private TableData tableData;
    private int bucketSize;
    private int blocksPerBucket;
    private int firstBucketBlock;
    private LinearHashHead head;
    private boolean needRebuild;
    // private ObjectArray buckets = new ObjectArray();

    public LinearHashIndex(Session session, TableData table, int id, String indexName, IndexColumn[] columns, IndexType indexType)
            throws SQLException {
        super(table, id, indexName, columns, indexType);
        this.tableData = table;
        // TODO linear hash: currently, changes are not logged
        String name = database.getName()+"."+id+Constants.SUFFIX_HASH_FILE;
        diskFile = new DiskFile(database, name, "rw", false, false, Constants.DEFAULT_CACHE_SIZE_LINEAR_INDEX);
        diskFile.init();
        bucketSize = 4 * DiskFile.BLOCK_SIZE - diskFile.getRecordOverhead();
        blocksPerBucket = 4;
        firstBucketBlock = 4;
        storage = database.getStorage(id, diskFile);
        storage.setReader(this);
        rowCount = table.getRowCount(session);
        int pos = storage.getNext(null);
        if (pos == -1) {
            truncate(session);
            needRebuild = true;
        } else {
            head = (LinearHashHead) storage.getRecord(session, pos);
        }
    }

    void writeHeader(Session session) throws SQLException {
        storage.updateRecord(session, head);
    }

//    public String getString() throws Exception {
//        // TODO index / linear hash: debug code here
//        StringBuffer buff = new StringBuffer();
//        buff.append("buckets " + bucketCount);
//        int records = 0;
//        int chained = 0;
//        int foreign = 0;
//        int access = 0;
//        for (int i = 0; i < bucketCount; i++) {
//            LinearHashBucket bucket = getBucket(i);
//            if (bucket == null) {
//                throw Message.internal("bucket=" + i + " is empty");
//            }
//            if (bucket.getRecordSize() > RECORDS_PER_BUCKET) {
//                throw Message.internal("bucket=" + i + " records=" + bucket.getRecordSize());
//            }
//            records += bucket.getRecordSize();
//            if (bucket.getNextBucket() != -1) {
//                chained++;
//            }
//            for (int j = 0; j < bucket.getRecordSize(); j++) {
//                LinearHashEntry record = bucket.getRecord(j);
//                if (record.home != i) {
//                    foreign++;
//                }
//                int oldReadCount = readCount;
//                get(record.key);
//                access += (readCount - oldReadCount);
//            }
//        }
//        buff.append(" records " + records);
//        buff.append(" utilization " + getUtilization());
//        buff.append(" access " + ((0.0 + access) / records));
//        buff.append(" chained " + chained);
//        buff.append(" foreign " + foreign);
//        if (TRACE) {
//            for (int i = 0; i < bucketCount; i++) {
//                LinearHashBucket bucket = getBucket(i);
//                int f = getForeignHome(i);
//                if (f >= 0) {
//                    buff.append(" from " + f);
//                }
//                buff.append(i);
//                buff.append(" next ");
//                buff.append(bucket.getNextBucket());
//                buff.append("{");
//                for (int j = 0; j < bucket.getRecordSize(); j++) {
//                    if (j > 0) {
//                        buff.append(", ");
//                    }
//                    LinearHashEntry r = bucket.getRecord(j);
//                    buff.append(r.key.toString());
//                    if (r.home != i && r.home != f) {
//                        throw new Exception("MULTIPLE LINKS TO! " + buff.toString());
//                    }
//                }
//                buff.append("} ");
//            }
//        }
//        return buff.toString();
//
//    }

    private void add(Session session, Value key, int value) throws SQLException {
        // trace.debug("add "+key.toString() + " " + value);
        if (getUtilization() >= UTILIZATION_FOR_SPLIT) {
            split(session);
        }
        int hash = key.hashCode();
        int home = getPos(hash);
        int index = home;
        LinearHashEntry record = new LinearHashEntry();
        record.hash = hash;
        record.key = key;
        record.home = home;
        record.value = value;
        int free = getNextFree(session, home);
        while (true) {

            LinearHashBucket bucket = getBucket(session, index);
            if (bucket.getRecordSize() < RECORDS_PER_BUCKET) {
                addRecord(session, bucket, record);
                break;
            }
            // this bucket is full
            int foreign = getForeignHome(session, index);
            if (foreign >= 0 && foreign != home) {
                // move out foreign records - add this record - add foreign
                // records again
                ObjectArray old = new ObjectArray();
                moveOut(session, foreign, old);
                addRecord(session, bucket, record);
                addAll(session, old);
                break;
            }
            // there is already a link to next
            if (bucket.getNextBucket() > 0) {
                index = bucket.getNextBucket();
                continue;
            }

            int nextFree = getNextFree(session, free);
            if (nextFree < 0) {
                // trace.debug("split because no chain " + head.bucketCount);
                split(session);
                add(session, key, value);
                break;
            }

            // it's possible that the bucket was removed from the cache (if searching for a bucket with space scanned many buckets)
            bucket = getBucket(session, index);

            bucket.setNext(session, free);
            free = nextFree;
            if (getForeignHome(session, free) >= 0) {
                throw Message.getInternalError("next already linked");
            }
            index = bucket.getNextBucket();
        }
    }

    private int getNextFree(Session session, int excluding) throws SQLException {
        for (int i = head.bucketCount - 1; i >= 0; i--) {
            LinearHashBucket bucket = getBucket(session, i);
            if (bucket.getRecordSize() >= RECORDS_PER_BUCKET) {
                continue;
            }
            if (getForeignHome(session, i) < 0 && i != excluding) {
                return i;
            }
        }
        return -1;
    }

    private int getForeignHome(Session session, int bucketId) throws SQLException {
        LinearHashBucket bucket = getBucket(session, bucketId);
        for (int i = 0; i < bucket.getRecordSize(); i++) {
            LinearHashEntry record = bucket.getRecord(i);
            if (record.home != bucketId) {
                return record.home;
            }
        }
        return -1;
    }

    public int getPos(int hash) {
        hash = Math.abs((hash << 7) - hash + (hash >>> 9) + (hash >>> 17));
        int pos = hash % (head.baseSize + head.baseSize);
        int len = head.bucketCount;
        return pos < len ? pos : (pos % head.baseSize);
    }

    private void split(Session session) throws SQLException {
        // trace.debug("split " + head.nextToSplit);
        ObjectArray old = new ObjectArray();
        moveOut(session, head.nextToSplit, old);
        head.nextToSplit++;
        if (head.nextToSplit >= head.baseSize) {
            head.baseSize += head.baseSize;
            head.nextToSplit = 0;
        }
        addBucket(session);
        addAll(session, old);
    }

    private void addAll(Session session, ObjectArray records) throws SQLException {
        for (int i = 0; i < records.size(); i++) {
            LinearHashEntry r = (LinearHashEntry) records.get(i);
            add(session, r.key, r.value);
        }
    }

    // moves all records of a bucket to the array (including chained)
    private void moveOut(Session session, int home, ObjectArray storeIn) throws SQLException {
        LinearHashBucket bucket = getBucket(session, home);
        int foreign = getForeignHome(session, home);
        while (true) {
            for (int i = 0; i < bucket.getRecordSize(); i++) {
                LinearHashEntry r = bucket.getRecord(i);
                if (r.home == home) {
                    storeIn.add(r);
                    removeRecord(session, bucket, i);
                    i--;
                }
            }
            if (foreign >= 0) {
                // this block contains foreign records
                // and therefore all home records have been found
                // (and it would be an error to set next to -1)
                moveOut(session, foreign, storeIn);
                if (SysProperties.CHECK && getBucket(session, foreign).getNextBucket() != -1) {
                    throw Message.getInternalError("moveOut " + foreign);
                }
                return;
            }
            int next = bucket.getNextBucket();
            if (SysProperties.CHECK && next >= head.bucketCount) {
                throw Message.getInternalError("next=" + next + " max=" + head.bucketCount);
            }
            if (next < 0) {
                break;
            }
            bucket.setNext(session, -1);
            bucket = getBucket(session, next);
        }
    }

    private void merge(Session session) throws SQLException {
        if (head.bucketCount <= 1) {
            return;
        }
        if (getUtilization() > UTILIZATION_FOR_MERGE) {
            return;
        }
        ObjectArray old = new ObjectArray();
        moveOut(session, head.nextToSplit, old);
        head.nextToSplit--;
        if (head.nextToSplit < 0) {
            head.baseSize /= 2;
            head.nextToSplit = head.baseSize - 1;
        }
        moveOut(session, head.nextToSplit, old);
        moveOut(session, head.bucketCount - 1, old);
        removeBucket(session);

//        for(int i=0; i<head.bucketCount; i++) {
//            LinearHashBucket bucket = this.getBucket(i);
//            if(bucket.getNextBucket() >= head.bucketCount) {
//                System.out.println("error");
//            }
//        }

        addAll(session, old);
//        int testALot;
//        for(int i=0; i<head.bucketCount; i++) {
//            LinearHashBucket bucket = this.getBucket(i);
//            if(bucket.getNextBucket() >= head.bucketCount) {
//                System.out.println("error");
//            }
//        }
    }

    private boolean isEquals(Session session, LinearHashEntry r, int hash, Value key) throws SQLException {
        if (r.hash == hash) {
            if (r.key == null) {
                r.key = getKey(tableData.getRow(session, r.value));
            }
            if (database.compareTypeSave(r.key, key) == 0) {
                return true;
            }
        }
        return false;
    }

    private int get(Session session, Value key) throws SQLException {
        int hash = key.hashCode();
        int home = getPos(hash);
        LinearHashBucket bucket = getBucket(session, home);
        while (true) {
            for (int i = 0; i < bucket.getRecordSize(); i++) {
                LinearHashEntry r = bucket.getRecord(i);
                if (isEquals(session, r, hash, key)) {
                    return r.value;
                }
            }
            if (bucket.getNextBucket() < 0) {
                return -1;
            }
            bucket = getBucket(session, bucket.getNextBucket());
        }
    }

    private void removeRecord(Session session, LinearHashBucket bucket, int index) throws SQLException {
        bucket.removeRecord(session, index);
        head.recordCount--;
        rowCount--;
    }

    private void addRecord(Session session, LinearHashBucket bucket, LinearHashEntry entry) throws SQLException {
        bucket.addRecord(session, entry);
        head.recordCount++;
        rowCount++;
    }

    private void remove(Session session, Value key) throws SQLException {
        merge(session);
        // trace.debug("remove "+key.toString());
        int hash = key.hashCode();
        int home = getPos(hash);
        int now = home;
        while (true) {
            LinearHashBucket bucket = getBucket(session, now);
            for (int i = 0; i < bucket.getRecordSize(); i++) {
                LinearHashEntry r = bucket.getRecord(i);
                if (isEquals(session, r, hash, key)) {
                    removeRecord(session, bucket, i);
                    if (home != now) {
                        ObjectArray old = new ObjectArray();
                        moveOut(session, home, old);
                        addAll(session, old);
                    }
                    return;
                }
            }
            int next = bucket.getNextBucket();
            if (next < 0) {
                throw Message.getInternalError("not found");
            }
            now = next;
        }
    }

    private int getBlockId(int i) {
        return i * blocksPerBucket + firstBucketBlock;
    }

    private LinearHashBucket getBucket(Session session, int i) throws SQLException {
        readCount++;
        if (SysProperties.CHECK && i >= head.bucketCount) {
            throw Message.getInternalError("get=" + i + " max=" + head.bucketCount);
        }
        // trace.debug("read " + i);
        // return (LinearHashBucket) buckets.get(i);
        i = getBlockId(i);
        // System.out.println("getBucket "+i);
        LinearHashBucket bucket = (LinearHashBucket) storage.getRecord(session, i);
        return bucket;
    }

    private void addBucket(Session session) throws SQLException {
        LinearHashBucket bucket = new LinearHashBucket(this);
        storage.addRecord(session, bucket, getBlockId(head.bucketCount));
        // bucket.setPos(buckets.size());
        // buckets.add(bucket);
        //System.out.println("addBucket "+bucket.getPos());
        if (SysProperties.CHECK && bucket.getBlockCount() > blocksPerBucket) {
            throw Message.getInternalError("blocks=" + bucket.getBlockCount());
        }
        head.bucketCount++;
    }

    private void removeBucket(Session session) throws SQLException {
        // buckets.remove(head.bucketCount-1);
        int pos = getBlockId(head.bucketCount - 1);
        //System.out.println("removeBucket "+pos);
        storage.removeRecord(session, pos);
        head.bucketCount--;
    }

    private int getUtilization() {
        return 100 * head.recordCount / (head.bucketCount * RECORDS_PER_BUCKET);
    }

    void updateBucket(Session session, LinearHashBucket bucket) throws SQLException {
        storage.updateRecord(session, bucket);
    }

    public Record read(Session session, DataPage s) throws SQLException {
        char c = (char) s.readByte();
        if (c == 'B') {
            return new LinearHashBucket(this, s);
        } else if (c == 'H') {
            return new LinearHashHead(this, s);
        } else {
            throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, getName());
        }
    }

    public void close(Session session) throws SQLException {
        // TODO flush from time to time (after a few seconds of no activity or so)
        writeHeader(session);
        diskFile.close();
    }

    public void add(Session session, Row row) throws SQLException {
        Value key = getKey(row);
        if (get(session, key) != -1) {
            // TODO index duplicate key for hash indexes: is this allowed?
            throw getDuplicateKeyException();
        }
        add(session, key, row.getPos());
    }

    public void remove(Session session, Row row) throws SQLException {
        remove(session, getKey(row));
    }

    private Value getKey(SearchRow row) throws SQLException {
        if (columns.length == 1) {
            Column column = columns[0];
            int index = column.getColumnId();
            Value v = row.getValue(index);
            return v;
        }
        Value[] list = new Value[columns.length];
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            int index = column.getColumnId();
            Value v = row.getValue(index);
            list[i] = v;
        }
        return ValueArray.get(list);
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        if (first == null || last == null) {
            // TODO hash index: should additionally check if values are the same
            throw Message.getInternalError();
        }
        int key = get(session, getKey(first));
        if (key == -1) {
            return new LinearHashCursor(null);
        }
        return new LinearHashCursor(tableData.getRow(session, key));
    }

    public double getCost(Session session, int[] masks) throws SQLException {
        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            int index = column.getColumnId();
            int mask = masks[index];
            if ((mask & IndexCondition.EQUALITY) != IndexCondition.EQUALITY) {
                return Long.MAX_VALUE;
            }
        }
        return 100;
    }

    public void remove(Session session) throws SQLException {
        storage.delete(session);
        diskFile.delete();
    }

    public void truncate(Session session) throws SQLException {
        storage.truncate(session);
        // buckets.clear();
        head = new LinearHashHead(this);
        head.recordCount = 0;
        head.bucketCount = 0;
        head.baseSize = 1;
        head.nextToSplit = 0;
        readCount = 0;
        storage.addRecord(session, head, 0);
        addBucket(session);
        rowCount = 0;
    }

    public void checkRename() throws SQLException {
    }

    public boolean needRebuild() {
        return needRebuild;
    }

    public int getBucketSize() {
        return bucketSize;
    }

    public boolean canGetFirstOrLast() {
        return false;
    }

    public SearchRow findFirstOrLast(Session session, boolean first) throws SQLException {
        throw Message.getUnsupportedException();
    }

}
