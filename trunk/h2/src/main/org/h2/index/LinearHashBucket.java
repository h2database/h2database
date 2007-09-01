/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.store.DataPage;
import org.h2.store.Record;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

/**
 * @author Thomas
 */
public class LinearHashBucket extends Record {
    private LinearHashIndex index;
    private int nextBucket;
    private ObjectArray records;
    private boolean writePos;
    
    public LinearHashBucket(LinearHashIndex index, DataPage s) throws SQLException {
        this.index = index;
        writePos = s.readByte() == 'P';        
        nextBucket = s.readInt();
        int len = s.readInt();
        records = new ObjectArray();
        for (int i = 0; i < len; i++) {
            LinearHashEntry entry = new LinearHashEntry();
            if (!writePos) {
                Value key = s.readValue();
                entry.key = key;
            }
            entry.hash = s.readInt();
            entry.value = s.readInt();
            entry.home = index.getPos(entry.hash);
            records.add(entry);
        }
    }    
    
    public LinearHashBucket(LinearHashIndex index) {
        this.index = index;
        this.records = new ObjectArray();
        this.nextBucket = -1;
    }

    private void update(Session session) throws SQLException {
        index.updateBucket(session, this);
    }
    
    void setNext(Session session, int nextBucket) throws SQLException {   
        this.nextBucket = nextBucket;
        update(session);
    }

    int getNextBucket() {
        return nextBucket;
    }

    LinearHashEntry getRecord(int i) {
        return (LinearHashEntry) records.get(i);
    }

    void addRecord(Session session, LinearHashEntry r) throws SQLException {
        records.add(r);
        update(session);
    }

    void removeRecord(Session session, int i) throws SQLException {
        records.remove(i);
        update(session);
    }

    int getRecordSize() {
        return records.size();
    }

    public void write(DataPage buff) throws SQLException {
        getRealByteCount(buff);
        buff.writeByte((byte) 'B');
        if (writePos) {
            buff.writeByte((byte) 'P');
        } else {
            buff.writeByte((byte) 'D');
        }
        buff.writeInt(nextBucket);
        buff.writeInt(records.size());
        for (int i = 0; i < records.size(); i++) {
            LinearHashEntry record = (LinearHashEntry) records.get(i);
            // TODO index: just add the hash if the key is too large
            if (!writePos) {
                buff.writeValue(record.key);
            }
            buff.writeInt(record.hash);
            buff.writeInt(record.value);
        }
    }

    public int getByteCount(DataPage dummy) throws SQLException {
        return index.getBucketSize();
    }

    public int getRealByteCount(DataPage dummy) throws SQLException {
        int size = 2 + dummy.getIntLen() + dummy.getIntLen();
        int dataSize = 0;
        for (int i = 0; i < records.size(); i++) {
            LinearHashEntry record = (LinearHashEntry) records.get(i);
            // TODO index: just add the hash if the key is too large
            dataSize += dummy.getValueLen(record.key);
            size += 2 * dummy.getIntLen();
        }
        if (size + dataSize >= index.getBucketSize()) {
            writePos = true;
            return size;
        } else {
            writePos = false;
            return size + dataSize;
        }
    }    

    public boolean isEmpty() {
        return false;
    }
    
}
