/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.store.DataPage;
import org.h2.store.Record;

/**
 * The head page of a linear hash index.
 */
public class LinearHashHead extends Record {

    private LinearHashIndex index;
    int baseSize;
    int nextToSplit;
    int recordCount, bucketCount;

    LinearHashHead(LinearHashIndex index) {
        this.index = index;
    }

    LinearHashHead(LinearHashIndex index, DataPage s) {
        this.index = index;
        baseSize = s.readInt();
        nextToSplit = s.readInt();
        recordCount = s.readInt();
        bucketCount = s.readInt();
    }

    public int getByteCount(DataPage dummy) throws SQLException {
        return index.getBucketSize();
    }

    public void write(DataPage buff) throws SQLException {
        buff.writeByte((byte) 'H');
        buff.writeInt(baseSize);
        buff.writeInt(nextToSplit);
        buff.writeInt(recordCount);
        buff.writeInt(bucketCount);
    }

    public boolean isPinned() {
        return true;
    }

}
