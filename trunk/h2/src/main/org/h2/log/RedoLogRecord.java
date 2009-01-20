/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.log;

import org.h2.store.Storage;

/**
 * Represents a redo-log record.
 * Such records are only used when recovering.
 */
public class RedoLogRecord {

    /**
     * The storage object to where this log record belongs to.
     */
    public Storage storage;

    /**
     * The sequence id. This id is used to sort the records in the same order as
     * they appear in the log file.
     */
    public int sequenceId;

    /**
     * The position in the data file.
     */
    public int recordId;

    /**
     * The offset in the data byte array.
     */
    public int offset;

    /**
     * The data.
     */
    public byte[] data;

    /**
     * Get the estimated memory size used by this object.
     *
     * @return the estimated memory size
     */
    public int getSize() {
        // estimated memory size in bytes ((5 variables+myself) * 4 bytes each)
        if (data == null) {
            return 24;
        }
        return 28 + data.length;
    }

}
