/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.value.Value;

/**
 * The base class for rows stored in a table, and for partial rows stored in the
 * index.
 */
public abstract class SearchRow {

    /**
     * Index of a virtual "_ROWID_" column within a row or a table
     */
    public static final int ROWID_INDEX = -1;

    public static final int MEMORY_CALCULATE = -1;

    protected long key;

    /**
     * Get the column count.
     *
     * @return the column count
     */
    public abstract int getColumnCount();

    /**
     * Get the value for the column
     *
     * @param index the column number (starting with 0)
     * @return the value
     */
    public abstract Value getValue(int index);

    /**
     * Set the value for given column
     *
     * @param index the column number (starting with 0)
     * @param v the new value
     */
    public abstract void setValue(int index, Value v);

    /**
     * Set the unique key of the row.
     *
     * @param key the key
     */
    public void setKey(long key) {
        this.key = key;
    }

    /**
     * Get the unique key of the row.
     *
     * @return the key
     */
    public long getKey() {
        return key;
    }

    /**
     * Get the estimated memory used for this row, in bytes.
     *
     * @return the memory
     */
    public abstract int getMemory();

}
