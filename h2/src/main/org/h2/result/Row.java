/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.store.Data;
import org.h2.value.Value;

/**
 * Represents a row in a table.
 */
public abstract class Row implements SearchRow {

    public static final int MEMORY_CALCULATE = -1;
    public static final Row[] EMPTY_ARRAY = {};

    /**
     * Get a copy of the row that is distinct from (not equal to) this row.
     * This is used for FOR UPDATE to allow pseudo-updating a row.
     *
     * @return a new row with the same data
     */
    public abstract Row getCopy();

    /**
     * Set version.
     *
     * @param version row version
     */
    public abstract void setVersion(int version);

    /**
     * Get the number of bytes required for the data.
     *
     * @param dummy the template buffer
     * @return the number of bytes
     */
    public abstract int getByteCount(Data dummy);

    /**
     * Check if this is an empty row.
     *
     * @return {@code true} if the row is empty
     */
    public abstract boolean isEmpty();

    /**
     * Mark the row as deleted.
     *
     * @param deleted deleted flag
     */
    public abstract void setDeleted(boolean deleted);

    /**
     * Set session id.
     *
     * @param sessionId the session id
     */
    public abstract void setSessionId(int sessionId);

    /**
     * Get session id.
     *
     * @return the session id
     */
    public abstract int getSessionId();

    /**
     * This record has been committed. The session id is reset.
     */
    public abstract void commit();

    /**
     * Check if the row is deleted.
     *
     * @return {@code true} if the row is deleted
     */
    public abstract boolean isDeleted();

    /**
     * Get values.
     *
     * @return values
     */
    public abstract Value[] getValueList();
}
