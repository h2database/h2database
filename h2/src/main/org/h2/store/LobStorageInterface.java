/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import org.h2.value.ValueBlob;
import org.h2.value.ValueClob;
import org.h2.value.ValueLob;

/**
 * A mechanism to store and retrieve lob data.
 */
public interface LobStorageInterface {

    /**
     * Create a CLOB object.
     *
     * @param reader the reader
     * @param maxLength the maximum length (-1 if not known)
     * @return the LOB
     */
    ValueClob createClob(Reader reader, long maxLength);

    /**
     * Create a BLOB object.
     *
     * @param in the input stream
     * @param maxLength the maximum length (-1 if not known)
     * @return the LOB
     */
    ValueBlob createBlob(InputStream in, long maxLength);

    /**
     * Copy a lob.
     *
     * @param old the old lob
     * @param tableId the new table id
     * @return the new lob
     */
    ValueLob copyLob(ValueLob old, int tableId);

    /**
     * Get the input stream for the given lob, only called on server side of a TCP connection.
     *
     * @param lobId the lob id
     * @param byteCount the number of bytes to read, or -1 if not known
     * @return the stream
     * @throws IOException on failure
     */
    InputStream getInputStream(long lobId, long byteCount) throws IOException;

    /**
     * Get the input stream for the given lob
     *
     * @param lobId the lob id
     * @param tableId the able id
     * @param byteCount the number of bytes to read, or -1 if not known
     * @return the stream
     * @throws IOException on failure
     */
    InputStream getInputStream(long lobId, int tableId, long byteCount) throws IOException;

    /**
     * Delete a LOB (from the database, if it is stored there).
     *
     * @param lob the lob
     */
    void removeLob(ValueLob lob);

    /**
     * Remove all LOBs for this table.
     *
     * @param tableId the table id
     */
    void removeAllForTable(int tableId);

    /**
     * Whether the storage is read-only
     *
     * @return true if yes
     */
    boolean isReadOnly();

    /**
     * Close LobStorage and release all resources
     */
    default void close() {}
}
