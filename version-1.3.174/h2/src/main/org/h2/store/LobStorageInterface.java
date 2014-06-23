/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import org.h2.value.Value;
import org.h2.value.ValueLobDb;

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
    Value createClob(Reader reader, long maxLength);

    /**
     * Create a BLOB object.
     *
     * @param in the input stream
     * @param maxLength the maximum length (-1 if not known)
     * @return the LOB
     */
    Value createBlob(InputStream in, long maxLength);

    /**
     * Copy a lob.
     *
     * @param type the type
     * @param oldLobId the old lob id
     * @param tableId the new table id
     * @param length the length
     * @return the new lob
     */
    ValueLobDb copyLob(int type, long oldLobId, int tableId, long length);

    /**
     * Get the input stream for the given lob.
     *
     * @param lobId the lob id
     * @param hmac the message authentication code (for remote input streams)
     * @param byteCount the number of bytes to read, or -1 if not known
     * @return the stream
     */
    InputStream getInputStream(long lobId, byte[] hmac, long byteCount)
            throws IOException;

    /**
     * Set the table reference of this lob.
     *
     * @param lobId the lob
     * @param table the table
     */
    void setTable(long lobId, int table);

    /**
     * Delete a LOB from the database.
     *
     * @param lob the lob id
     */
    void removeLob(long lob);

}
