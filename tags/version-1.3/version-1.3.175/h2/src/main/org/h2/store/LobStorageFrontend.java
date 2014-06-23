/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueLobDb;

/**
 * This class stores LOB objects in the database.
 * This is the front-end i.e. the client side of the LOB storage.
 */
public class LobStorageFrontend implements LobStorageInterface {

    /**
     * The table id for session variables (LOBs not assigned to a table).
     */
    public static final int TABLE_ID_SESSION_VARIABLE = -1;

    /**
     * The table id for temporary objects (not assigned to any object).
     */
    public static final int TABLE_TEMP = -2;

    private final DataHandler handler;

    public LobStorageFrontend(DataHandler handler) {
        this.handler = handler;
    }

    /**
     * Delete a LOB from the database.
     *
     * @param lob the lob id
     */
    @Override
    public void removeLob(long lob) {
        // TODO this should not be called at all,
        // but that's a refactoring for another day
    }

    /**
     * Get the input stream for the given lob.
     *
     * @param lobId the lob id
     * @param hmac the message authentication code (for remote input streams)
     * @param byteCount the number of bytes to read, or -1 if not known
     * @return the stream
     */
    @Override
    public InputStream getInputStream(long lobId, byte[] hmac, long byteCount) throws IOException {
        if (byteCount < 0) {
            byteCount = Long.MAX_VALUE;
        }
        return new BufferedInputStream(new LobStorageRemoteInputStream(handler, lobId, hmac, byteCount));
    }

    @Override
    public ValueLobDb copyLob(int type, long oldLobId, int tableId, long length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTable(long lobId, int tableIdSessionVariable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value createBlob(InputStream in, long maxLength) {
        if (SysProperties.LOB_IN_DATABASE) {
            // need to use a temp file, because the input stream could come from
            // the same database, which would create a weird situation (trying
            // to read a block while write something)
            return ValueLobDb.createTempBlob(in, maxLength, handler);
        }
        return ValueLob.createBlob(in, maxLength, handler);
    }

    /**
     * Create a CLOB object.
     *
     * @param reader the reader
     * @param maxLength the maximum length (-1 if not known)
     * @return the LOB
     */
    @Override
    public Value createClob(Reader reader, long maxLength) {
        if (SysProperties.LOB_IN_DATABASE) {
            // need to use a temp file, because the input stream could come from
            // the same database, which would create a weird situation (trying
            // to read a block while write something)
            return ValueLobDb.createTempClob(reader, maxLength, handler);
        }
        return ValueLob.createClob(reader, maxLength, handler);
    }


    /**
     * Create a LOB object that fits in memory.
     *
     * @param type the value type
     * @param small the byte array
     * @return the LOB
     */
    public static Value createSmallLob(int type, byte[] small) {
        if (SysProperties.LOB_IN_DATABASE) {
            int precision;
            if (type == Value.CLOB) {
                precision = new String(small, Constants.UTF8).length();
            } else {
                precision = small.length;
            }
            return ValueLobDb.createSmallLob(type, small, precision);
        }
        return ValueLob.createSmallLob(type, small);
    }

}
