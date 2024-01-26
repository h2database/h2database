/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import org.h2.engine.SessionRemote;
import org.h2.value.ValueBlob;
import org.h2.value.ValueClob;
import org.h2.value.ValueLob;

/**
 * This factory creates in-memory objects and temporary files. It is used on the
 * client side.
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

    /**
     * The table id for result sets.
     */
    public static final int TABLE_RESULT = -3;

    private final SessionRemote sessionRemote;

    public LobStorageFrontend(SessionRemote handler) {
        this.sessionRemote = handler;
    }

    @Override
    public void removeLob(ValueLob lob) {
        // not stored in the database
    }

    @Override
    public InputStream getInputStream(long lobId,
            long byteCount) throws IOException {
        // this method is only implemented on the server side of a TCP connection
        throw new IllegalStateException();
    }

    @Override
    public InputStream getInputStream(long lobId, int tableId, long byteCount) throws IOException {
        // this method is only implemented on the server side of a TCP
        // connection
        throw new IllegalStateException();
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public ValueLob copyLob(ValueLob old, int tableId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeAllForTable(int tableId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ValueBlob createBlob(InputStream in, long maxLength) {
        // need to use a temp file, because the input stream could come from
        // the same database, which would create a weird situation (trying
        // to read a block while writing something)
        return ValueBlob.createTempBlob(in, maxLength, sessionRemote);
    }

    /**
     * Create a CLOB object.
     *
     * @param reader the reader
     * @param maxLength the maximum length (-1 if not known)
     * @return the LOB
     */
    @Override
    public ValueClob createClob(Reader reader, long maxLength) {
        // need to use a temp file, because the input stream could come from
        // the same database, which would create a weird situation (trying
        // to read a block while writing something)
        return ValueClob.createTempClob(reader, maxLength, sessionRemote);
    }
}
