/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (https://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.h2.value;

import java.io.BufferedInputStream;
import java.io.InputStream;
import org.h2.engine.CastDataProvider;
import org.h2.engine.SessionRemote;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.store.LobStorageRemoteInputStream;

/**
 * A implementation of the BLOB and CLOB data types used on the client side of a
 * remote H2 connection. Fetches the underlying on data from the server.
 */
public final class ValueLobFetchOnDemand extends ValueLob {

    private SessionRemote handler;
    /**
     * If the LOB is managed by the one the LobStorageBackend classes, these are
     * the unique key inside that storage.
     */
    private final int tableId;
    private final long lobId;
    /**
     * If this is a client-side ValueLobDb object returned by a ResultSet, the
     * hmac acts a security cookie that the client can send back to the server
     * to ask for data related to this LOB.
     */
    protected final byte[] hmac;

    private ValueLobFetchOnDemand(int type, DataHandler handler, int tableId, long lobId, byte[] hmac,
            long precision) {
        super(type, precision);
        assert (type == BLOB || type == CLOB);
        this.hmac = hmac;
        this.handler = (SessionRemote) handler;
        this.tableId = tableId;
        this.lobId = lobId;
    }

    /**
     * Create a LOB value.
     *
     * @param type the type (Value.BLOB or CLOB)
     * @param handler the data handler
     * @param tableId the table id
     * @param id the lob id
     * @param hmac the message authentication code
     * @param precision the precision (number of bytes / characters)
     * @return the value
     */
    public static ValueLobFetchOnDemand create(int type, DataHandler handler, int tableId, long id, byte[] hmac,
            long precision) {
        return new ValueLobFetchOnDemand(type, handler, tableId, id, hmac, precision);
    }

    /**
     * Remove the underlying resource, if any. For values that are kept fully in
     * memory this method has no effect.
     */
    @Override
    public void remove() {
        handler.getLobStorage().removeLob(this);
    }

    /**
     * Copy a large value, to be used in the given table. For values that are
     * kept fully in memory this method has no effect.
     *
     * @param database the data handler
     * @param tableId the table where this object is used
     * @return the new value or itself
     */
    @Override
    public ValueLob copy(DataHandler database, int tableId) {
        return handler.getLobStorage().copyLob(this, tableId, precision);
    }

    /**
     * Check if this value is linked to a specific table. For values that are
     * kept fully in memory, this method returns false.
     *
     * @return true if it is
     */
    @Override
    public boolean isLinkedToTable() {
        throw new IllegalStateException();
    }

    /**
     * Get the current table id of this lob.
     *
     * @return the table id
     */
    public int getTableId() {
        return tableId;
    }

    public long getLobId() {
        return lobId;
    }

    @Override
    public int compareTypeSafe(Value v, CompareMode mode, CastDataProvider provider) {
        if (v == this) {
            return 0;
        }
        ValueLobFetchOnDemand v2 = (ValueLobFetchOnDemand) v;
        if (v2 != null && lobId == v2.lobId) {
            return 0;
        }
        return compare(this, v2);
    }

    @Override
    public InputStream getInputStream() {
        return new BufferedInputStream(new LobStorageRemoteInputStream(handler, lobId, hmac));
    }

    @Override
    public InputStream getInputStream(long oneBasedOffset, long length) {
        if (this.valueType == CLOB) {
            // Cannot usefully into index into a unicode based stream with a byte offset
            throw DbException.throwInternalError();
        }
        final InputStream inputStream = new BufferedInputStream(
                new LobStorageRemoteInputStream(handler, lobId, hmac));
        return rangeInputStream(inputStream, oneBasedOffset, length, precision);
    }

    /**
     * Returns the data handler.
     *
     * @return the data handler, or {@code null}
     */
    public DataHandler getDataHandler() {
        return handler;
    }

    /**
     * Convert the precision to the requested value.
     *
     * @param precision the new precision
     * @return the truncated or this value
     */
    @Override
    ValueLob convertPrecision(long precision) {
        if (this.precision <= precision) {
            return this;
        }
        ValueLob lob;
        if (valueType == CLOB) {
            lob = ValueLobFile.createTempClob(getReader(), precision, handler);
        } else {
            lob = ValueLobFile.createTempBlob(getInputStream(), precision, handler);
        }
        return lob;
    }

    @Override
    public String toString() {
        return "lob-table: table: " + tableId + " id: " + lobId;
    }
}
