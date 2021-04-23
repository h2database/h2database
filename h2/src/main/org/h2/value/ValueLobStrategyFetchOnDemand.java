/*
 * Copyright 2004-2021 H2 Group. Multiple-Licensed under the MPL 2.0, and the
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
final class ValueLobStrategyFetchOnDemand extends ValueLobStrategy {

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

    private ValueLobStrategyFetchOnDemand(DataHandler handler, int tableId, long lobId, byte[] hmac) {
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
    public static ValueLob create(int type, DataHandler handler, int tableId, long id, byte[] hmac,
            long precision) {
        assert (type == ValueLob.BLOB || type == ValueLob.CLOB);
        return new ValueLob(type, precision, new ValueLobStrategyFetchOnDemand(handler, tableId, id, hmac));
    }

    /**
     * Remove the underlying resource, if any. For values that are kept fully in
     * memory this method has no effect.
     */
    @Override
    public void remove(ValueLob lob) {
        handler.getLobStorage().removeLob(lob);
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
    public ValueLob copy(ValueLob lob, DataHandler database, int tableId) {
        return handler.getLobStorage().copyLob(lob, tableId, lob.precision);
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
    public int compareTypeSafe(ValueLob lob, Value v, CompareMode mode, CastDataProvider provider) {
        if (v == lob) {
            return 0;
        }
        ValueLobStrategyFetchOnDemand v2 = (ValueLobStrategyFetchOnDemand) ((ValueLob) v).getFetchStrategy();
        if (v2 != null && lobId == v2.lobId) {
            return 0;
        }
        return ValueLob.compare(lob, (ValueLob) v);
    }

    @Override
    public InputStream getInputStream(ValueLob lob) {
        return new BufferedInputStream(new LobStorageRemoteInputStream(handler, lobId, hmac));
    }

    @Override
    public InputStream getInputStream(ValueLob lob, long oneBasedOffset, long length) {
        if (lob.valueType == ValueLob.CLOB) {
            // Cannot usefully into index into a unicode based stream with a byte offset
            throw DbException.getInternalError();
        }
        final InputStream inputStream = new BufferedInputStream(
                new LobStorageRemoteInputStream(handler, lobId, hmac));
        return ValueLob.rangeInputStream(inputStream, oneBasedOffset, length, lob.precision);
    }

    @Override
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
    ValueLob convertPrecision(ValueLob oldLob, long precision) {
        if (oldLob.precision <= precision) {
            return oldLob;
        }
        ValueLob newLob;
        if (oldLob.valueType == ValueLob.CLOB) {
            newLob = ValueLobStrategyFile.createTempClob(oldLob.getReader(), precision, handler);
        } else {
            newLob = ValueLobStrategyFile.createTempBlob(oldLob.getInputStream(), precision, handler);
        }
        return newLob;
    }

    @Override
    public String toString() {
        return "lob-table: table: " + tableId + " id: " + lobId;
    }
}
