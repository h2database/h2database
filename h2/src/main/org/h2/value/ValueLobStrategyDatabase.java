/*
 * Copyright 2004-2021 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (https://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.h2.value;

import java.io.IOException;
import java.io.InputStream;
import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.store.LobStorageFrontend;
import org.h2.store.LobStorageInterface;

/**
 * A implementation of the BLOB and CLOB data types. Small objects are kept in
 * memory and stored in the record. Large objects are either stored in the
 * database, or in temporary files.
 */
public final class ValueLobStrategyDatabase extends ValueLobStrategy {

    private DataHandler handler;
    /**
     * If the LOB is managed by the one the LobStorageBackend classes, these are
     * the unique key inside that storage.
     */
    private final int tableId;
    private final long lobId;
    /**
     * Fix for recovery tool.
     */
    private boolean isRecoveryReference;

    private ValueLobStrategyDatabase(DataHandler handler, int tableId, long lobId) {
        this.handler = handler;
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
     * @param precision the precision (number of bytes / characters)
     * @return the value
     */
    public static ValueLob create(int type, DataHandler handler, int tableId, long id,
            long precision) {
        return new ValueLob(type, precision, new ValueLobStrategyDatabase(handler, tableId, id));
    }

    public static ValueLob createRecoveryRef(int type, DataHandler handler, int tableId, long id,
            long precision) {
        ValueLobStrategyDatabase vldb = new ValueLobStrategyDatabase(handler, tableId, id);
        vldb.setRecoveryReference(true);
        return new ValueLob(type, precision, vldb);
    }
    
    /**
     * Remove the underlying resource, if any. For values that are kept fully in
     * memory this method has no effect.
     */
    @Override
    public void remove(ValueLob lob) {
        if (handler != null) {
            handler.getLobStorage().removeLob(lob);
        }
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
        return tableId >= 0;
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
        if (!(v instanceof ValueLob)) {
            return super.compareTypeSafe(lob, v, mode, provider);
        }
        ValueLob otherLob = (ValueLob) v;
        if (!(otherLob.fetchStrategy instanceof ValueLobStrategyDatabase)) {
            return super.compareTypeSafe(lob, v, mode, provider);
        }
        ValueLobStrategyDatabase v2 = (ValueLobStrategyDatabase) otherLob.fetchStrategy;
        if (lobId == v2.lobId) {
            return 0;
        }
        return ValueLob.compare(lob, otherLob);
    }

    @Override
    public InputStream getInputStream(ValueLob lob) {
        long byteCount = (lob.valueType == Value.BLOB) ? lob.precision : -1;
        try {
            return handler.getLobStorage().getInputStream(lobId, tableId, byteCount);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    @Override
    public InputStream getInputStream(ValueLob lob, long oneBasedOffset, long length) {
        final InputStream inputStream;
        final long byteCount = (lob.valueType == Value.BLOB) ? lob.precision : -1;
        try {
            inputStream = handler.getLobStorage().getInputStream(lobId, tableId, byteCount);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
        return ValueLob.rangeInputStream(inputStream, oneBasedOffset, length, byteCount);
    }

    @Override
    public DataHandler getDataHandler() {
        return handler;
    }

    /**
     * Create an independent copy of this value, that will be bound to a result.
     *
     * @return the value (this for small objects)
     */
    @Override
    public ValueLob copyToResult(ValueLob lob) {
        LobStorageInterface s = handler.getLobStorage();
        if (s.isReadOnly()) {
            return lob;
        }
        return s.copyLob(lob, LobStorageFrontend.TABLE_RESULT, lob.precision);
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

    public void setRecoveryReference(boolean isRecoveryReference) {
        this.isRecoveryReference = isRecoveryReference;
    }

    public boolean isRecoveryReference() {
        return isRecoveryReference;
    }
}
