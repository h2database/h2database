/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value.lob;

import java.io.IOException;
import java.io.InputStream;

import org.h2.message.DbException;
import org.h2.store.DataHandler;
import org.h2.value.ValueLob;

/**
 * LOB data stored in database.
 */
public final class LobDataDatabase extends LobData {

    private final DataHandler handler;

    /**
     * If the LOB is managed by the one the LobStorageBackend classes, these are
     * the unique key inside that storage.
     */
    private final int tableId;

    private final long lobId;

    public LobDataDatabase(DataHandler handler, int tableId, long lobId) {
        this.handler = handler;
        this.tableId = tableId;
        this.lobId = lobId;
    }

    @Override
    public void remove(ValueLob value) {
        if (handler != null) {
            handler.getLobStorage().removeLob(value);
        }
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
    public InputStream getInputStream(long precision) {
        try {
            return handler.getLobStorage().getInputStream(lobId, tableId, precision);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    @Override
    public DataHandler getDataHandler() {
        return handler;
    }

    @Override
    public String toString() {
        return "lob-table: table: " + tableId + " id: " + lobId;
    }
}
