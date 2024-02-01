/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value.lob;

import java.io.BufferedInputStream;
import java.io.InputStream;

import org.h2.engine.SessionRemote;
import org.h2.store.DataHandler;
import org.h2.store.LobStorageRemoteInputStream;

/**
 * A implementation of the LOB data used on the client side of a remote H2
 * connection. Fetches the underlying on data from the server.
 */
public final class LobDataFetchOnDemand extends LobData {

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
    private final byte[] hmac;

    public LobDataFetchOnDemand(DataHandler handler, int tableId, long lobId, byte[] hmac) {
        this.hmac = hmac;
        this.handler = (SessionRemote) handler;
        this.tableId = tableId;
        this.lobId = lobId;
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
    public InputStream getInputStream(long precision) {
        return new BufferedInputStream(new LobStorageRemoteInputStream(handler, lobId, hmac));
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
