/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value.lob;

import java.io.InputStream;

import org.h2.store.DataHandler;
import org.h2.value.ValueLob;

/**
 * LOB data.
 */
public abstract class LobData {

    LobData() {
    }

    /**
     * Get stream to read LOB data from
     * @param precision octet length of the stream, or -1 if unknown
     * @return stream to read LOB data from
     */
    public abstract InputStream getInputStream(long precision);

    public DataHandler getDataHandler() {
        return null;
    }

    public boolean isLinkedToTable() {
        return false;
    }

    /**
     * Remove the underlying resource, if any. For values that are kept fully in
     * memory this method has no effect.
     * @param value to remove
     */
    public void remove(ValueLob value) {
    }

    /**
     * Get the memory used by this object.
     *
     * @return the memory used in bytes
     */
    public int getMemory() {
        return 140;
    }

}
