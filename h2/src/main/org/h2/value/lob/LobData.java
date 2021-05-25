/*
 * Copyright 2004-2021 H2 Group. Multiple-Licensed under the MPL 2.0,
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

    public abstract InputStream getInputStream(long precision);

    public DataHandler getDataHandler() {
        return null;
    }

    public boolean isLinkedToTable() {
        return false;
    }

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
