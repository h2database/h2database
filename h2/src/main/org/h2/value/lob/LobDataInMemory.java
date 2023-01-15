/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value.lob;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * LOB data stored in memory.
 */
public final class LobDataInMemory extends LobData {

    /**
     * If the LOB is below the inline size, we just store/load it directly here.
     */
    private final byte[] small;

    public LobDataInMemory(byte[] small) {
        if (small == null) {
            throw new IllegalStateException();
        }
        this.small = small;
    }

    @Override
    public InputStream getInputStream(long precision) {
        return new ByteArrayInputStream(small);
    }

    /**
     * Get the data if this a small lob value.
     *
     * @return the data
     */
    public byte[] getSmall() {
        return small;
    }

    @Override
    public int getMemory() {
        /*
         * Java 11 with -XX:-UseCompressedOops 0 bytes: 120 bytes 1 byte: 128
         * bytes
         */
        return small.length + 127;
    }

}
