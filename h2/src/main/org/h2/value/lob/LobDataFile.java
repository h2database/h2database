/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value.lob;

import java.io.BufferedInputStream;
import java.io.InputStream;

import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.store.DataHandler;
import org.h2.store.FileStore;
import org.h2.store.FileStoreInputStream;
import org.h2.store.fs.FileUtils;
import org.h2.value.ValueLob;

/**
 * LOB data stored in a temporary file.
 */
public final class LobDataFile extends LobData {

    private DataHandler handler;

    /**
     * If the LOB is a temporary LOB being managed by a temporary ResultSet, it
     * is stored in a temporary file.
     */
    private final String fileName;

    private final FileStore tempFile;

    public LobDataFile(DataHandler handler, String fileName, FileStore tempFile) {
        this.handler = handler;
        this.fileName = fileName;
        this.tempFile = tempFile;
    }

    @Override
    public void remove(ValueLob value) {
        if (fileName != null) {
            if (tempFile != null) {
                tempFile.stopAutoDelete();
            }
            // synchronize on the database, to avoid concurrent temp file
            // creation / deletion / backup
            synchronized (handler.getLobSyncObject()) {
                FileUtils.delete(fileName);
            }
        }
    }

    @Override
    public InputStream getInputStream(long precision) {
        FileStore store = handler.openFile(fileName, "r", true);
        boolean alwaysClose = SysProperties.lobCloseBetweenReads;
        return new BufferedInputStream(new FileStoreInputStream(store, false, alwaysClose),
                Constants.IO_BUFFER_SIZE);
    }

    @Override
    public DataHandler getDataHandler() {
        return handler;
    }

    @Override
    public String toString() {
        return "lob-file: " + fileName;
    }

}
