/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.retry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import org.h2.store.fs.FilePathWrapper;

/**
 * A file system that re-opens and re-tries the operation if the file was
 * closed, because a thread was interrupted. This will clear the interrupt flag.
 * It is mainly useful for applications that call Thread.interrupt by mistake.
 */
public class FilePathRetryOnInterrupt extends FilePathWrapper {

    /**
     * The prefix.
     */
    static final String SCHEME = "retry";

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileRetryOnInterrupt(name.substring(getScheme().length() + 1), mode);
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

}

