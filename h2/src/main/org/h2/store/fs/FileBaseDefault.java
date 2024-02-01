/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Default implementation of the slow operations that need synchronization because they
 * involve the file position.
 */
public abstract class FileBaseDefault extends FileBase {

    private long position = 0;

    @Override
    public final synchronized long position() throws IOException {
        return position;
    }

    @Override
    public final synchronized FileChannel position(long newPosition) throws IOException {
        if (newPosition < 0) {
            throw new IllegalArgumentException();
        }
        position = newPosition;
        return this;
    }

    @Override
    public final synchronized int read(ByteBuffer dst) throws IOException {
        int read = read(dst, position);
        if (read > 0) {
            position += read;
        }
        return read;
    }

    @Override
    public final synchronized int write(ByteBuffer src) throws IOException {
        int written = write(src, position);
        if (written > 0) {
            position += written;
        }
        return written;
    }

    @Override
    public final synchronized FileChannel truncate(long newLength) throws IOException {
        implTruncate(newLength);
        if (newLength < position) {
            position = newLength;
        }
        return this;
    }

    /**
     * The truncate implementation.
     *
     * @param size the new size
     * @throws IOException on failure
     */
    protected abstract void implTruncate(long size) throws IOException;
}
