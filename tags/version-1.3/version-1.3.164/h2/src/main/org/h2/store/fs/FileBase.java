/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * The base class for file implementations.
 */
public abstract class FileBase extends FileChannel {

    public void force(boolean metaData) throws IOException {
        // ignore
    }

    public FileLock lock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException();
    }

    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        throw new UnsupportedOperationException();
    }

    public abstract long position() throws IOException;

    public abstract FileChannel position(long newPosition) throws IOException;

    public abstract int read(ByteBuffer dst) throws IOException;

    public int read(ByteBuffer dst, long position) throws IOException {
        throw new UnsupportedOperationException();
    }

    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    public abstract long size() throws IOException;

    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        throw new UnsupportedOperationException();
    }

    public long transferTo(long position, long count, WritableByteChannel target)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    public abstract FileChannel truncate(long size) throws IOException;

    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        throw new UnsupportedOperationException();
    }

    public abstract int write(ByteBuffer src) throws IOException;

    public int write(ByteBuffer src, long position) throws IOException {
        throw new UnsupportedOperationException();    }

    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();    }

    protected void implCloseChannel() throws IOException {
        // ignore
    }

}
