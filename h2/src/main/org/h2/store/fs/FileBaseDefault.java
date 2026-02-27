/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import org.h2.store.fs.BatchFileOperations.BatchReadOp;
import org.h2.store.fs.BatchFileOperations.BatchWriteOp;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

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

    // ======================== BATCH I/O API ========================

    /**
     * Read multiple buffers from the file in a single (potentially batched)
     * operation.
     *
     * <p>The default implementation issues sequential
     * {@link #read(ByteBuffer, long)} calls, looping each until the buffer
     * is fully populated or EOF is reached.</p>
     *
     * <p>Subclasses that can batch I/O (e.g. io_uring) should override this
     * to submit all operations in one system-call batch.</p>
     *
     * @param ops list of (position, buffer) pairs
     * @throws IOException on any read error or premature EOF
     */
    public void readBatch(List<BatchReadOp> ops) throws IOException {
        for (BatchReadOp op : ops) {
            readFullyInternal(op.dst(), op.position());
        }
    }

    /**
     * Write multiple buffers to the file in a single (potentially batched)
     * operation.
     *
     * <p>The default implementation issues sequential
     * {@link #write(ByteBuffer, long)} calls.</p>
     *
     * @param ops list of (position, buffer) pairs
     * @throws IOException on any write error
     */
    public void writeBatch(List<BatchWriteOp> ops) throws IOException {
        for (BatchWriteOp op : ops) {
            writeFullyInternal(op.src(), op.position());
        }
    }

    /**
     * Whether this channel's {@link #readBatch}/{@link #writeBatch} actually
     * submit multiple operations in one kernel round-trip.
     *
     * @return {@code true} for batch-capable channels, {@code false} by default
     */
    public boolean supportsBatchIO() {
        return false;
    }

    // ── private helpers: loop-until-done for the serial fallback ─────────

    private void readFullyInternal(ByteBuffer dst, long position)
            throws IOException {
        long pos = position;
        do {
            int len = read(dst, pos);
            if (len < 0) {
                throw new EOFException(
                        "readBatch: unexpected EOF at position " + pos);
            }
            pos += len;
        } while (dst.hasRemaining());
    }

    private void writeFullyInternal(ByteBuffer src, long position)
            throws IOException {
        long pos = position;
        do {
            int len = write(src, pos);
            pos += len;
        } while (src.hasRemaining());
    }
}
