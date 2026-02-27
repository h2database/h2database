/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.nio.ByteBuffer;

/**
 * Batch I/O operation descriptors for use with
 * {@link FileBaseDefault#readBatch} / {@link FileBaseDefault#writeBatch}
 * and {@link org.h2.mvstore.FileStore#readBatch} /
 * {@link org.h2.mvstore.FileStore#writeBatch}.
 *
 * <p>These are intentionally plain records so that every FileChannel
 * implementation (NIO, NIO-mapped, JUring, etc.) can accept and produce
 * them without introducing new dependencies.</p>
 */
public final class BatchFileOperations {

    private BatchFileOperations() {
        // utility class — not instantiable
    }

    /**
     * A single positional read request.
     *
     * @param position absolute byte offset in the file
     * @param dst      destination buffer; on return the buffer's position will
     *                 have advanced by the number of bytes read
     */
    public record BatchReadOp(long position, ByteBuffer dst) {}

    /**
     * A single positional write request.
     *
     * @param position absolute byte offset in the file
     * @param src      source buffer; the bytes between {@code position()} and
     *                 {@code limit()} are written
     */
    public record BatchWriteOp(long position, ByteBuffer src) {}
}