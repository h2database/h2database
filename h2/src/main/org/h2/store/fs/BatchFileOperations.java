/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.nio.ByteBuffer;
import java.util.Objects;

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
     */
    public static final class BatchReadOp {
        /** Absolute byte offset in the file. */
        public final long position;
        /**
         * Destination buffer; on return the buffer's position will have
         * advanced by the number of bytes read.
         */
        public final ByteBuffer dst;

        public BatchReadOp(long position, ByteBuffer dst) {
            this.position = position;
            this.dst = dst;
        }

        public long position() {
            return position;
        }

        public ByteBuffer dst() {
            return dst;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BatchReadOp)) return false;
            BatchReadOp other = (BatchReadOp) o;
            return position == other.position && Objects.equals(dst, other.dst);
        }

        @Override
        public int hashCode() {
            return Objects.hash(position, dst);
        }

        @Override
        public String toString() {
            return "BatchReadOp[position=" + position + ", dst=" + dst + ']';
        }
    }

    /**
     * A single positional write request.
     */
    public static final class BatchWriteOp {
        /** Absolute byte offset in the file. */
        public final long position;
        /**
         * Source buffer; the bytes between {@code position()} and
         * {@code limit()} are written.
         */
        public final ByteBuffer src;

        public BatchWriteOp(long position, ByteBuffer src) {
            this.position = position;
            this.src = src;
        }

        public long position() {
            return position;
        }

        public ByteBuffer src() {
            return src;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BatchWriteOp)) return false;
            BatchWriteOp other = (BatchWriteOp) o;
            return position == other.position && Objects.equals(src, other.src);
        }

        @Override
        public int hashCode() {
            return Objects.hash(position, src);
        }

        @Override
        public String toString() {
            return "BatchWriteOp[position=" + position + ", src=" + src + ']';
        }
    }
}