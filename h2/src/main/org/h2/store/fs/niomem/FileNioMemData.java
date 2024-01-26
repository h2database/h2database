/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.niomem;

import java.nio.ByteBuffer;
import java.nio.channels.NonWritableChannelException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.h2.compress.CompressLZF;
import org.h2.util.MathUtils;

/**
 * This class contains the data of an in-memory random access file.
 * Data compression using the LZF algorithm is supported as well.
 */
class FileNioMemData {

    private static final int CACHE_MIN_SIZE = 8;
    private static final int BLOCK_SIZE_SHIFT = 16;

    private static final int BLOCK_SIZE = 1 << BLOCK_SIZE_SHIFT;
    private static final int BLOCK_SIZE_MASK = BLOCK_SIZE - 1;
    private static final ByteBuffer COMPRESSED_EMPTY_BLOCK;

    private static final ThreadLocal<CompressLZF> LZF_THREAD_LOCAL = ThreadLocal.withInitial(CompressLZF::new);

    /** the output buffer when compressing */
    private static final ThreadLocal<byte[]> COMPRESS_OUT_BUF_THREAD_LOCAL = ThreadLocal
            .withInitial(() -> new byte[BLOCK_SIZE * 2]);

    /**
     * The hash code of the name.
     */
    final int nameHashCode;

    private final CompressLaterCache<CompressItem, CompressItem> compressLaterCache =
        new CompressLaterCache<>(CACHE_MIN_SIZE);

    private String name;
    private final boolean compress;
    private final float compressLaterCachePercent;
    private volatile long length;
    private volatile AtomicReference<ByteBuffer>[] buffers;
    private long lastModified;
    private boolean isReadOnly;
    private boolean isLockedExclusive;
    private int sharedLockCount;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    static {
        final byte[] n = new byte[BLOCK_SIZE];
        final byte[] output = new byte[BLOCK_SIZE * 2];
        int len = new CompressLZF().compress(n, 0, BLOCK_SIZE, output, 0);
        COMPRESSED_EMPTY_BLOCK = ByteBuffer.allocateDirect(len);
        COMPRESSED_EMPTY_BLOCK.put(output, 0, len);
    }

    @SuppressWarnings("unchecked")
    FileNioMemData(String name, boolean compress, float compressLaterCachePercent) {
        this.name = name;
        this.nameHashCode = name.hashCode();
        this.compress = compress;
        this.compressLaterCachePercent = compressLaterCachePercent;
        buffers = new AtomicReference[0];
        lastModified = System.currentTimeMillis();
    }

    /**
     * Lock the file in exclusive mode if possible.
     *
     * @return if locking was successful
     */
    synchronized boolean lockExclusive() {
        if (sharedLockCount > 0 || isLockedExclusive) {
            return false;
        }
        isLockedExclusive = true;
        return true;
    }

    /**
     * Lock the file in shared mode if possible.
     *
     * @return if locking was successful
     */
    synchronized boolean lockShared() {
        if (isLockedExclusive) {
            return false;
        }
        sharedLockCount++;
        return true;
    }

    /**
     * Unlock the file.
     */
    synchronized void unlock() {
        if (isLockedExclusive) {
            isLockedExclusive = false;
        } else {
            sharedLockCount = Math.max(0, sharedLockCount - 1);
        }
    }

    /**
     * This small cache compresses the data if an element leaves the cache.
     */
    static class CompressLaterCache<K, V> extends LinkedHashMap<K, V> {

        private static final long serialVersionUID = 1L;
        private int size;

        CompressLaterCache(int size) {
            super(size, (float) 0.75, true);
            this.size = size;
        }

        @Override
        public synchronized V put(K key, V value) {
            return super.put(key, value);
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            if (size() < size) {
                return false;
            }
            CompressItem c = (CompressItem) eldest.getKey();
            c.data.compressPage(c.page);
            return true;
        }

        public void setCacheSize(int size) {
            this.size = size;
        }
    }

    /**
     * Represents a compressed item.
     */
    static class CompressItem {

        /**
         * The file data.
         */
        public final FileNioMemData data;

        /**
         * The page to compress.
         */
        public final int page;

        public CompressItem(FileNioMemData data, int page) {
            this.data = data;
            this.page = page;
        }

        @Override
        public int hashCode() {
            return page ^ data.nameHashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof CompressItem) {
                CompressItem c = (CompressItem) o;
                return c.data == data && c.page == page;
            }
            return false;
        }

    }

    private void addToCompressLaterCache(int page) {
        CompressItem c = new CompressItem(this, page);
        compressLaterCache.put(c, c);
    }

    private ByteBuffer expandPage(int page) {
        final ByteBuffer d = buffers[page].get();
        if (d.capacity() == BLOCK_SIZE) {
            // already expanded, or not compressed
            return d;
        }
        synchronized (d) {
            if (d.capacity() == BLOCK_SIZE) {
                return d;
            }
            ByteBuffer out = ByteBuffer.allocateDirect(BLOCK_SIZE);
            if (d != COMPRESSED_EMPTY_BLOCK) {
                d.position(0);
                CompressLZF.expand(d, out);
            }
            buffers[page].compareAndSet(d, out);
            return out;
        }
    }

    /**
     * Compress the data in a byte array.
     *
     * @param page which page to compress
     */
    void compressPage(int page) {
        final ByteBuffer d = buffers[page].get();
        synchronized (d) {
            if (d.capacity() != BLOCK_SIZE) {
                // already compressed
                return;
            }
            final byte[] compressOutputBuffer = COMPRESS_OUT_BUF_THREAD_LOCAL.get();
            int len = LZF_THREAD_LOCAL.get().compress(d, 0, compressOutputBuffer, 0);
            ByteBuffer out = ByteBuffer.allocateDirect(len);
            out.put(compressOutputBuffer, 0, len);
            buffers[page].compareAndSet(d, out);
        }
    }

    /**
     * Update the last modified time.
     *
     * @param openReadOnly if the file was opened in read-only mode
     */
    void touch(boolean openReadOnly) {
        if (isReadOnly || openReadOnly) {
            throw new NonWritableChannelException();
        }
        lastModified = System.currentTimeMillis();
    }

    /**
     * Get the file length.
     *
     * @return the length
     */
    long length() {
        return length;
    }

    /**
     * Truncate the file.
     *
     * @param newLength the new length
     */
    void truncate(long newLength) {
        rwLock.writeLock().lock();
        try {
            changeLength(newLength);
            long end = MathUtils.roundUpLong(newLength, BLOCK_SIZE);
            if (end != newLength) {
                int lastPage = (int) (newLength >>> BLOCK_SIZE_SHIFT);
                ByteBuffer d = expandPage(lastPage);
                for (int i = (int) (newLength & BLOCK_SIZE_MASK); i < BLOCK_SIZE; i++) {
                    d.put(i, (byte) 0);
                }
                if (compress) {
                    addToCompressLaterCache(lastPage);
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private void changeLength(long len) {
        length = len;
        len = MathUtils.roundUpLong(len, BLOCK_SIZE);
        int blocks = (int) (len >>> BLOCK_SIZE_SHIFT);
        if (blocks != buffers.length) {
            final AtomicReference<ByteBuffer>[] newBuffers = new AtomicReference[blocks];
            System.arraycopy(buffers, 0, newBuffers, 0,
                    Math.min(buffers.length, newBuffers.length));
            for (int i = buffers.length; i < blocks; i++) {
                newBuffers[i] = new AtomicReference<>(COMPRESSED_EMPTY_BLOCK);
            }
            buffers = newBuffers;
        }
        compressLaterCache.setCacheSize(Math.max(CACHE_MIN_SIZE, (int) (blocks *
                compressLaterCachePercent / 100)));
    }

    /**
     * Read or write.
     *
     * @param pos the position
     * @param b the byte array
     * @param off the offset within the byte array
     * @param len the number of bytes
     * @param write true for writing
     * @return the new position
     */
    long readWrite(long pos, ByteBuffer b, int off, int len, boolean write) {
        final java.util.concurrent.locks.Lock lock = write ? rwLock.writeLock()
                : rwLock.readLock();
        lock.lock();
        try {

            long end = pos + len;
            if (end > length) {
                if (write) {
                    changeLength(end);
                } else {
                    len = (int) (length - pos);
                }
            }
            while (len > 0) {
                final int l = (int) Math.min(len, BLOCK_SIZE - (pos & BLOCK_SIZE_MASK));
                final int page = (int) (pos >>> BLOCK_SIZE_SHIFT);
                final ByteBuffer block = expandPage(page);
                int blockOffset = (int) (pos & BLOCK_SIZE_MASK);
                if (write) {
                    final ByteBuffer srcTmp = b.slice();
                    final ByteBuffer dstTmp = block.duplicate();
                    srcTmp.position(off);
                    srcTmp.limit(off + l);
                    dstTmp.position(blockOffset);
                    dstTmp.put(srcTmp);
                } else {
                    // duplicate, so this can be done concurrently
                    final ByteBuffer tmp = block.duplicate();
                    tmp.position(blockOffset);
                    tmp.limit(l + blockOffset);
                    int oldPosition = b.position();
                    b.position(off);
                    b.put(tmp);
                    // restore old position
                    b.position(oldPosition);
                }
                if (compress) {
                    addToCompressLaterCache(page);
                }
                off += l;
                pos += l;
                len -= l;
            }
            return pos;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Set the file name.
     *
     * @param name the name
     */
    void setName(String name) {
        this.name = name;
    }

    /**
     * Get the file name
     *
     * @return the name
     */
    String getName() {
        return name;
    }

    /**
     * Get the last modified time.
     *
     * @return the time
     */
    long getLastModified() {
        return lastModified;
    }

    /**
     * Check whether writing is allowed.
     *
     * @return true if it is
     */
    boolean canWrite() {
        return !isReadOnly;
    }

    /**
     * Set the read-only flag.
     *
     * @return true
     */
    boolean setReadOnly() {
        isReadOnly = true;
        return true;
    }

}