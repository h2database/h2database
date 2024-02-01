/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.mem;

import java.io.IOException;
import java.nio.channels.NonWritableChannelException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.h2.compress.CompressLZF;
import org.h2.util.MathUtils;

/**
 * This class contains the data of an in-memory random access file.
 * Data compression using the LZF algorithm is supported as well.
 */
class FileMemData {

    private static final int CACHE_SIZE = 8;
    private static final int BLOCK_SIZE_SHIFT = 10;
    private static final int BLOCK_SIZE = 1 << BLOCK_SIZE_SHIFT;
    private static final int BLOCK_SIZE_MASK = BLOCK_SIZE - 1;
    private static final CompressLZF LZF = new CompressLZF();
    private static final byte[] BUFFER = new byte[BLOCK_SIZE * 2];
    private static final byte[] COMPRESSED_EMPTY_BLOCK;

    private static final Cache<CompressItem, CompressItem> COMPRESS_LATER =
        new Cache<>(CACHE_SIZE);

    private String name;
    private final int id;
    private final boolean compress;
    private volatile long length;
    private AtomicReference<byte[]>[] data;
    private long lastModified;
    private boolean isReadOnly;
    private boolean isLockedExclusive;
    private int sharedLockCount;

    static {
        byte[] n = new byte[BLOCK_SIZE];
        int len = LZF.compress(n, 0, BLOCK_SIZE, BUFFER, 0);
        COMPRESSED_EMPTY_BLOCK = Arrays.copyOf(BUFFER, len);
    }

    @SuppressWarnings("unchecked")
    FileMemData(String name, boolean compress) {
        this.name = name;
        this.id = name.hashCode();
        this.compress = compress;
        this.data = new AtomicReference[0];
        lastModified = System.currentTimeMillis();
    }

    /**
     * Get the page if it exists.
     *
     * @param page the page id
     * @return the byte array, or null
     */
    private byte[] getPage(int page) {
        AtomicReference<byte[]>[] b = data;
        if (page >= b.length) {
            return null;
        }
        return b[page].get();
    }

    /**
     * Set the page data.
     *
     * @param page the page id
     * @param oldData the old data
     * @param newData the new data
     * @param force whether the data should be overwritten even if the old data
     *            doesn't match
     */
    private void setPage(int page, byte[] oldData, byte[] newData, boolean force) {
        AtomicReference<byte[]>[] b = data;
        if (page >= b.length) {
            return;
        }
        if (force) {
            b[page].set(newData);
        } else {
            b[page].compareAndSet(oldData, newData);
        }
    }

    int getId() {
        return id;
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
    synchronized void unlock() throws IOException {
        if (isLockedExclusive) {
            isLockedExclusive = false;
        } else if (sharedLockCount > 0) {
            sharedLockCount--;
        } else {
            throw new IOException("not locked");
        }
    }

    /**
     * This small cache compresses the data if an element leaves the cache.
     */
    static class Cache<K, V> extends LinkedHashMap<K, V> {

        private static final long serialVersionUID = 1L;
        private final int size;

        Cache(int size) {
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
            c.file.compress(c.page);
            return true;
        }
    }

    /**
     * Points to a block of bytes that needs to be compressed.
     */
    static class CompressItem {

        /**
         * The file.
         */
        FileMemData file;

        /**
         * The page to compress.
         */
        int page;

        @Override
        public int hashCode() {
            return page ^ file.getId();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof CompressItem) {
                CompressItem c = (CompressItem) o;
                return c.page == page && c.file == file;
            }
            return false;
        }

    }

    private void compressLater(int page) {
        CompressItem c = new CompressItem();
        c.file = this;
        c.page = page;
        synchronized (LZF) {
            COMPRESS_LATER.put(c, c);
        }
    }

    private byte[] expand(int page) {
        byte[] d = getPage(page);
        if (d.length == BLOCK_SIZE) {
            return d;
        }
        byte[] out = new byte[BLOCK_SIZE];
        if (d != COMPRESSED_EMPTY_BLOCK) {
            synchronized (LZF) {
                LZF.expand(d, 0, d.length, out, 0, BLOCK_SIZE);
            }
        }
        setPage(page, d, out, false);
        return out;
    }

    /**
     * Compress the data in a byte array.
     *
     * @param page which page to compress
     */
    void compress(int page) {
        byte[] old = getPage(page);
        if (old == null || old.length != BLOCK_SIZE) {
            // not yet initialized or already compressed
            return;
        }
        synchronized (LZF) {
            int len = LZF.compress(old, 0, BLOCK_SIZE, BUFFER, 0);
            if (len <= BLOCK_SIZE) {
                byte[] d = Arrays.copyOf(BUFFER, len);
                // maybe data was changed in the meantime
                setPage(page, old, d, false);
            }
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
        changeLength(newLength);
        long end = MathUtils.roundUpLong(newLength, BLOCK_SIZE);
        if (end != newLength) {
            int lastPage = (int) (newLength >>> BLOCK_SIZE_SHIFT);
            byte[] d = expand(lastPage);
            byte[] d2 = Arrays.copyOf(d, d.length);
            for (int i = (int) (newLength & BLOCK_SIZE_MASK); i < BLOCK_SIZE; i++) {
                d2[i] = 0;
            }
            setPage(lastPage, d, d2, true);
            if (compress) {
                compressLater(lastPage);
            }
        }
    }

    private void changeLength(long len) {
        length = len;
        len = MathUtils.roundUpLong(len, BLOCK_SIZE);
        int blocks = (int) (len >>> BLOCK_SIZE_SHIFT);
        if (blocks != data.length) {
            AtomicReference<byte[]>[] n = Arrays.copyOf(data, blocks);
            for (int i = data.length; i < blocks; i++) {
                n[i] = new AtomicReference<>(COMPRESSED_EMPTY_BLOCK);
            }
            data = n;
        }
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
    long readWrite(long pos, byte[] b, int off, int len, boolean write) {
        long end = pos + len;
        if (end > length) {
            if (write) {
                changeLength(end);
            } else {
                len = (int) (length - pos);
            }
        }
        while (len > 0) {
            int l = (int) Math.min(len, BLOCK_SIZE - (pos & BLOCK_SIZE_MASK));
            int page = (int) (pos >>> BLOCK_SIZE_SHIFT);
            byte[] block = expand(page);
            int blockOffset = (int) (pos & BLOCK_SIZE_MASK);
            if (write) {
                byte[] p2 = Arrays.copyOf(block, block.length);
                System.arraycopy(b, off, p2, blockOffset, l);
                setPage(page, block, p2, true);
            } else {
                System.arraycopy(block, blockOffset, b, off, l);
            }
            if (compress) {
                compressLater(page);
            }
            off += l;
            pos += l;
            len -= l;
        }
        return pos;
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