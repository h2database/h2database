/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.h2.compress.CompressLZF;
import org.h2.util.MathUtils;

/**
 * This class is an abstraction of an in-memory random access file.
 * Data compression using the LZF algorithm is supported as well.
 */
public class FileObjectMemory implements FileObject {
    private static final int CACHE_SIZE = 8;
    private static final int BLOCK_SIZE_SHIFT = 16;
    private static final int BLOCK_SIZE = 1 << BLOCK_SIZE_SHIFT;
    private static final int BLOCK_SIZE_MASK = BLOCK_SIZE - 1;
    private static final CompressLZF LZF = new CompressLZF();
    private static final byte[] BUFFER = new byte[BLOCK_SIZE * 2];
    private static byte[] cachedCompressedEmptyBlock;

//## Java 1.4 begin ##
    private static final Cache<CompressItem, CompressItem> COMPRESS_LATER = new Cache<CompressItem, CompressItem>(CACHE_SIZE);
//## Java 1.4 end ##

    private String name;
    private final boolean compress;
    private long length;
    private long pos;
    private byte[][] data;
    private long lastModified;

    /**
     * This small cache compresses the data if an element leaves the cache.
     */
//## Java 1.4 begin ##
    static class Cache<K, V> extends LinkedHashMap<K, V> {
        private static final long serialVersionUID = 5549197956072850355L;
        private int size;

        Cache(int size) {
            super(size, (float) 0.75, true);
            this.size = size;
        }

        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            if (size() < size) {
                return false;
            }
            CompressItem c = (CompressItem) eldest.getKey();
            compress(c.data, c.page);
            return true;
        }
    }

    /**
     * Represents a compressed item.
     */
    static class CompressItem {

        /**
         * The file data.
         */
        byte[][] data;

        /**
         * The page to compress.
         */
        int page;

        public int hashCode() {
            return data.hashCode() ^ page;
        }

        public boolean equals(Object o) {
            if (o instanceof CompressItem) {
                CompressItem c = (CompressItem) o;
                return c.data == data && c.page == page;
            }
            return false;
        }
    }
//## Java 1.4 end ##

    FileObjectMemory(String name, boolean compress) {
        this.name = name;
        this.compress = compress;
        data = new byte[0][];
        touch();
    }

    private static void compressLater(byte[][] data, int page) {
//## Java 1.4 begin ##
        CompressItem c = new CompressItem();
        c.data = data;
        c.page = page;
        synchronized (LZF) {
            COMPRESS_LATER.put(c, c);
        }
//## Java 1.4 end ##
    }

    private static void expand(byte[][] data, int page) {
        byte[] d = data[page];
        if (d.length == BLOCK_SIZE) {
            return;
        }
        byte[] out = new byte[BLOCK_SIZE];
        synchronized (LZF) {
            LZF.expand(d, 0, d.length, out, 0, BLOCK_SIZE);
        }
        data[page] = out;
    }

    /**
     * Compress the data in a byte array.
     *
     * @param data the page array
     * @param page which page to compress
     */
    static void compress(byte[][] data, int page) {
        byte[] d = data[page];
        synchronized (LZF) {
            int len = LZF.compress(d, BLOCK_SIZE, BUFFER, 0);
            if (len <= BLOCK_SIZE) {
                d = new byte[len];
                System.arraycopy(BUFFER, 0, d, 0, len);
                data[page] = d;
            }
        }
    }

    static byte[] getCompressedEmptyBlock() {
        if (cachedCompressedEmptyBlock == null) {
            byte[] n = new byte[BLOCK_SIZE];
            int len = LZF.compress(n, BLOCK_SIZE, BUFFER, 0);
            cachedCompressedEmptyBlock = new byte[len];
            System.arraycopy(BUFFER, 0, cachedCompressedEmptyBlock, 0, len);
        }
        return cachedCompressedEmptyBlock;
    }

    private void touch() {
        lastModified = System.currentTimeMillis();
    }

    public long length() {
        return length;
    }

    public void setFileLength(long newLength) {
        touch();
        if (newLength < length) {
            pos = Math.min(pos, newLength);
            changeLength(newLength);
            long end = MathUtils.roundUpLong(newLength, BLOCK_SIZE);
            if (end != newLength) {
                int lastPage = (int) (newLength >>> BLOCK_SIZE_SHIFT);
                expand(data, lastPage);
                byte[] d = data[lastPage];
                for (int i = (int) (newLength & BLOCK_SIZE_MASK); i < BLOCK_SIZE; i++) {
                    d[i] = 0;
                }
                if (compress) {
                    compressLater(data, lastPage);
                }
            }
        } else {
            changeLength(newLength);
        }
    }

    public void seek(long pos) {
        this.pos = (int) pos;
    }

    private void changeLength(long len) {
        length = len;
        len = MathUtils.roundUpLong(len, BLOCK_SIZE);
        int blocks = (int) (len >>> BLOCK_SIZE_SHIFT);
        if (blocks != data.length) {
            byte[][] n = new byte[blocks][];
            System.arraycopy(data, 0, n, 0, Math.min(data.length, n.length));
            for (int i = data.length; i < blocks; i++) {
                n[i] = getCompressedEmptyBlock();
            }
            data = n;
        }

    }

    private void readWrite(byte[] b, int off, int len, boolean write) throws IOException {
        long end = pos + len;
        if (end > length) {
            if (write) {
                changeLength(end);
            } else {
                if (len == 0) {
                    return;
                }
                throw new EOFException("File: " + name);
            }
        }
        while (len > 0) {
            int l = (int) Math.min(len, BLOCK_SIZE - (pos & BLOCK_SIZE_MASK));
            int page = (int) (pos >>> BLOCK_SIZE_SHIFT);
            expand(data, page);
            byte[] block = data[page];
            int blockOffset = (int) (pos & BLOCK_SIZE_MASK);
            if (write) {
                System.arraycopy(b, off, block, blockOffset, l);
            } else {
                System.arraycopy(block, blockOffset, b, off, l);
            }
            if (compress) {
                compressLater(data, page);
            }
            off += l;
            pos += l;
            len -= l;
        }
    }

    public void write(byte[] b, int off, int len) throws IOException {
        touch();
        readWrite(b, off, len, true);
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        readWrite(b, off, len, false);
    }

    public long getFilePointer() {
        return pos;
    }

    public void close() {
        pos = 0;
    }

    public void sync() {
        // nothing to do
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public long getLastModified() {
        return lastModified;
    }
}
