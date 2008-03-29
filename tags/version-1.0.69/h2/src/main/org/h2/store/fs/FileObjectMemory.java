/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
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
    private String name;
    private final boolean compress;
    private long length;
    private long pos;
    private byte[][] data;
    private long lastModified;
    
    private static final CompressLZF LZF = new CompressLZF();
    private static final byte[] BUFFER = new byte[BLOCK_SIZE * 2];
    private static final byte[] COMPRESSED_BLOCK;
    
//#ifdef JDK14
    static class Cache extends LinkedHashMap {
        private static final long serialVersionUID = 5549197956072850355L;
        private int size;
      
        public Cache(int size) {
            this.size = size;
        }

        protected boolean removeEldestEntry(Map.Entry eldest) {
            if (size() < size) {
                return false;
            }
            CompressItem c = (CompressItem) eldest.getKey();
            compress(c.data, c.l);
            return true;
        }
    }

    static class CompressItem {
        byte[][] data;
        int l;

        public int hashCode() {
            return data.hashCode() ^ l;
        }

        public boolean equals(Object o) {
            if (o instanceof CompressItem) {
                CompressItem c = (CompressItem) o;
                return c.data == data && c.l == l;
            }
            return false;
        }
    }
    private static final Cache COMPRESS_LATER = new Cache(CACHE_SIZE);
//#endif
    
    
    private static void compressLater(byte[][] data, int l) {
//#ifdef JDK14
        CompressItem c = new CompressItem();
        c.data = data;
        c.l = l;
        synchronized (LZF) {
            COMPRESS_LATER.put(c, c);
        }
//#endif
    }
    
    private static void expand(byte[][] data, int i) {
        byte[] d = data[i];
        if (d.length == BLOCK_SIZE) {
            return;
        }
        byte[] out = new byte[BLOCK_SIZE];
        synchronized (LZF) {
            LZF.expand(d, 0, d.length, out, 0, BLOCK_SIZE);
        }
        data[i] = out;
    }

    private static void compress(byte[][] data, int i) {
        byte[] d = data[i];
        synchronized (LZF) {
            int len = LZF.compress(d, BLOCK_SIZE, BUFFER, 0);
            if (len <= BLOCK_SIZE) {
                d = new byte[len];
                System.arraycopy(BUFFER, 0, d, 0, len);
                data[i] = d;
            }
        }
    }
    
    static {
        byte[] n = new byte[BLOCK_SIZE];
        int len = LZF.compress(n, BLOCK_SIZE, BUFFER, 0);
        COMPRESSED_BLOCK = new byte[len];
        System.arraycopy(BUFFER, 0, COMPRESSED_BLOCK, 0, len);
    }
    
    public FileObjectMemory(String name, boolean compress) {
        this.name = name;
        this.compress = compress;
        data = new byte[0][];
        touch();
    }
    
    private void touch() {
        lastModified = System.currentTimeMillis();
    }
    
    public long length() {
        return length;
    }
    
    public void setFileLength(long l) {
        touch();
        if (l < length) {
            pos = Math.min(pos, l);
            changeLength(l);
            long end = MathUtils.roundUpLong(l, BLOCK_SIZE);
            if (end != l) {
                int lastBlock = (int) (l >>> BLOCK_SIZE_SHIFT);
                expand(data, lastBlock);
                byte[] d = data[lastBlock];
                for (int i = (int) (l & BLOCK_SIZE_MASK); i < BLOCK_SIZE; i++) {
                    d[i] = 0;
                }
                if (compress) {
                    compressLater(data, lastBlock);
                }
            }
        } else {
            changeLength(l);
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
                n[i] = COMPRESSED_BLOCK;
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
            int id = (int) (pos >>> BLOCK_SIZE_SHIFT);
            expand(data, id);
            byte[] block = data[id];
            int blockOffset = (int) (pos & BLOCK_SIZE_MASK);
            if (write) {
                System.arraycopy(b, off, block, blockOffset, l);
            } else {
                System.arraycopy(block, blockOffset, b, off, l);
            }
            if (compress) {
                compressLater(data, id);
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

    public void sync() throws IOException {
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
