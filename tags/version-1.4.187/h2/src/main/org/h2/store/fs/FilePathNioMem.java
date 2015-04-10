/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.h2.api.ErrorCode;
import org.h2.compress.CompressLZF;
import org.h2.message.DbException;
import org.h2.util.MathUtils;
import org.h2.util.New;

/**
 * This file system keeps files fully in memory. There is an option to compress
 * file blocks to save memory.
 */
public class FilePathNioMem extends FilePath {

    private static final TreeMap<String, FileNioMemData> MEMORY_FILES =
            new TreeMap<String, FileNioMemData>();

    @Override
    public FilePathNioMem getPath(String path) {
        FilePathNioMem p = new FilePathNioMem();
        p.name = getCanonicalPath(path);
        return p;
    }

    @Override
    public long size() {
        return getMemoryFile().length();
    }

    @Override
    public void moveTo(FilePath newName, boolean atomicReplace) {
        synchronized (MEMORY_FILES) {
            if (!atomicReplace && !name.equals(newName.name) &&
                    MEMORY_FILES.containsKey(newName.name)) {
                throw DbException.get(ErrorCode.FILE_RENAME_FAILED_2,
                        new String[] { name, newName + " (exists)" });
            }
            FileNioMemData f = getMemoryFile();
            f.setName(newName.name);
            MEMORY_FILES.remove(name);
            MEMORY_FILES.put(newName.name, f);
        }
    }

    @Override
    public boolean createFile() {
        synchronized (MEMORY_FILES) {
            if (exists()) {
                return false;
            }
            getMemoryFile();
        }
        return true;
    }

    @Override
    public boolean exists() {
        if (isRoot()) {
            return true;
        }
        synchronized (MEMORY_FILES) {
            return MEMORY_FILES.get(name) != null;
        }
    }

    @Override
    public void delete() {
        if (isRoot()) {
            return;
        }
        synchronized (MEMORY_FILES) {
            MEMORY_FILES.remove(name);
        }
    }

    @Override
    public List<FilePath> newDirectoryStream() {
        ArrayList<FilePath> list = New.arrayList();
        synchronized (MEMORY_FILES) {
            for (String n : MEMORY_FILES.tailMap(name).keySet()) {
                if (n.startsWith(name)) {
                    list.add(getPath(n));
                } else {
                    break;
                }
            }
            return list;
        }
    }

    @Override
    public boolean setReadOnly() {
        return getMemoryFile().setReadOnly();
    }

    @Override
    public boolean canWrite() {
        return getMemoryFile().canWrite();
    }

    @Override
    public FilePathNioMem getParent() {
        int idx = name.lastIndexOf('/');
        return idx < 0 ? null : getPath(name.substring(0, idx));
    }

    @Override
    public boolean isDirectory() {
        if (isRoot()) {
            return true;
        }
        // TODO in memory file system currently
        // does not really support directories
        synchronized (MEMORY_FILES) {
            return MEMORY_FILES.get(name) == null;
        }
    }

    @Override
    public boolean isAbsolute() {
        // TODO relative files are not supported
        return true;
    }

    @Override
    public FilePathNioMem toRealPath() {
        return this;
    }

    @Override
    public long lastModified() {
        return getMemoryFile().getLastModified();
    }

    @Override
    public void createDirectory() {
        if (exists() && isDirectory()) {
            throw DbException.get(ErrorCode.FILE_CREATION_FAILED_1,
                    name + " (a file with this name already exists)");
        }
        // TODO directories are not really supported
    }

    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        FileNioMemData obj = getMemoryFile();
        FileNioMem m = new FileNioMem(obj, false);
        return new FileChannelOutputStream(m, append);
    }

    @Override
    public InputStream newInputStream() {
        FileNioMemData obj = getMemoryFile();
        FileNioMem m = new FileNioMem(obj, true);
        return new FileChannelInputStream(m, true);
    }

    @Override
    public FileChannel open(String mode) {
        FileNioMemData obj = getMemoryFile();
        return new FileNioMem(obj, "r".equals(mode));
    }

    private FileNioMemData getMemoryFile() {
        synchronized (MEMORY_FILES) {
            FileNioMemData m = MEMORY_FILES.get(name);
            if (m == null) {
                m = new FileNioMemData(name, compressed());
                MEMORY_FILES.put(name, m);
            }
            return m;
        }
    }

    private boolean isRoot() {
        return name.equals(getScheme() + ":");
    }

    /**
     * Get the canonical path of a file (with backslashes replaced with forward
     * slashes).
     *
     * @param fileName the file name
     * @return the canonical path
     */
    protected static String getCanonicalPath(String fileName) {
        fileName = fileName.replace('\\', '/');
        int idx = fileName.indexOf(':') + 1;
        if (fileName.length() > idx && fileName.charAt(idx) != '/') {
            fileName = fileName.substring(0, idx) + "/" + fileName.substring(idx);
        }
        return fileName;
    }

    @Override
    public String getScheme() {
        return "nioMemFS";
    }

    /**
     * Whether the file should be compressed.
     *
     * @return if it should be compressed.
     */
    boolean compressed() {
        return false;
    }

}

/**
 * A memory file system that compresses blocks to conserve memory.
 */
class FilePathNioMemLZF extends FilePathNioMem {

    @Override
    boolean compressed() {
        return true;
    }

    @Override
    public FilePathNioMem getPath(String path) {
        FilePathNioMemLZF p = new FilePathNioMemLZF();
        p.name = getCanonicalPath(path);
        return p;
    }

    @Override
    public String getScheme() {
        return "nioMemLZF";
    }

}

/**
 * This class represents an in-memory file.
 */
class FileNioMem extends FileBase {

    /**
     * The file data.
     */
    final FileNioMemData data;

    private final boolean readOnly;
    private long pos;

    FileNioMem(FileNioMemData data, boolean readOnly) {
        this.data = data;
        this.readOnly = readOnly;
    }

    @Override
    public long size() {
        return data.length();
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        // compatibility with JDK FileChannel#truncate
        if (readOnly) {
            throw new NonWritableChannelException();
        }
        if (newLength < size()) {
            data.touch(readOnly);
            pos = Math.min(pos, newLength);
            data.truncate(newLength);
        }
        return this;
    }

    @Override
    public FileChannel position(long newPos) {
        this.pos = (int) newPos;
        return this;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int len = src.remaining();
        if (len == 0) {
            return 0;
        }
        data.touch(readOnly);
        // offset is 0 because we start writing from src.position()
        pos = data.readWrite(pos, src, 0, len, true);
        src.position(src.position() + len);
        return len;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int len = dst.remaining();
        if (len == 0) {
            return 0;
        }
        long newPos = data.readWrite(pos, dst, dst.position(), len, false);
        len = (int) (newPos - pos);
        if (len <= 0) {
            return -1;
        }
        dst.position(dst.position() + len);
        pos = newPos;
        return len;
    }

    @Override
    public long position() {
        return pos;
    }

    @Override
    public void implCloseChannel() throws IOException {
        pos = 0;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        // do nothing
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        if (shared) {
            if (!data.lockShared()) {
                return null;
            }
        } else {
            if (!data.lockExclusive()) {
                return null;
            }
        }

        // cast to FileChannel to avoid JDK 1.7 ambiguity
        FileLock lock = new FileLock((FileChannel) null, position, size, shared) {

            @Override
            public boolean isValid() {
                return true;
            }

            @Override
            public void release() throws IOException {
                data.unlock();
            }
        };
        return lock;
    }

    @Override
    public String toString() {
        return data.getName();
    }

}

/**
 * This class contains the data of an in-memory random access file.
 * Data compression using the LZF algorithm is supported as well.
 */
class FileNioMemData {

    private static final int CACHE_SIZE = 8;
    private static final int BLOCK_SIZE_SHIFT = 16;

    private static final int BLOCK_SIZE = 1 << BLOCK_SIZE_SHIFT;
    private static final int BLOCK_SIZE_MASK = BLOCK_SIZE - 1;
    private static final CompressLZF LZF = new CompressLZF();
    private static final byte[] BUFFER = new byte[BLOCK_SIZE * 2];
    private static final ByteBuffer COMPRESSED_EMPTY_BLOCK;

    private static final Cache<CompressItem, CompressItem> COMPRESS_LATER =
        new Cache<CompressItem, CompressItem>(CACHE_SIZE);

    private String name;
    private final boolean compress;
    private long length;
    private ByteBuffer[] data;
    private long lastModified;
    private boolean isReadOnly;
    private boolean isLockedExclusive;
    private int sharedLockCount;

    static {
        byte[] n = new byte[BLOCK_SIZE];
        int len = LZF.compress(n, BLOCK_SIZE, BUFFER, 0);
        COMPRESSED_EMPTY_BLOCK = ByteBuffer.allocateDirect(len);
        COMPRESSED_EMPTY_BLOCK.put(BUFFER, 0, len);
    }

    FileNioMemData(String name, boolean compress) {
        this.name = name;
        this.compress = compress;
        data = new ByteBuffer[0];
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
    static class Cache<K, V> extends LinkedHashMap<K, V> {

        private static final long serialVersionUID = 1L;
        private final int size;

        Cache(int size) {
            super(size, (float) 0.75, true);
            this.size = size;
        }

        @Override
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
        ByteBuffer[] data;

        /**
         * The page to compress.
         */
        int page;

        @Override
        public int hashCode() {
            return page;
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

    private static void compressLater(ByteBuffer[] data, int page) {
        CompressItem c = new CompressItem();
        c.data = data;
        c.page = page;
        synchronized (LZF) {
            COMPRESS_LATER.put(c, c);
        }
    }

    private static void expand(ByteBuffer[] data, int page) {
        ByteBuffer d = data[page];
        if (d.capacity() == BLOCK_SIZE) {
            return;
        }
        ByteBuffer out = ByteBuffer.allocateDirect(BLOCK_SIZE);
        if (d != COMPRESSED_EMPTY_BLOCK) {
            synchronized (LZF) {
                d.position(0);
                CompressLZF.expand(d, out);
            }
        }
        data[page] = out;
    }

    /**
     * Compress the data in a byte array.
     *
     * @param data the page array
     * @param page which page to compress
     */
    static void compress(ByteBuffer[] data, int page) {
        ByteBuffer d = data[page];
        synchronized (LZF) {
            int len = LZF.compress(d, 0, BUFFER, 0);
            d = ByteBuffer.allocateDirect(len);
            d.put(BUFFER, 0, len);
            data[page] = d;
        }
    }

    /**
     * Update the last modified time.
     *
     * @param openReadOnly if the file was opened in read-only mode
     */
    void touch(boolean openReadOnly) throws IOException {
        if (isReadOnly || openReadOnly) {
            throw new IOException("Read only");
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
            expand(data, lastPage);
            ByteBuffer d = data[lastPage];
            for (int i = (int) (newLength & BLOCK_SIZE_MASK); i < BLOCK_SIZE; i++) {
                d.put(i, (byte) 0);
            }
            if (compress) {
                compressLater(data, lastPage);
            }
        }
    }

    private void changeLength(long len) {
        length = len;
        len = MathUtils.roundUpLong(len, BLOCK_SIZE);
        int blocks = (int) (len >>> BLOCK_SIZE_SHIFT);
        if (blocks != data.length) {
            ByteBuffer[] n = new ByteBuffer[blocks];
            System.arraycopy(data, 0, n, 0, Math.min(data.length, n.length));
            for (int i = data.length; i < blocks; i++) {
                n[i] = COMPRESSED_EMPTY_BLOCK;
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
    long readWrite(long pos, ByteBuffer b, int off, int len, boolean write) {
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
            expand(data, page);
            ByteBuffer block = data[page];
            int blockOffset = (int) (pos & BLOCK_SIZE_MASK);
            if (write) {
                ByteBuffer tmp = b.slice();
                tmp.position(off);
                tmp.limit(off + l);
                block.position(blockOffset);
                block.put(tmp);
            } else {
                block.position(blockOffset);
                ByteBuffer tmp = block.slice();
                tmp.limit(l);
                int oldPosition = b.position();
                b.position(off);
                b.put(tmp);
                // restore old position
                b.position(oldPosition);
            }
            if (compress) {
                compressLater(data, page);
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


