/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: Jan Kotek
 */
package org.h2.store.fs;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileLock;
import java.nio.channels.FileChannel.MapMode;
import org.h2.constant.SysProperties;

/**
 * FileObject which is using NIO MappedByteBuffer mapped to memory from file.
 * The file size is limited to 2 GB.
 */
public class FileObjectNioMapped implements FileObject {

    private static final long GC_TIMEOUT_MS = 10000;
    private final String name;
    private final MapMode mode;
    private RandomAccessFile file;
    private MappedByteBuffer mapped;
    private FileLock lock;

    /**
     * The position within the file. Can't use the position of the mapped buffer
     * because it doesn't support seeking past the end of the file.
     */
    private int pos;

    FileObjectNioMapped(String fileName, String mode) throws IOException {
        if ("r".equals(mode)) {
            this.mode = MapMode.READ_ONLY;
        } else {
            this.mode = MapMode.READ_WRITE;
        }
        this.name = fileName;
        file = new RandomAccessFile(fileName, mode);
        reMap();
    }

    private void unMap() throws IOException {
        if (mapped == null) {
            return;
        }
        // first write all data
        mapped.force();

        // need to dispose old direct buffer, see bug
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038

        boolean useSystemGc = true;
        if (SysProperties.NIO_CLEANER_HACK) {
            try {
                Method cleanerMethod = mapped.getClass().getMethod("cleaner");
                cleanerMethod.setAccessible(true);
                Object cleaner = cleanerMethod.invoke(mapped);
                if (cleaner != null) {
                    Method clearMethod = cleaner.getClass().getMethod("clean");
                    clearMethod.invoke(cleaner);
                }
                useSystemGc = false;
            } catch (Throwable e) {
                // useSystemGc is already true
            } finally {
                mapped = null;
            }
        }
        if (useSystemGc) {
            WeakReference<MappedByteBuffer> bufferWeakRef = new WeakReference<MappedByteBuffer>(mapped);
            mapped = null;
            long start = System.currentTimeMillis();
            while (bufferWeakRef.get() != null) {
                if (System.currentTimeMillis() - start > GC_TIMEOUT_MS) {
                    throw new IOException("Timeout (" + GC_TIMEOUT_MS
                            + " ms) reached while trying to GC mapped buffer");
                }
                System.gc();
                Thread.yield();
            }
        }
    }

    /**
     * Re-map byte buffer into memory, called when file size has changed or file
     * was created.
     */
    private void reMap() throws IOException {
        int oldPos = 0;
        if (mapped != null) {
            oldPos = pos;
            unMap();
        }
        long length = file.length();
        checkFileSizeLimit(length);
        // maps new MappedByteBuffer; the old one is disposed during GC
        mapped = file.getChannel().map(mode, 0, length);
        int limit = mapped.limit();
        int capacity = mapped.capacity();
        if (limit < length || capacity < length) {
            throw new IOException("Unable to map: length=" + limit + " capacity=" + capacity + " length=" + length);
        }
        if (SysProperties.NIO_LOAD_MAPPED) {
            mapped.load();
        }
        this.pos = Math.min(oldPos, (int) length);
    }

    private static void checkFileSizeLimit(long length) throws IOException {
        if (length > Integer.MAX_VALUE) {
            throw new IOException("File over 2GB is not supported yet when using this file system");
        }
    }

    public synchronized void close() throws IOException {
        if (file != null) {
            unMap();
            file.close();
            file = null;
        }
    }

    public long position() {
        return pos;
    }

    public String toString() {
        return "nioMapped:" + name;
    }

    public synchronized long size() throws IOException {
        return file.length();
    }

    public synchronized void readFully(byte[] b, int off, int len) throws EOFException {
        try {
            mapped.position(pos);
            mapped.get(b, off, len);
            pos += len;
        } catch (IllegalArgumentException e) {
            EOFException e2 = new EOFException("EOF");
            e2.initCause(e);
            throw e2;
        } catch (BufferUnderflowException e) {
            EOFException e2 = new EOFException("EOF");
            e2.initCause(e);
            throw e2;
        }
    }

    public void position(long pos) throws IOException {
        checkFileSizeLimit(pos);
        this.pos = (int) pos;
    }

    public synchronized void truncate(long newLength) throws IOException {
        if (newLength >= size()) {
            return;
        }
        setFileLength(newLength);
    }

    public synchronized void setFileLength(long newLength) throws IOException {
        checkFileSizeLimit(newLength);
        int oldPos = pos;
        unMap();
        for (int i = 0;; i++) {
            try {
                file.setLength(newLength);
                break;
            } catch (IOException e) {
                if (i > 16 || e.toString().indexOf("user-mapped section open") < 0) {
                    throw e;
                }
            }
            System.gc();
        }
        reMap();
        pos = (int) Math.min(newLength, oldPos);
    }

    public synchronized void sync() throws IOException {
        mapped.force();
        file.getFD().sync();
    }

    public synchronized void write(byte[] b, int off, int len) throws IOException {
        // check if need to expand file
        if (mapped.capacity() < pos + len) {
            setFileLength(pos + len);
        }
        mapped.position(pos);
        mapped.put(b, off, len);
        pos += len;
    }

    public synchronized boolean tryLock() {
        if (lock == null) {
            try {
                lock = file.getChannel().tryLock();
            } catch (IOException e) {
                // could not lock
            }
            return lock != null;
        }
        return false;
    }

    public synchronized void releaseLock() {
        if (lock != null) {
            try {
                lock.release();
            } catch (IOException e) {
                // ignore
            }
            lock = null;
        }
    }

}
