/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
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
import java.nio.channels.FileChannel.MapMode;
import org.h2.constant.SysProperties;
import org.h2.util.IOUtils;

/**
 * FileObject which is using NIO MappedByteBuffer mapped to memory from file.
 * The file size is limited to 2 GB.
 */
public class FileObjectDiskMapped implements FileObject {

    private static final long GC_TIMEOUT_MS = 10000;
    private final String name;
    private final MapMode mode;
    private RandomAccessFile file;
    private MappedByteBuffer mapped;

    FileObjectDiskMapped(String fileName, String mode) throws IOException {
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
        if (mapped != null) {
            // first write all data
            mapped.force();

            // need to dispose old direct buffer, see bug
            // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038

            boolean useSystemGc;
            if (SysProperties.NIO_CLEANER_HACK) {
                try {
                    useSystemGc = false;
                    Method cleanerMethod = mapped.getClass().getMethod("cleaner");
                    cleanerMethod.setAccessible(true);
                    Object cleaner = cleanerMethod.invoke(mapped);
                    Method clearMethod = cleaner.getClass().getMethod("clear");
                    clearMethod.invoke(cleaner);
                } catch (Throwable e) {
                    useSystemGc = true;
                } finally {
                    mapped = null;
                }
            } else {
                useSystemGc = true;
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
    }

    /**
     * Re-map byte buffer into memory, called when file size has changed or file
     * was created.
     */
    private void reMap() throws IOException {
        int oldPos = 0;
        if (mapped != null) {
            oldPos = mapped.position();
            mapped.force();
            unMap();
        }
        long length = file.length();
        checkFileLength(length);
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
        int pos = Math.min(oldPos, (int) length);
        mapped.position(pos);
    }

    private void checkFileLength(long length) throws IOException {
        if (length > Integer.MAX_VALUE) {
            throw new IOException("File over 2GB is not supported yet when using this file system");
        }
    }

    public void close() throws IOException {
        unMap();
        file.close();
        file = null;
    }

    public long getFilePointer() {
        return mapped.position();
    }

    public String getName() {
        return name;
    }

    public long length() throws IOException {
        return file.length();
    }

    public void readFully(byte[] b, int off, int len) throws EOFException {
        try {
            mapped.get(b, off, len);
        } catch (BufferUnderflowException e) {
            EOFException e2 = new EOFException("EOF");
            e2.initCause(e);
            throw e2;
        }
    }

    public void seek(long pos) throws IOException {
        checkFileLength(pos);
        mapped.position((int) pos);
    }

    public void setFileLength(long newLength) throws IOException {
        checkFileLength(newLength);
        IOUtils.setLength(file, newLength);
        reMap();
    }

    public void sync() throws IOException {
        file.getFD().sync();
        mapped.force();
    }

    public void write(byte[] b, int off, int len) throws IOException {
        // check if need to expand file
        if (mapped.capacity() < mapped.position() + len) {
            setFileLength(mapped.position() + len);
        }
        mapped.put(b, off, len);
    }

}
