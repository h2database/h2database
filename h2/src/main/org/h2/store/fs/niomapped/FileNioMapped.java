/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.niomapped;

import java.io.EOFException;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.Paths;
import org.h2.engine.SysProperties;
import org.h2.store.fs.FileBaseDefault;
import org.h2.store.fs.FileUtils;
import org.h2.util.MemoryUnmapper;

/**
 * Uses memory mapped files.
 * The file size is limited to 2 GB.
 */
class FileNioMapped extends FileBaseDefault {

    private static final int GC_TIMEOUT_MS = 10_000;
    private final String name;
    private final MapMode mode;
    private FileChannel channel;
    private MappedByteBuffer mapped;
    private long fileLength;

    FileNioMapped(String fileName, String mode) throws IOException {
        if ("r".equals(mode)) {
            this.mode = MapMode.READ_ONLY;
        } else {
            this.mode = MapMode.READ_WRITE;
        }
        this.name = fileName;
        channel = FileChannel.open(Paths.get(fileName), FileUtils.modeToOptions(mode), FileUtils.NO_ATTRIBUTES);
        reMap();
    }

    private void unMap() throws IOException {
        if (mapped == null) {
            return;
        }
        // first write all data
        mapped.force();

        // need to dispose old direct buffer, see bug
        // https://bugs.openjdk.java.net/browse/JDK-4724038

        if (SysProperties.NIO_CLEANER_HACK) {
            if (MemoryUnmapper.unmap(mapped)) {
                mapped = null;
                return;
            }
        }
        WeakReference<MappedByteBuffer> bufferWeakRef = new WeakReference<>(mapped);
        mapped = null;
        long stopAt = System.nanoTime() + GC_TIMEOUT_MS * 1_000_000L;
        while (bufferWeakRef.get() != null) {
            if (System.nanoTime() - stopAt > 0L) {
                throw new IOException("Timeout (" + GC_TIMEOUT_MS + " ms) reached while trying to GC mapped buffer");
            }
            System.gc();
            Thread.yield();
        }
    }

    /**
     * Re-map byte buffer into memory, called when file size has changed or file
     * was created.
     */
    private void reMap() throws IOException {
        if (mapped != null) {
            unMap();
        }
        fileLength = channel.size();
        checkFileSizeLimit(fileLength);
        // maps new MappedByteBuffer; the old one is disposed during GC
        mapped = channel.map(mode, 0, fileLength);
        int limit = mapped.limit();
        int capacity = mapped.capacity();
        if (limit < fileLength || capacity < fileLength) {
            throw new IOException("Unable to map: length=" + limit +
                    " capacity=" + capacity + " length=" + fileLength);
        }
        if (SysProperties.NIO_LOAD_MAPPED) {
            mapped.load();
        }
    }

    private static void checkFileSizeLimit(long length) throws IOException {
        if (length > Integer.MAX_VALUE) {
            throw new IOException(
                    "File over 2GB is not supported yet when using this file system");
        }
    }

    @Override
    public void implCloseChannel() throws IOException {
        if (channel != null) {
            unMap();
            channel.close();
            channel = null;
        }
    }

    @Override
    public String toString() {
        return "nioMapped:" + name;
    }

    @Override
    public synchronized long size() throws IOException {
        return fileLength;
    }

    @Override
    public synchronized int read(ByteBuffer dst, long pos) throws IOException {
        checkFileSizeLimit(pos);
        try {
            int len = dst.remaining();
            if (len == 0) {
                return 0;
            }
            len = (int) Math.min(len, fileLength - pos);
            if (len <= 0) {
                return -1;
            }
            mapped.position((int)pos);
            mapped.get(dst.array(), dst.arrayOffset() + dst.position(), len);
            dst.position(dst.position() + len);
            pos += len;
            return len;
        } catch (IllegalArgumentException | BufferUnderflowException e) {
            EOFException e2 = new EOFException("EOF");
            e2.initCause(e);
            throw e2;
        }
    }

    @Override
    protected void implTruncate(long newLength) throws IOException {
        // compatibility with JDK FileChannel#truncate
        if (mode == MapMode.READ_ONLY) {
            throw new NonWritableChannelException();
        }
        if (newLength < size()) {
            setFileLength(newLength);
        }
    }

    public synchronized void setFileLength(long newLength) throws IOException {
        if (mode == MapMode.READ_ONLY) {
            throw new NonWritableChannelException();
        }
        checkFileSizeLimit(newLength);
        unMap();
        for (int i = 0;; i++) {
            try {
                long length = channel.size();
                if (length >= newLength) {
                    channel.truncate(newLength);
                } else {
                    channel.write(ByteBuffer.wrap(new byte[1]), newLength - 1);
                }
                break;
            } catch (IOException e) {
                if (i > 16 || !e.toString().contains("user-mapped section open")) {
                    throw e;
                }
            }
            System.gc();
        }
        reMap();
    }

    @Override
    public void force(boolean metaData) throws IOException {
        mapped.force();
        channel.force(metaData);
    }

    @Override
    public synchronized int write(ByteBuffer src, long position) throws IOException {
        checkFileSizeLimit(position);
        int len = src.remaining();
        // check if need to expand file
        if (mapped.capacity() < position + len) {
            setFileLength(position + len);
        }
        mapped.position((int)position);
        mapped.put(src);
        return len;
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        return channel.tryLock(position, size, shared);
    }

}