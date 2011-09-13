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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;

/**
 * File which uses NIO FileChannel.
 */
public class FileObjectDiskChannel implements FileObject {

    private final String name;
    private final RandomAccessFile file;
    private final FileChannel channel;
    private FileLock lock;
    private long length;

    FileObjectDiskChannel(String fileName, String mode) throws IOException {
        this.name = fileName;
        file = new RandomAccessFile(fileName, mode);
        channel = file.getChannel();
        length = file.length();
    }

    public void close() throws IOException {
        channel.close();
        file.close();
    }

    public long position() throws IOException {
        return channel.position();
    }

    public long size() throws IOException {
        return channel.size();
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return;
        }
        if (channel.position() + len > length) {
            throw new EOFException();
        }
        ByteBuffer buf = ByteBuffer.wrap(b);
        buf.position(off);
        buf.limit(off + len);
        channel.read(buf);
    }

    public void position(long pos) throws IOException {
        channel.position(pos);
    }

    public void truncate(long newLength) throws IOException {
        if (newLength >= channel.size()) {
            return;
        }
        long pos = channel.position();
        try {
            channel.truncate(newLength);
        } catch (NonWritableChannelException e) {
            throw new IOException("read only");
        }
        if (pos > newLength) {
            pos = newLength;
        }
        channel.position(pos);
        length = newLength;
    }

    public void sync() throws IOException {
        channel.force(true);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(b);
        buf.position(off);
        buf.limit(off + len);
        try {
            channel.write(buf);
        } catch (NonWritableChannelException e) {
            throw new IOException("read only");
        }
    }

    public synchronized boolean tryLock() {
        if (lock == null) {
            try {
                lock = channel.tryLock();
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

    public String toString() {
        return name;
    }

}
