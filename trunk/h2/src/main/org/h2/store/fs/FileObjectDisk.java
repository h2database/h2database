/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import org.h2.constant.SysProperties;

/**
 * This class extends a java.io.RandomAccessFile.
 */
public class FileObjectDisk implements FileObject {

    private final RandomAccessFile file;
    private final String name;
    private FileLock lock;

    FileObjectDisk(String fileName, String mode) throws FileNotFoundException {
        this.file = new RandomAccessFile(fileName, mode);
        this.name = fileName;
    }

    public void sync() throws IOException {
        String m = SysProperties.SYNC_METHOD;
        if ("".equals(m)) {
            // do nothing
        } else if ("sync".equals(m)) {
            file.getFD().sync();
        } else if ("force".equals(m)) {
            file.getChannel().force(true);
        } else if ("forceFalse".equals(m)) {
            file.getChannel().force(false);
        } else {
            file.getFD().sync();
        }
    }

    public void truncate(long newLength) throws IOException {
        if (newLength < file.length()) {
            // some implementations actually only support truncate
            file.setLength(newLength);
        }
    }

    public synchronized boolean tryLock() {
        if (lock == null) {
            try {
                lock = file.getChannel().tryLock();
            } catch (Exception e) {
                // could not lock (OverlappingFileLockException)
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

    public void close() throws IOException {
        file.close();
    }

    public long position() throws IOException {
        return file.getFilePointer();
    }

    public long size() throws IOException {
        return file.length();
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        file.readFully(b, off, len);
    }

    public void position(long pos) throws IOException {
        file.seek(pos);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        file.write(b, off, len);
    }

    public String toString() {
        return name;
    }

}
