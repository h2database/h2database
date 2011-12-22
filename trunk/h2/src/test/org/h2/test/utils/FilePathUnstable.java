/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.h2.store.fs.FileBase;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathWrapper;

/**
 * An unstable file system. It is used to simulate file system problems (for
 * example out of disk space).
 */
public class FilePathUnstable extends FilePathWrapper {

    private static final FilePathUnstable INSTANCE = new FilePathUnstable();

    private static final IOException DISK_FULL = new IOException("Disk full");

    private static int diskFullOffCount;

    /**
     * Register the file system.
     *
     * @return the instance
     */
    public static FilePathUnstable register() {
        FilePath.register(INSTANCE);
        return INSTANCE;
    }

    /**
     * Check if the simulated problem occurred.
     * This call will decrement the countdown.
     *
     * @throws IOException if the simulated power failure occurred
     */
    void checkError() throws IOException {
        if (diskFullOffCount == 0) {
            return;
        }
        if (--diskFullOffCount > 0) {
            return;
        }
        if (diskFullOffCount >= -4) {
            diskFullOffCount--;
            throw DISK_FULL;
        }
    }

    public void createDirectory() {
        super.createDirectory();
    }

    public boolean createFile() {
        return super.createFile();
    }

    public void delete() {
        super.delete();
    }

    public boolean exists() {
        return super.exists();
    }

    public String getName() {
        return super.getName();
    }

    public long lastModified() {
        return super.lastModified();
    }

    public FilePath getParent() {
        return super.getParent();
    }

    public boolean isAbsolute() {
        return super.isAbsolute();
    }

    public boolean isDirectory() {
        return super.isDirectory();
    }

    public boolean canWrite() {
        return super.canWrite();
    }

    public boolean setReadOnly() {
        return super.setReadOnly();
    }

    public long size() {
        return super.size();
    }

    public List<FilePath> newDirectoryStream() {
        return super.newDirectoryStream();
    }

    public FilePath toRealPath() {
        return super.toRealPath();
    }

    public InputStream newInputStream() throws IOException {
        return super.newInputStream();
    }

    public FileChannel open(String mode) throws IOException {
        return new FileUnstable(this, super.open(mode));
    }

    public OutputStream newOutputStream(boolean append) {
        return super.newOutputStream(append);
    }

    public void moveTo(FilePath newName) {
        super.moveTo(newName);
    }

    public FilePath createTempFile(String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        return super.createTempFile(suffix, deleteOnExit, inTempDir);
    }

    public void setDiskFullCount(int count) {
        diskFullOffCount = count;
    }

    public int getDiskFullCount() {
        return diskFullOffCount;
    }

    public String getScheme() {
        return "unstable";
    }

}

/**
 * An file that checks for errors before each write operation.
 */
class FileUnstable extends FileBase {

    private final FilePathUnstable file;
    private final FileChannel channel;

    FileUnstable(FilePathUnstable file, FileChannel channel) {
        this.file = file;
        this.channel = channel;
    }

    public void implCloseChannel() throws IOException {
        channel.close();
    }

    public long position() throws IOException {
        return channel.position();
    }

    public long size() throws IOException {
        return channel.size();
    }

    public int read(ByteBuffer dst) throws IOException {
        return channel.read(dst);
    }

    public FileChannel position(long pos) throws IOException {
        channel.position(pos);
        return this;
    }

    public FileChannel truncate(long newLength) throws IOException {
        checkError();
        channel.truncate(newLength);
        return this;
    }

    public void force(boolean metaData) throws IOException {
        checkError();
        channel.force(metaData);
    }

    public int write(ByteBuffer src) throws IOException {
        checkError();
        return channel.write(src);
    }

    private void checkError() throws IOException {
        file.checkError();
    }

    public synchronized FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return channel.tryLock();
    }

}