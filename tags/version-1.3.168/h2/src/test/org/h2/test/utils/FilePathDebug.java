/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.io.FilterInputStream;
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
 * A debugging file system that logs all operations.
 */
public class FilePathDebug extends FilePathWrapper {

    private static final FilePathDebug INSTANCE = new FilePathDebug();

    private static final IOException POWER_OFF = new IOException("Simulated power failure");

    private int powerOffCount;
    private boolean trace;

    /**
     * Register the file system.
     *
     * @return the instance
     */
    public static FilePathDebug register() {
        FilePath.register(INSTANCE);
        return INSTANCE;
    }

    /**
     * Check if the simulated power failure occurred.
     * This call will decrement the countdown.
     *
     * @throws IOException if the simulated power failure occurred
     */
    void checkPowerOff() throws IOException {
        if (powerOffCount == 0) {
            return;
        }
        if (powerOffCount > 1) {
            powerOffCount--;
            return;
        }
        powerOffCount = -1;
        // throw new IOException("Simulated power failure");
        throw POWER_OFF;
    }

    public void createDirectory() {
        trace(name, "createDirectory");
        super.createDirectory();
    }

    public boolean createFile() {
        trace(name, "createFile");
        return super.createFile();
    }

    public void delete() {
        trace(name, "fileName");
        super.delete();
    }

    public boolean exists() {
        trace(name, "exists");
        return super.exists();
    }

    public String getName() {
        trace(name, "getName");
        return super.getName();
    }

    public long lastModified() {
        trace(name, "lastModified");
        return super.lastModified();
    }

    public FilePath getParent() {
        trace(name, "getParent");
        return super.getParent();
    }

    public boolean isAbsolute() {
        trace(name, "isAbsolute");
        return super.isAbsolute();
    }

    public boolean isDirectory() {
        trace(name, "isDirectory");
        return super.isDirectory();
    }

    public boolean canWrite() {
        trace(name, "canWrite");
        return super.canWrite();
    }

    public boolean setReadOnly() {
        trace(name, "setReadOnly");
        return super.setReadOnly();
    }

    public long size() {
        trace(name, "size");
        return super.size();
    }

    public List<FilePath> newDirectoryStream() {
        trace(name, "newDirectoryStream");
        return super.newDirectoryStream();
    }

    public FilePath toRealPath() {
        trace(name, "toRealPath");
        return super.toRealPath();
    }

    public InputStream newInputStream() throws IOException {
        trace(name, "newInputStream");
        InputStream in = super.newInputStream();
        if (!trace) {
            return in;
        }
        final String fileName = name;
        return new FilterInputStream(in) {
            public int read(byte[] b) throws IOException {
                trace(fileName, "in.read(b)");
                return super.read(b);
            }

            public int read(byte[] b, int off, int len) throws IOException {
                trace(fileName, "in.read(b)", "in.read(b, " + off + ", " + len + ")");
                return super.read(b, off, len);
            }

            public long skip(long n) throws IOException {
                trace(fileName, "in.read(b)", "in.skip(" + n + ")");
                return super.skip(n);
            }
        };
    }

    public FileChannel open(String mode) throws IOException {
        trace(name, "open", mode);
        return new FileDebug(this, super.open(mode), name);
    }

    public OutputStream newOutputStream(boolean append) {
        trace(name, "newOutputStream", append);
        return super.newOutputStream(append);
    }

    public void moveTo(FilePath newName) {
        trace(name, "moveTo", unwrap(((FilePathDebug) newName).name));
        super.moveTo(newName);
    }

    public FilePath createTempFile(String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        trace(name, "createTempFile", suffix, deleteOnExit, inTempDir);
        return super.createTempFile(suffix, deleteOnExit, inTempDir);
    }

    /**
     * Print a debug message.
     *
     * @param fileName the (wrapped) file name
     * @param method the method name
     * @param params parameters if any
     */
    void trace(String fileName, String method, Object... params) {
        if (trace) {
            StringBuilder buff = new StringBuilder("    ");
            buff.append(unwrap(fileName)).append(' ').append(method);
            for (Object s : params) {
                buff.append(' ').append(s);
            }
            System.out.println(buff);
        }
    }

    public void setPowerOffCount(int count) {
        this.powerOffCount = count;
    }

    public int getPowerOffCount() {
        return powerOffCount;
    }

    public boolean isTrace() {
        return trace;
    }

    public void setTrace(boolean trace) {
        this.trace = trace;
    }

    public String getScheme() {
        return "debug";
    }

}

/**
 * A debugging file that logs all operations.
 */
class FileDebug extends FileBase {

    private final FilePathDebug debug;
    private final FileChannel channel;
    private final String name;

    FileDebug(FilePathDebug debug, FileChannel channel, String name) {
        this.debug = debug;
        this.channel = channel;
        this.name = debug.getScheme() + ":" + name;
    }

    public void implCloseChannel() throws IOException {
        debug("close");
        channel.close();
    }

    public long position() throws IOException {
        debug("getFilePointer");
        return channel.position();
    }

    public long size() throws IOException {
        debug("length");
        return channel.size();
    }

    public int read(ByteBuffer dst) throws IOException {
        debug("read", channel.position(), dst.position(), dst.remaining());
        return channel.read(dst);
    }

    public FileChannel position(long pos) throws IOException {
        debug("seek", pos);
        channel.position(pos);
        return this;
    }

    public FileChannel truncate(long newLength) throws IOException {
        checkPowerOff();
        debug("truncate", newLength);
        channel.truncate(newLength);
        return this;
    }

    public void force(boolean metaData) throws IOException {
        debug("force");
        channel.force(metaData);
    }

    public int write(ByteBuffer src) throws IOException {
        checkPowerOff();
        debug("write", channel.position(), src.position(), src.remaining());
        return channel.write(src);
    }

    private void debug(String method, Object... params) {
        debug.trace(name, method, params);
    }

    private void checkPowerOff() throws IOException {
        try {
            debug.checkPowerOff();
        } catch (IOException e) {
            try {
                channel.close();
            } catch (IOException e2) {
                // ignore
            }
            throw e;
        }
    }

    public synchronized FileLock tryLock(long position, long size, boolean shared) throws IOException {
        debug("tryLock");
        return channel.tryLock();
    }

}
