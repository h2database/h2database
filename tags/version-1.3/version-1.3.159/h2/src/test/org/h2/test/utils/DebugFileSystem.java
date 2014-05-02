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
import org.h2.store.fs.FileObject;
import org.h2.store.fs.FileSystem;
import org.h2.store.fs.FileSystemWrapper;

/**
 * A debugging file system that logs all operations.
 */
public class DebugFileSystem extends FileSystemWrapper {

    /**
     * The prefix used for a debugging file system.
     */
    static final String PREFIX = "debug:";

    private static final DebugFileSystem INSTANCE = new DebugFileSystem();

    private static final IOException POWER_OFF = new IOException("Simulated power failure");

    private int powerOffCount;
    private boolean trace;

    /**
     * Register the file system.
     *
     * @return the instance
     */
    public static DebugFileSystem register() {
        FileSystem.register(INSTANCE);
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

    public boolean canWrite(String fileName) {
        trace(fileName, "canWrite");
        return super.canWrite(fileName);
    }

    public void createDirs(String fileName) {
        trace(fileName, "createDirs");
        super.createDirs(fileName);
    }

    public boolean createNewFile(String fileName) {
        trace(fileName, "createNewFile");
        return super.createNewFile(fileName);
    }

    public String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        trace(prefix, "createTempFile", suffix, deleteOnExit, inTempDir);
        return super.createTempFile(prefix, suffix, deleteOnExit, inTempDir);
    }

    public void delete(String fileName) {
        trace(fileName, "fileName");
        super.delete(fileName);
    }

    public void deleteRecursive(String directory, boolean tryOnly) {
        trace(directory, "deleteRecursive", tryOnly);
        super.deleteRecursive(directory, tryOnly);
    }

    public boolean exists(String fileName) {
        trace(fileName, "exists");
        return super.exists(fileName);
    }

    public boolean fileStartsWith(String fileName, String prefix) {
        trace(fileName, "fileStartsWith", unwrap(prefix));
        return super.fileStartsWith(fileName, prefix);
    }

    public String getFileName(String name) {
        trace(name, "getFileName");
        return super.getFileName(name);
    }

    public long getLastModified(String fileName) {
        trace(fileName, "getLastModified");
        return super.getLastModified(fileName);
    }

    public String getParent(String fileName) {
        trace(fileName, "getParent");
        return super.getParent(fileName);
    }

    public boolean isAbsolute(String fileName) {
        trace(fileName, "isAbsolute");
        return super.isAbsolute(fileName);
    }

    public boolean isDirectory(String fileName) {
        trace(fileName, "isDirectory");
        return super.isDirectory(fileName);
    }

    public boolean isReadOnly(String fileName) {
        trace(fileName, "isReadOnly");
        return super.isReadOnly(fileName);
    }

    public boolean setReadOnly(String fileName) {
        trace(fileName, "setReadOnly");
        return super.setReadOnly(fileName);
    }

    public long length(String fileName) {
        trace(fileName, "length");
        return super.length(fileName);
    }

    public String[] listFiles(String directory) {
        trace(directory, "listFiles");
        return super.listFiles(directory);
    }

    public String getCanonicalPath(String fileName) {
        trace(fileName, "normalize");
        return super.getCanonicalPath(fileName);
    }

    public InputStream openFileInputStream(final String fileName) throws IOException {
        trace(fileName, "openFileInputStream");
        InputStream in = super.openFileInputStream(fileName);
        if (!trace) {
            return in;
        }
        return new FilterInputStream(in) {
            public int read(byte[] b) throws IOException {
                trace(fileName, "in.read(b)");
                return super.read(b);
            }

            public int read(byte[] b, int off, int len) throws IOException {
                trace(fileName, "in.read(b, " + off + ", " + len + ")");
                return super.read(b, off, len);
            }

            public long skip(long n) throws IOException {
                trace(fileName, "in.skip(" + n + ")");
                return super.skip(n);
            }
        };
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        trace(fileName, "openFileObject", mode);
        return new DebugFileObject(this, super.openFileObject(fileName, mode));
    }

    public OutputStream openFileOutputStream(String fileName, boolean append) {
        trace(fileName, "openFileOutputStream", append);
        return super.openFileOutputStream(fileName, append);
    }

    public void rename(String oldName, String newName) {
        trace(oldName, "rename", unwrap(newName));
        super.rename(oldName, newName);
    }

    public boolean tryDelete(String fileName) {
        trace(fileName, "tryDelete");
        return super.tryDelete(fileName);
    }

    public String getPrefix() {
        return PREFIX;
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

}
