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
import org.h2.message.DbException;
import org.h2.store.fs.FileObject;
import org.h2.store.fs.FileSystem;

/**
 * A debugging file system that logs all operations.
 */
public class DebugFileSystem extends FileSystem {

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
        fileName = translateFileName(fileName);
        trace(fileName, "canWrite");
        return FileSystem.getInstance(fileName).canWrite(fileName);
    }

    public void copy(String original, String copy) {
        original = translateFileName(original);
        copy = translateFileName(copy);
        trace(original, "copy", copy);
        FileSystem.getInstance(original).copy(original, copy);
    }

    public void createDirs(String fileName) {
        fileName = translateFileName(fileName);
        trace(fileName, "createDirs");
        FileSystem.getInstance(fileName).createDirs(fileName);
    }

    public boolean createNewFile(String fileName) {
        fileName = translateFileName(fileName);
        trace(fileName, "createNewFile");
        return FileSystem.getInstance(fileName).createNewFile(fileName);
    }

    public String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        prefix = translateFileName(prefix);
        trace(prefix, "createTempFile", suffix, deleteOnExit, inTempDir);
        return PREFIX + FileSystem.getInstance(prefix).createTempFile(prefix, suffix, deleteOnExit, inTempDir);
    }

    public void delete(String fileName) {
        fileName = translateFileName(fileName);
        trace(fileName, "fileName");
        FileSystem.getInstance(fileName).delete(fileName);
    }

    public void deleteRecursive(String directory, boolean tryOnly) {
        directory = translateFileName(directory);
        trace(directory, "deleteRecursive");
        FileSystem.getInstance(directory).deleteRecursive(directory, tryOnly);
    }

    public boolean exists(String fileName) {
        fileName = translateFileName(fileName);
        trace(fileName, "exists");
        return FileSystem.getInstance(fileName).exists(fileName);
    }

    public boolean fileStartsWith(String fileName, String prefix) {
        fileName = translateFileName(fileName);
        prefix = translateFileName(prefix);
        trace(fileName, "fileStartsWith", prefix);
        return FileSystem.getInstance(fileName).fileStartsWith(fileName, prefix);
    }

    public String getAbsolutePath(String fileName) {
        fileName = translateFileName(fileName);
        trace(fileName, "getAbsolutePath");
        return PREFIX + FileSystem.getInstance(fileName).getAbsolutePath(fileName);
    }

    public String getFileName(String name) {
        name = translateFileName(name);
        trace(name, "getFileName");
        return FileSystem.getInstance(name).getFileName(name);
    }

    public long getLastModified(String fileName) {
        fileName = translateFileName(fileName);
        trace(fileName, "getLastModified");
        return FileSystem.getInstance(fileName).getLastModified(fileName);
    }

    public String getParent(String fileName) {
        fileName = translateFileName(fileName);
        trace(fileName, "getParent");
        return PREFIX + FileSystem.getInstance(fileName).getParent(fileName);
    }

    public boolean isAbsolute(String fileName) {
        fileName = translateFileName(fileName);
        trace(fileName, "isAbsolute");
        return FileSystem.getInstance(fileName).isAbsolute(fileName);
    }

    public boolean isDirectory(String fileName) {
        fileName = translateFileName(fileName);
        trace(fileName, "isDirectory");
        return FileSystem.getInstance(fileName).isDirectory(fileName);
    }

    public boolean isReadOnly(String fileName) {
        fileName = translateFileName(fileName);
        trace(fileName, "isReadOnly");
        return FileSystem.getInstance(fileName).isReadOnly(fileName);
    }

    public boolean setReadOnly(String fileName) {
        fileName = translateFileName(fileName);
        trace(fileName, "setReadOnly");
        return FileSystem.getInstance(fileName).setReadOnly(fileName);
    }

    public long length(String fileName) {
        fileName = translateFileName(fileName);
        trace(fileName, "length");
        return FileSystem.getInstance(fileName).length(fileName);
    }

    public String[] listFiles(String directory) {
        directory = translateFileName(directory);
        trace(directory, "listFiles");
        String[] list = FileSystem.getInstance(directory).listFiles(directory);
        for (int i = 0; i < list.length; i++) {
            list[i] = PREFIX + list[i];
        }
        return list;
    }

    public String normalize(String fileName) {
        fileName = translateFileName(fileName);
        trace(fileName, "normalize");
        return PREFIX + FileSystem.getInstance(fileName).normalize(fileName);
    }

    public InputStream openFileInputStream(String fileName) throws IOException {
        fileName = translateFileName(fileName);
        trace(fileName, "openFileInputStream");
        return FileSystem.getInstance(fileName).openFileInputStream(fileName);
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        fileName = translateFileName(fileName);
        trace(fileName, "openFileObject", mode);
        return new DebugFileObject(this, FileSystem.getInstance(fileName).openFileObject(fileName, mode));
    }

    public OutputStream openFileOutputStream(String fileName, boolean append) {
        fileName = translateFileName(fileName);
        trace(fileName, "openFileOutputStream", append);
        return FileSystem.getInstance(fileName).openFileOutputStream(fileName, append);
    }

    public void rename(String oldName, String newName) {
        oldName = translateFileName(oldName);
        newName = translateFileName(newName);
        trace(oldName, "rename", newName);
        FileSystem.getInstance(oldName).rename(oldName, newName);
    }

    public boolean tryDelete(String fileName) {
        fileName = translateFileName(fileName);
        trace(fileName, "tryDelete");
        return FileSystem.getInstance(fileName).tryDelete(fileName);
    }

    protected boolean accepts(String fileName) {
        return fileName.startsWith(PREFIX);
    }

    private String translateFileName(String fileName) {
        if (!fileName.startsWith(PREFIX)) {
            DbException.throwInternalError(fileName + " doesn't start with " + PREFIX);
        }
        return fileName.substring(PREFIX.length());
    }

    /**
     * Print a debug message.
     * @param fileName the file name
     * @param method the method name
     * @param params parameters if any
     */
    void trace(String fileName, String method, Object... params) {
        if (trace) {
            StringBuilder buff = new StringBuilder("    ");
            buff.append(fileName).append(' ').append(method);
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
