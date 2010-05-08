/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
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
 * A file system that records all write operations and can re-play them.
 */
public class RecordingFileSystem extends FileSystem {

    /**
     * The prefix used for a debugging file system.
     */
    public static final String PREFIX = "rec:";

    private static final RecordingFileSystem INSTANCE = new RecordingFileSystem();

    private static Recorder recorder;

    private boolean trace;

    /**
     * Register the file system.
     */
    public static void register() {
        FileSystem.register(INSTANCE);
    }

    /**
     * Set the recorder class.
     *
     * @param recorder the recorder
     */
    public static void setRecorder(Recorder recorder) {
        RecordingFileSystem.recorder = recorder;
    }

    public boolean canWrite(String fileName) {
        fileName = translateFileName(fileName);
        return FileSystem.getInstance(fileName).canWrite(fileName);
    }

    public void copy(String original, String copy) {
        original = translateFileName(original);
        copy = translateFileName(copy);
        log(Recorder.COPY, original + ":" + copy);
        FileSystem.getInstance(original).copy(original, copy);
    }

    public void createDirs(String fileName) {
        fileName = translateFileName(fileName);
        log(Recorder.CREATE_DIRS, fileName);
        FileSystem.getInstance(fileName).createDirs(fileName);
    }

    public boolean createNewFile(String fileName) {
        fileName = translateFileName(fileName);
        log(Recorder.CREATE_NEW_FILE, fileName);
        return FileSystem.getInstance(fileName).createNewFile(fileName);
    }

    public String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        prefix = translateFileName(prefix);
        log(Recorder.CREATE_TEMP_FILE, prefix + ":" + suffix + ":" + deleteOnExit + ":" + inTempDir);
        return PREFIX + FileSystem.getInstance(prefix).createTempFile(prefix, suffix, deleteOnExit, inTempDir);
    }

    public void delete(String fileName) {
        fileName = translateFileName(fileName);
        log(Recorder.DELETE, fileName);
        FileSystem.getInstance(fileName).delete(fileName);
    }

    public void deleteRecursive(String directory, boolean tryOnly) {
        directory = translateFileName(directory);
        log(Recorder.DELETE_RECURSIVE, directory);
        FileSystem.getInstance(directory).deleteRecursive(directory, tryOnly);
    }

    public boolean exists(String fileName) {
        fileName = translateFileName(fileName);
        return FileSystem.getInstance(fileName).exists(fileName);
    }

    public boolean fileStartsWith(String fileName, String prefix) {
        fileName = translateFileName(fileName);
        prefix = translateFileName(prefix);
        return FileSystem.getInstance(fileName).fileStartsWith(fileName, prefix);
    }

    public String getAbsolutePath(String fileName) {
        fileName = translateFileName(fileName);
        return PREFIX + FileSystem.getInstance(fileName).getAbsolutePath(fileName);
    }

    public String getFileName(String name) {
        name = translateFileName(name);
        return FileSystem.getInstance(name).getFileName(name);
    }

    public long getLastModified(String fileName) {
        fileName = translateFileName(fileName);
        return FileSystem.getInstance(fileName).getLastModified(fileName);
    }

    public String getParent(String fileName) {
        fileName = translateFileName(fileName);
        return PREFIX + FileSystem.getInstance(fileName).getParent(fileName);
    }

    public boolean isAbsolute(String fileName) {
        fileName = translateFileName(fileName);
        return FileSystem.getInstance(fileName).isAbsolute(fileName);
    }

    public boolean isDirectory(String fileName) {
        fileName = translateFileName(fileName);
        return FileSystem.getInstance(fileName).isDirectory(fileName);
    }

    public boolean isReadOnly(String fileName) {
        fileName = translateFileName(fileName);
        return FileSystem.getInstance(fileName).isReadOnly(fileName);
    }

    public boolean setReadOnly(String fileName) {
        fileName = translateFileName(fileName);
        return FileSystem.getInstance(fileName).setReadOnly(fileName);
    }

    public long length(String fileName) {
        fileName = translateFileName(fileName);
        return FileSystem.getInstance(fileName).length(fileName);
    }

    public String[] listFiles(String directory) {
        directory = translateFileName(directory);
        String[] list = FileSystem.getInstance(directory).listFiles(directory);
        for (int i = 0; i < list.length; i++) {
            list[i] = PREFIX + list[i];
        }
        return list;
    }

    public String normalize(String fileName) {
        fileName = translateFileName(fileName);
        return PREFIX + FileSystem.getInstance(fileName).normalize(fileName);
    }

    public InputStream openFileInputStream(String fileName) throws IOException {
        fileName = translateFileName(fileName);
        return FileSystem.getInstance(fileName).openFileInputStream(fileName);
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        fileName = translateFileName(fileName);
        return new RecordingFileObject(this, FileSystem.getInstance(fileName).openFileObject(fileName, mode));
    }

    public OutputStream openFileOutputStream(String fileName, boolean append) {
        fileName = translateFileName(fileName);
        log(Recorder.OPEN_OUTPUT_STREAM, fileName);
        return FileSystem.getInstance(fileName).openFileOutputStream(fileName, append);
    }

    public void rename(String oldName, String newName) {
        oldName = translateFileName(oldName);
        newName = translateFileName(newName);
        log(Recorder.RENAME, oldName + ":" + newName);
        FileSystem.getInstance(oldName).rename(oldName, newName);
    }

    public boolean tryDelete(String fileName) {
        fileName = translateFileName(fileName);
        log(Recorder.TRY_DELETE, fileName);
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

    public boolean isTrace() {
        return trace;
    }

    public void setTrace(boolean trace) {
        this.trace = trace;
    }

    /**
     * Log the operation.
     *
     * @param op the operation
     * @param fileName the file name
     */
    void log(int op, String fileName) {
        log(op, fileName, null, 0);
    }

    /**
     * Log the operation.
     *
     * @param op the operation
     * @param fileName the file name
     * @param data the data or null
     * @param x the value or 0
     */
    void log(int op, String fileName, byte[] data, long x) {
        if (recorder != null) {
            recorder.log(op, fileName, data, x);
        }
    }

}
