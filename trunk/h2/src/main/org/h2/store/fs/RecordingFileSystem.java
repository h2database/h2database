/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A file system that records all write operations and can re-play them.
 */
public class RecordingFileSystem extends FileSystemWrapper {

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

    public void createDirectories(String directoryName) {
        log(Recorder.CREATE_DIRECTORY, unwrap(directoryName));
        super.createDirectory(directoryName);
    }

    public boolean createNewFile(String fileName) {
        log(Recorder.CREATE_NEW_FILE, unwrap(fileName));
        return super.createNewFile(fileName);
    }

    public String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        log(Recorder.CREATE_TEMP_FILE, unwrap(prefix) + ":" + suffix + ":" + deleteOnExit + ":" + inTempDir);
        return super.createTempFile(prefix, suffix, deleteOnExit, inTempDir);
    }

    public void delete(String fileName) {
        log(Recorder.DELETE, unwrap(fileName));
        super.delete(fileName);
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        return new RecordingFileObject(this, super.openFileObject(fileName, mode));
    }

    public OutputStream openFileOutputStream(String fileName, boolean append) {
        log(Recorder.OPEN_OUTPUT_STREAM, unwrap(fileName));
        return super.openFileOutputStream(fileName, append);
    }

    public void rename(String oldName, String newName) {
        log(Recorder.RENAME, unwrap(oldName) + ":" + unwrap(newName));
        super.rename(oldName, newName);
    }

    public boolean tryDelete(String fileName) {
        log(Recorder.TRY_DELETE, unwrap(fileName));
        return super.tryDelete(fileName);
    }

    public String getPrefix() {
        return PREFIX;
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
