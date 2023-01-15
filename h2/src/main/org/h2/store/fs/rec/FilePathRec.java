/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs.rec;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathWrapper;
import org.h2.store.fs.Recorder;

/**
 * A file system that records all write operations and can re-play them.
 */
public class FilePathRec extends FilePathWrapper {

    private static final FilePathRec INSTANCE = new FilePathRec();

    private static Recorder recorder;

    private boolean trace;

    /**
     * Register the file system.
     */
    public static void register() {
        FilePath.register(INSTANCE);
    }

    /**
     * Set the recorder class.
     *
     * @param recorder the recorder
     */
    public static void setRecorder(Recorder recorder) {
        FilePathRec.recorder = recorder;
    }

    @Override
    public boolean createFile() {
        log(Recorder.CREATE_NEW_FILE, name);
        return super.createFile();
    }

    @Override
    public FilePath createTempFile(String suffix, boolean inTempDir) throws IOException {
        log(Recorder.CREATE_TEMP_FILE, unwrap(name) + ":" + suffix + ":" + inTempDir);
        return super.createTempFile(suffix, inTempDir);
    }

    @Override
    public void delete() {
        log(Recorder.DELETE, name);
        super.delete();
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileRec(this, super.open(mode), name);
    }

    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        log(Recorder.OPEN_OUTPUT_STREAM, name);
        return super.newOutputStream(append);
    }

    @Override
    public void moveTo(FilePath newPath, boolean atomicReplace) {
        log(Recorder.RENAME, unwrap(name) + ":" + unwrap(newPath.name));
        super.moveTo(newPath, atomicReplace);
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
     * @param fileName the file name(s)
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

    /**
     * Get the prefix for this file system.
     *
     * @return the prefix
     */
    @Override
    public String getScheme() {
        return "rec";
    }

}
