/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.io.IOException;
import org.h2.store.fs.FileObject;

/**
 * A file object that records all write operations and can re-play them.
 */
public class RecordingFileObject implements FileObject {

    private final RecordingFileSystem fs;
    private final FileObject file;
    private final String name;

    RecordingFileObject(RecordingFileSystem fs, FileObject file) {
        this.fs = fs;
        this.file = file;
        this.name = file.getName();
    }

    public void close() throws IOException {
        file.close();
    }

    public long getFilePointer() throws IOException {
        return file.getFilePointer();
    }

    public String getName() {
        return RecordingFileSystem.PREFIX + name;
    }

    public long length() throws IOException {
        return file.length();
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        file.readFully(b, off, len);
    }

    public void seek(long pos) throws IOException {
        file.seek(pos);
    }

    public void setFileLength(long newLength) throws IOException {
        fs.log(Recorder.SET_LENGTH, name, null, newLength);
        file.setFileLength(newLength);
    }

    public void sync() throws IOException {
        file.sync();
    }

    public void write(byte[] b, int off, int len) throws IOException {
        byte[] buff = b;
        if (off != 0 || len != b.length) {
            buff = new byte[len];
            System.arraycopy(b, off, buff, 0, len);
        }
        file.write(b, off, len);
        fs.log(Recorder.WRITE, name, buff, file.getFilePointer());
    }

}
