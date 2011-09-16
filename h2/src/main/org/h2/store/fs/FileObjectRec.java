/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.nio.channels.FileLock;

/**
 * A file object that records all write operations and can re-play them.
 */
public class FileObjectRec implements FileObject {

    private final FilePathRec fs;
    private final FileObject file;
    private final String name;

    FileObjectRec(FilePathRec fs, FileObject file, String fileName) {
        this.fs = fs;
        this.file = file;
        this.name = fileName;
    }

    public void close() throws IOException {
        file.close();
    }

    public long position() throws IOException {
        return file.position();
    }

    public long size() throws IOException {
        return file.size();
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        file.readFully(b, off, len);
    }

    public void position(long pos) throws IOException {
        file.position(pos);
    }

    public void truncate(long newLength) throws IOException {
        fs.log(Recorder.TRUNCATE, name, null, newLength);
        file.truncate(newLength);
    }

    public void force(boolean metaData) throws IOException {
        file.force(metaData);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        byte[] buff = b;
        if (off != 0 || len != b.length) {
            buff = new byte[len];
            System.arraycopy(b, off, buff, 0, len);
        }
        file.write(b, off, len);
        fs.log(Recorder.WRITE, name, buff, file.position());
    }

    public FileLock tryLock() throws IOException {
        return file.tryLock();
    }

}