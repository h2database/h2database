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
 * This class represents an in-memory file.
 */
public class FileObjectMem implements FileObject {

    private final FileObjectMemData data;
    private final boolean readOnly;
    private long pos;

    FileObjectMem(FileObjectMemData data, boolean readOnly) {
        this.data = data;
        this.readOnly = readOnly;
    }

    public long size() {
        return data.length();
    }

    public void truncate(long newLength) throws IOException {
        if (newLength >= size()) {
            return;
        }
        data.touch(readOnly);
        pos = Math.min(pos, newLength);
        data.truncate(newLength);
    }

    public void position(long newPos) {
        this.pos = (int) newPos;
    }

    public void write(byte[] b, int off, int len) throws IOException {
        data.touch(readOnly);
        pos = data.readWrite(pos, b, off, len, true);
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        pos = data.readWrite(pos, b, off, len, false);
    }

    public long position() {
        return pos;
    }

    public void close() {
        pos = 0;
    }

    public void sync() {
        // do nothing
    }

    public FileLock tryLock() {
        return null;
    }

}
