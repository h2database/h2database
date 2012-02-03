/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;

/**
 * This class represents an in-memory file.
 */
public class FileObjectMemory implements FileObject {

    private final FileObjectMemoryData data;
    private long pos;

    FileObjectMemory(FileObjectMemoryData data) {
        this.data = data;
    }

    public long length() {
        return data.length();
    }

    public void setFileLength(long newLength) throws IOException {
        data.touch();
        if (newLength < length()) {
            pos = Math.min(pos, newLength);
        }
        data.setFileLength(newLength);
    }

    public void seek(long newPos) {
        this.pos = (int) newPos;
    }

    public void write(byte[] b, int off, int len) throws IOException {
        data.touch();
        pos = data.readWrite(pos, b, off, len, true);
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        pos = data.readWrite(pos, b, off, len, false);
    }

    public long getFilePointer() {
        return pos;
    }

    public void close() {
        pos = 0;
    }

    public void sync() {
        // do nothing
    }

    public void setName(String name) {
        data.setName(name);
    }

    public String getName() {
        return data.getName();
    }

    public long getLastModified() {
        return data.getLastModified();
    }

}
