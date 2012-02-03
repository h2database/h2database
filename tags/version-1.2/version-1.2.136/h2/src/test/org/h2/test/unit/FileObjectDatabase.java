/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.EOFException;
import java.io.IOException;

import org.h2.store.fs.FileObject;
import org.h2.util.Utils;

/**
 * In this file system, files are kept fully in memory until stored.
 */
public class FileObjectDatabase implements FileObject {

    private FileSystemDatabase db;
    private String fileName;
    private byte[] data;
    private int pos, length;
    private boolean changed;

    FileObjectDatabase(FileSystemDatabase db, String fileName, byte[] data, boolean changed) {
        this.db = db;
        this.fileName = fileName;
        this.data = data;
        this.length = data.length;
        this.changed = changed;
    }

    public void close() {
        sync();
    }

    public long getFilePointer() {
        return pos;
    }

    public long length() {
        return length;
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        if (pos + len > length) {
            throw new EOFException();
        }
        System.arraycopy(data, pos, b, off, len);
        pos += len;
    }

    public void seek(long newPos) {
        this.pos = (int) newPos;
    }

    public void setFileLength(long newLength) {
        this.length = (int) newLength;
        if (length != data.length) {
            byte[] n = Utils.newBytes(length);
            System.arraycopy(data, 0, n, 0, Math.min(data.length, n.length));
            data = n;
            changed = true;
        }
        pos = Math.min(pos, length);
    }

    public void sync() {
        if (changed) {
            db.write(fileName, data, length);
            changed = false;
        }
    }

    public void write(byte[] b, int off, int len) {
        if (pos + len > data.length) {
            int newLen = Math.max(data.length * 2, pos + len);
            byte[] n = Utils.newBytes(newLen);
            System.arraycopy(data, 0, n, 0, length);
            data = n;
        }
        System.arraycopy(b, off, data, pos, len);
        pos += len;
        length = Math.max(length, pos);
        changed = true;
    }

    public String getName() {
        return fileName;
    }

}
