/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;


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

    public void close() throws IOException {
        sync();
    }

    public long getFilePointer() throws IOException {
        return pos;
    }

    public long length() throws IOException {
        return length;
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        if (pos + len > length) {
            throw new IOException("Can not read past EOF");
        }
        System.arraycopy(data, pos, b, off, len);
        pos += len;
    }

    public void seek(long pos) throws IOException {
        this.pos = (int) pos;
    }

    public void setLength(long newLength) throws IOException {
        this.length = (int) newLength;
        if (length != data.length) {
            byte[] n = new byte[length];
            System.arraycopy(data, 0, n, 0, Math.min(data.length, n.length));
            data = n;
            changed = true;
        }
    }

    public void sync() throws IOException {
        if (changed) {
            db.write(fileName, data, length);
            changed = false;
        }
    }

    public void write(byte[] b, int off, int len) throws IOException {
        if (pos + len > data.length) {
            int newLen = Math.max(data.length * 2, 1024);
            byte[] n = new byte[newLen];
            System.arraycopy(data, 0, n, 0, length);
            data = n;
        }
        System.arraycopy(b, off, data, pos, len);
        pos += len;
        length = Math.max(length, pos);
        changed = true;
    }

}
