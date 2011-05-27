/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.h2.util.FileUtils;

/**
 * This class is wraps a RandomAccessFile.
 */
public class FileObjectDisk implements FileObject {
    private RandomAccessFile file;
    
    FileObjectDisk(RandomAccessFile file) {
        this.file = file;
    }

    public long length() throws IOException {
        return file.length();
    }

    public void close() throws IOException {
        file.close();
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        file.readFully(b, off, len);
    }

    public void seek(long pos) throws IOException {
        file.seek(pos);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        file.write(b, off, len);
    }

    public long getFilePointer() throws IOException {
        return file.getFilePointer();
    }
    
    public void sync() throws IOException {
        file.getFD().sync();
    }
    
    public void setLength(long newLength) throws IOException {
        FileUtils.setLength(file, newLength);
    }

}
