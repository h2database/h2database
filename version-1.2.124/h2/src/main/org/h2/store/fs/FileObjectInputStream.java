/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.InputStream;

/**
 * Allows to read from a file object like an input stream.
 */
public class FileObjectInputStream extends InputStream {

    private FileObject file;
    private byte[] buffer = new byte[1];

    /**
     * Create a new file object input stream from the file object.
     *
     * @param file the file object
     */
    public FileObjectInputStream(FileObject file) {
        this.file = file;
    }

    public int read() throws IOException {
        if (file.getFilePointer() >= file.length()) {
            return -1;
        }
        file.readFully(buffer, 0, 1);
        return buffer[0] & 0xff;
    }

    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        if (file.getFilePointer() + len < file.length()) {
            file.readFully(b, off, len);
            return len;
        }
        return super.read(b, off, len);
    }

    public void close() throws IOException {
        file.close();
    }

}
