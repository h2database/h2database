/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Allows to write to a file object like an output stream.
 */
public class FileObjectOutputStream extends OutputStream {

    private FileObject file;
    private byte[] buffer = new byte[1];

    /**
     * Create a new file object output stream from the file object.
     *
     * @param file the file object
     * @param append true for append mode, false for truncate and overwrite
     */
    public FileObjectOutputStream(FileObject file, boolean append) throws IOException {
        this.file = file;
        if (append) {
            file.seek(file.length());
        } else {
            file.seek(0);
            file.setFileLength(0);
        }
    }

    public void write(int b) throws IOException {
        buffer[0] = (byte) b;
        file.write(buffer, 0, 1);
    }

    public void write(byte[] b) throws IOException {
        file.write(b, 0, b.length);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        file.write(b, off, len);
    }

    public void close() throws IOException {
        file.close();
    }

}
