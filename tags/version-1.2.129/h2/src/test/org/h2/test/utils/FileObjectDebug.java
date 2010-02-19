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
 * A debugging file that logs all operations.
 */
public class FileObjectDebug implements FileObject {

    private final FileSystemDebug fs;
    private final FileObject file;
    private final String name;

    FileObjectDebug(FileSystemDebug fs, FileObject file) {
        this.fs = fs;
        this.file = file;
        this.name = file.getName();
    }

    public void close() throws IOException {
        debug("close");
        file.close();
    }

    public long getFilePointer() throws IOException {
        debug("getFilePointer");
        return file.getFilePointer();
    }

    public String getName() {
        debug("getName");
        return FileSystemDebug.PREFIX + file.getName();
    }

    public long length() throws IOException {
        debug("length");
        return file.length();
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        debug("readFully", off, len);
        file.readFully(b, off, len);
    }

    public void seek(long pos) throws IOException {
        debug("seek", pos);
        file.seek(pos);
    }

    public void setFileLength(long newLength) throws IOException {
        checkPowerOff();
        debug("setFileLength", newLength);
        file.setFileLength(newLength);
    }

    public void sync() throws IOException {
        debug("sync");
        file.sync();
    }

    public void write(byte[] b, int off, int len) throws IOException {
        checkPowerOff();
        debug("write", off, len);
        file.write(b, off, len);
    }

    private void debug(String method, Object... params) {
        fs.trace(method, name, params);
    }

    private void checkPowerOff() throws IOException {
        try {
            fs.checkPowerOff();
        } catch (IOException e) {
            try {
                file.close();
            } catch (IOException e2) {
                // ignore
            }
            throw e;
        }
    }
}
