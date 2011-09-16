/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.io.IOException;
import java.nio.channels.FileLock;
import org.h2.store.fs.FileObject;

/**
 * A debugging file that logs all operations.
 */
public class FileDebug implements FileObject {

    private final FilePathDebug fs;
    private final FileObject file;
    private final String name;

    FileDebug(FilePathDebug fs, FileObject file, String name) {
        this.fs = fs;
        this.file = file;
        this.name = fs.getScheme() + ":" + name;
    }

    public void close() throws IOException {
        debug("close");
        file.close();
    }

    public long position() throws IOException {
        debug("getFilePointer");
        return file.position();
    }

    public long size() throws IOException {
        debug("length");
        return file.size();
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        debug("readFully", file.position(), off, len);
        file.readFully(b, off, len);
    }

    public void position(long pos) throws IOException {
        debug("seek", pos);
        file.position(pos);
    }

    public void truncate(long newLength) throws IOException {
        checkPowerOff();
        debug("truncate", newLength);
        file.truncate(newLength);
    }

    public void force(boolean metaData) throws IOException {
        debug("force");
        file.force(metaData);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        checkPowerOff();
        debug("write", file.position(), off, len);
        file.write(b, off, len);
    }

    private void debug(String method, Object... params) {
        fs.trace(name, method, params);
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

    public FileLock tryLock() throws IOException {
        debug("tryLock");
        return file.tryLock();
    }

}