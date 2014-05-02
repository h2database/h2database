/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.io.IOException;
import org.h2.store.fs.FileObject;
import org.h2.store.fs.FileSystem;
import org.h2.store.fs.FileSystemWrapper;

/**
 * This file system encrypts the data.
 */
public class FileSystemCrypt extends FileSystemWrapper {

    private static final String PREFIX = "aes:";

    private static final int HEADER_LENGTH = 4096;

    static {
        FileSystem.register(new FileSystemCrypt());
    }

    protected String getPrefix() {
        return PREFIX;
    }

    public long length(String fileName) {
        return super.length(fileName) - HEADER_LENGTH;
    }

    public FileObject openFileObject(String fileName, String mode) {
        return null;
    }

    /**
     * An encrypted file.
     */
    static class FileObjectCrypt implements FileObject {

//        private FileObject

        public void close() throws IOException {
            // TODO Auto-generated method stub

        }

        public long getFilePointer() throws IOException {
            // TODO Auto-generated method stub
            return 0;
        }

        public String getName() {
            // TODO Auto-generated method stub
            return null;
        }

        public long length() throws IOException {
            // TODO Auto-generated method stub
            return 0;
        }

        public void readFully(byte[] b, int off, int len) throws IOException {
            // TODO Auto-generated method stub

        }

        public void releaseLock() {
            // TODO Auto-generated method stub

        }

        public void seek(long pos) throws IOException {
            // TODO Auto-generated method stub

        }

        public void setFileLength(long newLength) throws IOException {
            // TODO Auto-generated method stub

        }

        public void sync() throws IOException {
            // TODO Auto-generated method stub

        }

        public boolean tryLock() {
            // TODO Auto-generated method stub
            return false;
        }

        public void write(byte[] b, int off, int len) throws IOException {
            // TODO Auto-generated method stub

        }

    }

}
