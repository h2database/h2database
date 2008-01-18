/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.h2.util.FileUtils;

/**
 * This class is extends a java.io.RandomAccessFile.
 */
public class FileObjectDisk extends RandomAccessFile implements FileObject {

    public FileObjectDisk(String fileName, String mode) throws FileNotFoundException {
        super(fileName, mode);
    }

    public void sync() throws IOException {
        getFD().sync();
    }

    public void setFileLength(long newLength) throws IOException {
        FileUtils.setLength(this, newLength);
    }

}
