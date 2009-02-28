/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
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

    private final String name;

    FileObjectDisk(String fileName, String mode) throws FileNotFoundException {
        super(fileName, mode);
        this.name = fileName;
    }

    public void sync() throws IOException {
        getFD().sync();
    }

    public void setFileLength(long newLength) throws IOException {
        FileUtils.setLength(this, newLength);
    }

    public String getName() {
        return name;
    }

}
