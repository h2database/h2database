/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.h2.constant.SysProperties;
import org.h2.util.IOUtils;

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
        String m = SysProperties.SYNC_METHOD;
        if ("".equals(m)) {
            // do nothing
        } else if ("sync".equals(m)) {
            getFD().sync();
        } else if ("force".equals(m)) {
            getChannel().force(true);
        } else if ("forceFalse".equals(m)) {
            getChannel().force(false);
        } else {
            getFD().sync();
        }
    }

    public void setFileLength(long newLength) throws IOException {
        IOUtils.setLength(this, newLength);
    }

    public String getName() {
        return name;
    }

}
