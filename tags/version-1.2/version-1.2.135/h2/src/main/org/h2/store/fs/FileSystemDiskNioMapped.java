/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: Jan Kotek
 */
package org.h2.store.fs;

import java.io.IOException;

/**
 * This file system stores files on disk and uses java.nio to access the files.
 * This class used memory mapped files.
 */
public class FileSystemDiskNioMapped extends FileSystemDiskNio {

    private static final String PREFIX = "nioMapped:";

    static {
        FileSystem.register(new FileSystemDiskNioMapped());
    }

    protected String getPrefix() {
        return PREFIX;
    }

    protected FileObject open(String fileName, String mode) throws IOException {
        return new FileObjectDiskMapped(fileName, mode);
    }

}
