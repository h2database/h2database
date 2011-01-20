/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: Jan Kotek
 */
package org.h2.store.fs;

import java.io.IOException;
import org.h2.util.IOUtils;

/**
 * This file system stores files on disk and uses java.nio to access the files.
 * This class uses FileChannel.
 */
public class FileSystemDiskNio extends FileSystemWrapper {

    /**
     * The prefix for the file system that uses java.nio.channels.FileChannel.
     */
    static final String PREFIX = "nio:";

    static {
        FileSystem.register(new FileSystemDiskNio());
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        fileName = unwrap(fileName);
        FileObject f;
        try {
            f = open(fileName, mode);
            IOUtils.trace("openFileObject", fileName, f);
        } catch (IOException e) {
            FileSystemDisk.freeMemoryAndFinalize();
            try {
                f = open(fileName, mode);
            } catch (IOException e2) {
                throw e;
            }
        }
        return f;
    }

    /**
     * Get the prefix for this file system.
     *
     * @return the prefix
     */
    protected String getPrefix() {
        return PREFIX;
    }

    /**
     * Try to open a file with this name and mode.
     *
     * @param fileName the file name
     * @param mode the open mode
     * @return the file object
     * @throws IOException if opening fails
     */
    protected FileObject open(String fileName, String mode) throws IOException {
        return new FileObjectDiskChannel(fileName, mode);
    }

}
