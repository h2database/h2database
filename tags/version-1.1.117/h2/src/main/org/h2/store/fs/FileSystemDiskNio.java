/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: Jan Kotek
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

/**
 * This file system stores files on disk and uses java.nio to access the files.
 * This class uses FileChannel.
 */
public class FileSystemDiskNio extends FileSystemDisk {

    private static final FileSystemDiskNio INSTANCE = new FileSystemDiskNio();

    public static FileSystemDisk getInstance() {
        return INSTANCE;
    }

    public String createTempFile(String name, String suffix, boolean deleteOnExit, boolean inTempDir)
    throws IOException {
        String file = super.createTempFile(name, suffix, deleteOnExit, inTempDir);
        return getPrefix() + file;
    }

    protected String translateFileName(String fileName) {
        if (fileName.startsWith(getPrefix())) {
            fileName = fileName.substring(getPrefix().length());
        }
        return super.translateFileName(fileName);
    }

    public InputStream openFileInputStream(String fileName) throws IOException {
        return super.openFileInputStream(translateFileName(fileName));
    }

    public String normalize(String fileName) throws SQLException {
        return getPrefix() + super.normalize(fileName);
    }

    public String[] listFiles(String path) throws SQLException {
        String[] list = super.listFiles(path);
        for (int i = 0; list != null && i < list.length; i++) {
            list[i] = getPrefix() + list[i];
        }
        return list;
    }

    public String getParent(String fileName) {
        return getPrefix() + super.getParent(fileName);
    }

    public String getAbsolutePath(String fileName) {
        return getPrefix() + super.getAbsolutePath(fileName);
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        fileName = translateFileName(fileName);
        FileObject f;
        try {
            f = open(fileName, mode);
            trace("openRandomAccessFile", fileName, f);
        } catch (IOException e) {
            freeMemoryAndFinalize();
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
        return FileSystem.PREFIX_NIO;
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
