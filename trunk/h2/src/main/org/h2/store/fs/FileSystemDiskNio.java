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
        return FileSystem.PREFIX_NIO + file;
    }

    protected String translateFileName(String fileName) {
        if (fileName.startsWith(FileSystem.PREFIX_NIO)) {
            fileName = fileName.substring(FileSystem.PREFIX_NIO.length());
        }
        return super.translateFileName(fileName);
    }

    public InputStream openFileInputStream(String fileName) throws IOException {
        return super.openFileInputStream(translateFileName(fileName));
    }

    public String normalize(String fileName) throws SQLException {
        return FileSystem.PREFIX_NIO + super.normalize(fileName);
    }

    public String[] listFiles(String path) throws SQLException {
        String[] list = super.listFiles(path);
        for (int i = 0; list != null && i < list.length; i++) {
            list[i] = FileSystem.PREFIX_NIO + list[i];
        }
        return list;
    }

    public String getParent(String fileName) {
        return FileSystem.PREFIX_NIO + super.getParent(fileName);
    }

    public String getAbsolutePath(String fileName) {
        return FileSystem.PREFIX_NIO + super.getAbsolutePath(fileName);
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        fileName = translateFileName(fileName);
        FileObject f;
        try {
            f = new FileObjectDiskMapped(fileName, mode);
            trace("openRandomAccessFile", fileName, f);
        } catch (IOException e) {
            freeMemoryAndFinalize();
            try {
                f = new FileObjectDiskMapped(fileName, mode);
            } catch (IOException e2) {
                e2.initCause(e);
                throw e2;
            }
        }
        return f;
    }

    protected FileObject open(String fileName, String mode) throws IOException {
        return new FileObjectDiskChannel(fileName, mode);
    }

}
