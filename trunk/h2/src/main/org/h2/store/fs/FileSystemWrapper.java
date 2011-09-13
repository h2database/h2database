/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.h2.message.DbException;

/**
 * The base class for wrapping / delegating file systems such as
 * FileSystemSplit
 */
public abstract class FileSystemWrapper extends FileSystem {

    /**
     * Get the prefix for this file system.
     *
     * @return the prefix
     */
    protected abstract String getPrefix();

    public boolean canWrite(String fileName) {
        return FileUtils.canWrite(unwrap(fileName));
    }

    public boolean setReadOnly(String fileName) {
        return FileUtils.setReadOnly(unwrap(fileName));
    }

    public void createDirectory(String directoryName) {
        FileUtils.createDirectory(unwrap(directoryName));
    }

    public boolean createFile(String fileName) {
        return FileUtils.createFile(unwrap(fileName));
    }

    public String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        return wrap(FileUtils.createTempFile(unwrap(prefix), suffix, deleteOnExit, inTempDir));
    }

    public void delete(String path) {
        FileUtils.delete(unwrap(path));
    }

    public boolean exists(String fileName) {
        return FileUtils.exists(unwrap(fileName));
    }

    public boolean fileStartsWith(String fileName, String prefix) {
        return FileUtils.fileStartsWith(unwrap(fileName), unwrap(prefix));
    }

    public String getName(String path) {
        return FileUtils.getName(unwrap(path));
    }

    public long lastModified(String fileName) {
        return FileUtils.lastModified(unwrap(fileName));
    }

    public String getParent(String fileName) {
        return wrap(FileUtils.getParent(unwrap(fileName)));
    }

    public boolean isAbsolute(String fileName) {
        return FileUtils.isAbsolute(unwrap(fileName));
    }

    public boolean isDirectory(String fileName) {
        return FileUtils.isDirectory(unwrap(fileName));
    }

    public boolean isReadOnly(String fileName) {
        return FileUtils.isReadOnly(unwrap(fileName));
    }

    public long size(String fileName) {
        return FileUtils.size(unwrap(fileName));
    }

    public String[] listFiles(String directory) {
        String[] array = FileUtils.listFiles(unwrap(directory));
        for (int i = 0; i < array.length; i++) {
            array[i] = wrap(array[i]);
        }
        return array;
    }

    public String getCanonicalPath(String fileName) {
        return wrap(FileUtils.getCanonicalPath(unwrap(fileName)));
    }

    public InputStream newInputStream(String fileName) throws IOException {
        return FileUtils.newInputStream(unwrap(fileName));
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        return FileUtils.openFileObject(unwrap(fileName), mode);
    }

    public OutputStream newOutputStream(String fileName, boolean append) {
        return FileUtils.newOutputStream(unwrap(fileName), append);
    }

    public void moveTo(String oldName, String newName) {
        FileUtils.moveTo(unwrap(oldName), unwrap(newName));
    }

    protected boolean accepts(String fileName) {
        return fileName.startsWith(getPrefix());
    }

    private String wrap(String fileName) {
        return getPrefix() + fileName;
    }

    public String unwrap(String fileName) {
        String prefix = getPrefix();
        if (!fileName.startsWith(prefix)) {
            DbException.throwInternalError(fileName + " doesn't start with " + prefix);
        }
        return fileName.substring(prefix.length());
    }

}
