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
import org.h2.util.IOUtils;

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

    /**
     * Wrap the file object if required.
     *
     * @param o the file object
     * @return the wrapped object
     */
    protected FileObject wrap(FileObject o) {
        return null;
    }

    public boolean canWrite(String fileName) {
        return IOUtils.canWrite(unwrap(fileName));
    }

    public boolean setReadOnly(String fileName) {
        return IOUtils.setReadOnly(unwrap(fileName));
    }

    public void copy(String source, String target) {
        IOUtils.copy(unwrap(source), unwrap(target));
    }

    public void createDirs(String fileName) {
        IOUtils.createDirs(unwrap(fileName));
    }

    public boolean createNewFile(String fileName) {
        return IOUtils.createNewFile(unwrap(fileName));
    }

    public String createTempFile(String prefix, String suffix, boolean deleteOnExit, boolean inTempDir)
            throws IOException {
        return wrap(IOUtils.createTempFile(unwrap(prefix), suffix, deleteOnExit, inTempDir));
    }

    public void delete(String fileName) {
        IOUtils.delete(unwrap(fileName));
    }

    public void deleteRecursive(String directory, boolean tryOnly) {
        IOUtils.deleteRecursive(unwrap(directory), tryOnly);
    }

    public boolean exists(String fileName) {
        return IOUtils.exists(unwrap(fileName));
    }

    public boolean fileStartsWith(String fileName, String prefix) {
        return IOUtils.fileStartsWith(unwrap(fileName), unwrap(prefix));
    }

    public String getAbsolutePath(String fileName) {
        return wrap(IOUtils.getAbsolutePath(unwrap(fileName)));
    }

    public String getFileName(String name) {
        return IOUtils.getFileName(unwrap(name));
    }

    public long getLastModified(String fileName) {
        return IOUtils.getLastModified(unwrap(fileName));
    }

    public String getParent(String fileName) {
        return wrap(IOUtils.getParent(unwrap(fileName)));
    }

    public boolean isAbsolute(String fileName) {
        return IOUtils.isAbsolute(unwrap(fileName));
    }

    public boolean isDirectory(String fileName) {
        return IOUtils.isDirectory(unwrap(fileName));
    }

    public boolean isReadOnly(String fileName) {
        return IOUtils.isReadOnly(unwrap(fileName));
    }

    public long length(String fileName) {
        return IOUtils.length(unwrap(fileName));
    }

    public String[] listFiles(String directory) {
        String[] array = IOUtils.listFiles(unwrap(directory));
        for (int i = 0; i < array.length; i++) {
            array[i] = wrap(array[i]);
        }
        return array;
    }

    public String normalize(String fileName) {
        return wrap(IOUtils.normalize(unwrap(fileName)));
    }

    public InputStream openFileInputStream(String fileName) throws IOException {
        return IOUtils.openFileInputStream(unwrap(fileName));
    }

    public FileObject openFileObject(String fileName, String mode) throws IOException {
        return IOUtils.openFileObject(unwrap(fileName), mode);
    }

    public OutputStream openFileOutputStream(String fileName, boolean append) {
        return IOUtils.openFileOutputStream(unwrap(fileName), append);
    }

    public void rename(String oldName, String newName) {
        IOUtils.rename(unwrap(oldName), unwrap(newName));
    }

    public boolean tryDelete(String fileName) {
        return IOUtils.tryDelete(unwrap(fileName));
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
